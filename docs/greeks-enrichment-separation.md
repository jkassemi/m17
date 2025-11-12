# Greeks Enrichment Separation Design

## Context

Flatfile ingestion currently writes a single parquet set per asset class (equities/options) after GreeksEngine runs inline. When treasury curves (or other dependencies) are missing the service still advances its checkpoints, so the “completed” parquet can contain partial or missing Greeks with no easy way to recover besides deleting checkpoints and rerunning entire days. The latest incident (weekend curves unavailable) highlighted three key pain points:

1. Dependencies outside our control (Massive’s `/fed/v1/treasury-yields`) can delay enrichment without blocking raw data downloads.
2. Inline enrichment forces us to rewrite raw trades whenever we want to reprocess Greeks, multiplying storage, IO, and operational risk.
3. Downstream consumers cannot tell whether Greeks were actually computed for a given day, so CRIT states silently degrade quality.

To fix this we propose treating Greeks as a derived dataset instead of a mutation of the raw trades. Raw parquet remains immutable; derived Greeks files reference the raw trades via stable identifiers. This lets us recompute Greeks whenever dependencies recover, version the outputs, and audit completeness.

## Goals

- Preserve immutable raw trade dumps; never rewrite them for enrichment replays.
- Emit Greeks-only parquet (options) and any auxiliary per-trade metrics (e.g., treasury curve snapshot, NBBO references) keyed by a stable trade identifier.
- Track per-day enrichment status so reruns resume automatically when dependencies unblock.
- Minimize additional IO/CPU overhead so end-to-end throughput stays within today’s budget.
- Provide a clear rollout path that doesn’t break existing consumers.

## Non-goals

- Rewriting historical data immediately (migration can be incremental).
- Building a general-purpose feature store; scope is Greeks, NBBO overlays, and DADV-at-spot-delta outputs directly tied to each ingestion day.
- Solving arbitrarily late data arrivals (>30 days) – we assume gaps close within a week.
- Supporting multiple production versions simultaneously; for now the latest published run per day is canonical and supersedes prior runs.

## Proposed Architecture

### Data Layout

```
data/
  raw/
    options/YYYY/MM/DD/options_trades-*.parquet
    equities/YYYY/MM/DD/equity_trades-*.parquet
  greeks/
    options/YYYY/MM/DD/greeks-<engine_version>-<curve_hash>-<run_id>-*.parquet
  nbbo/
    options/YYYY/MM/DD/nbbo_overlay-<run_id>-*.parquet
    equities/YYYY/MM/DD/nbbo_overlay-<run_id>-*.parquet
  manifests/
    options/YYYY/MM/DD/manifest-<run_id>.json
    equities/YYYY/MM/DD/manifest-<run_id>.json
```

- Raw data keeps the existing schema.
- Greeks files contain:
  - `trade_id` (or synthetic hash) that matches a column in raw parquet.
  - Computed Greeks (delta/gamma/vega/theta/iv) plus diagnostics (greeks_flags, treasury_curve_ts, underlying_price_used, nbbo_age_us, etc.).
  - Metadata columns: `engine_version`, `treasury_curve_date`, `enriched_at_ns`, `run_id`, optional `expires_at`.
- NBBO overlay files (equities + options) record quote context keyed by the same identifiers, enabling downstream DADV-at-spot-delta computations without reparsing NBBO dumps.
- Each run (scheduled or on-demand) gets a unique `run_id`. Manifests capture the run metadata, target raw files, derived artifacts, hashes, and optional `expires_at`. When a run supersedes a prior one, the manifest notes `supersedes: <run_id>` so GC can delete the old data immediately. Only the newest non-expired run per day is considered canonical; we are not supporting parallel production versions at this stage.

### Pipeline Changes

1. **Checkpoint split**: keep existing per-dataset/hour checkpoints for raw ingestion. Introduce a per-day enrichment state machine with states `{Pending, InFlight, Blocked(dep_missing), Published}` stored under `checkpoints/enrichment/<dataset>/<date>.json`.
2. **Derived jobs (Greeks + NBBO overlays)**: after raw batches are written, enqueue their day+dataset into dedicated workers that:
   - Loads raw parquet batches (or reuses in-memory batches during the same run).
   - Runs GreeksEngine and NBBO overlay builders with the required treasury/NBBO context.
   - Writes derived parquet (Greeks + overlay) and updates the manifest when all batches succeed.
   - For on-the-fly analyses (e.g., DADV at a hypothetical spot delta), we can submit ad-hoc runs with custom params + `expires_at`. They reuse the same queue but emit into `run_id`-scoped directories so GC can clean them automatically.
3. **Dependency handling**: if treasury data is missing (or fails midway), mark the day `Blocked` but leave raw checkpoints untouched. A periodic reconciliation loop inspects blocked days after each successful treasury refresh and resubmits them.
4. **Publication**: only mark a day `Published` once raw, Greeks, and NBBO overlays are present for the selected run. The TUI can show multi-stage progress (“Raw complete / Derived pending / On-the-fly pending”).

### Identifier Strategy

- Always use a local blake3-based trade and quote id for both options and equities. We have concerns about trade identifiers from external sources.

- Generate `options_trade_uid = blake3(contract || trade_ts_ns || seq || exchange)` during ingestion and persist it in both raw and Greeks datasets.
- Store the UID in raw parquet going forward so historical joins are trivial.
- Greeks parquet only needs the UID plus derived metrics, keeping files slim.
- NBBO overlay rows reference either `trade_uid` (for options) or `quote_uid` (for equities). For equities we can hash `(symbol, quote_ts_ns, sequence_number)` into `quote_uid` to support joins with storage and DADV calculations.

### Identifier Options & Join Optimizations

We need a single canonical identifier to stitch raw trades, Greeks, and NBBO overlays together efficiently. Options include:

1. **Upstream identifiers (preferred when available)**
   - Use Massive’s `trade_id` / `seq` fields directly.
   - Pros: deterministic, already part of the dataset, no extra computation.
   - Cons: equities trades sometimes lack globally unique IDs; NBBO rows generally do not publish one.
2. **Deterministic hashes**
   - Compute `uid = blake3(symbol || contract || trade_ts_ns || seq || exchange)` (16 bytes) for trades, and similar for NBBO quotes.
   - Pros: uniform width, easy to sort, collision-resistant, works across all asset classes.
   - Cons: extra CPU during ingest; we must persist inputs exactly to maintain determinism.
3. **Monotonic sequence numbers per day**
   - Assign an incremental ID as batches stream through.
   - Pros: trivial to store/join; can be 64-bit integers for cache-friendly joins.
   - Cons: requires strict ordering guarantees and checkpoint coordination to avoid gaps/duplication.

Join-friendly representation:

- Store identifiers as fixed-size binary (16 bytes) or unsigned 128-bit integers to keep Arrow/Parquet encodings compact and SIMD-friendly.
- Sort derived datasets by identifier within each parquet row group so Arrow/Polars can perform merge joins without additional shuffles.
- Persist a min/max identifier range per file in the manifest; this lets consumers prune files during lookups.
- **Decision**: adopt a 128-bit blake3-based `trade_uid`/`quote_uid` across both equities and options, while still persisting upstream IDs for traceability. Writing the UID as `FixedSizeBinary(16)` and sorting by it enables cache-friendly merge joins and keeps manifests simple (single key space). Input fields must be identical between reruns to guarantee determinism.

## Performance Considerations

| Component | Impact | Notes |
|-----------|--------|-------|
| Storage footprint | +10–30% | Greeks-only and NBBO overlay files are much smaller than raw trades (≈5–15 numeric columns). Two copies of raw data are no longer needed for reruns, but on-the-fly runs increase usage until GC reclaims them. |
| Write amplification | Slight increase | Separate parquet writers per dataset, but each writes fewer columns. We can buffer multiple Greek batches per file to hit existing 128 MB targets. |
| CPU usage | Neutral | GreeksEngine workload is unchanged; separating it just shifts when writes occur. |
| IO on rerun | Lower | Re-enriching a day rereads raw parquet once and writes only the derived dataset, instead of rewriting raw trades. |
| Join cost for consumers | Depends | Consumers must join raw trades to Greeks and (optionally) NBBO overlays; helper APIs in `storage`/`data-client` can hide the merge and serve Arrow/Polars dataframes. |
| GC overhead | Bounded | Background janitor scans manifests for expired or superseded runs and deletes them during low-traffic windows. |

### Mitigations

- Use the existing batch pipeline to stream raw rows directly into the Greeks writer during the same process when dependencies are satisfied, avoiding disk rereads in the common case.
- For reruns, pin parquet row groups in object storage or local cache to minimize IO.
- GC queue: manifests with `expires_at` schedule deletions in a priority queue keyed by deadline. Superseded runs enter the queue immediately but respect a short grace window to drain readers. Expired on-the-fly runs default to “never” unless `expires_at` is set.

## Rollout Plan

Because we can recreate the dataset from scratch, we do not need a backwards-compatible migration. We’ll perform a clean cutover:

1. **Immediate schema change**
   - Update raw parquet writers to include `trade_uid` and drop inline Greeks in a single deploy. Rebuild the historical dataset from Massive’s dumps so every partition uses the new schema before we restart the orchestrator.
2. **Greeks backfill**
   - Run the enrichment pipeline in batch mode across the historical window to populate `data/greeks/...` alongside manifests. Since downstream reads are paused, we can reuse all cluster resources for this job.
3. **Consumer switch**
   - Point internal jobs directly at `data/raw` + `data/greeks` with the documented join contract; no fallback to legacy columns is required.
4. **Resume live ingest**
   - Re-enable daily ingestion with the enrichment state machine so new days produce raw + Greeks outputs automatically. Blocked states will auto-rerun once dependencies land, preventing future manual intervention.

## Operations & Monitoring

- **Status dashboard**: expose per-day enrichment states for raw, Greeks, and NBBO overlays; include counts for on-the-fly runs (`enrichment_runs_inflight`, `runs_expiring_soon`, etc.).
- **Curve snapshots**: store the treasury curve date/hash in each manifest so data scientists can trace which macro inputs were used.
- **Alerting**: fire alerts if a day stays `Blocked` beyond N hours after dependencies recover, if Greeks/NBBO row counts don’t match raw counts within a tolerance, or if expired runs linger past their grace window.
- **GC observability**: metrics for `gc_queue_depth`, `gc_runs_deleted_total`, and logs showing which run_ids were reclaimed.

## Open Questions

1. Confirm the 128-bit blake3-based identifier implementation (trade_uid/quote_uid) and wire it through ingestion, storage, manifests, and downstream consumers.

## Next Steps

1. Finalize the identifier strategy, manifest schema, directory layout, and GC policy (default `expires_at` for on-the-fly runs).
2. Implement the new writers/readers plus enrichment state machine, then run a full backfill to rebuild `data/raw`, `data/greeks`, and `data/nbbo`.
3. Extend the TUI/metrics to surface enrichment-specific gauges (including GC + on-the-fly run tracking) so operators can monitor the new pipeline.
4. Publish runbooks for backfills/reruns and ad-hoc analyses (e.g., DADV at spot delta) now that derived datasets are standalone.
