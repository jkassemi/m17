# Greeks Enrichment: Execution Checklist

Living task list that captures the outstanding work needed to fully separate Greeks enrichment from raw ingestion. Update this file as milestones land so we have a single source of truth for rollout tracking.

## Raw Pipeline & Storage

- [x] **Schema cutover:** update raw parquet schemas (options/equities) to `v2` that keep `trade_uid`, drop inline Greeks, and surface UID min/max per file for manifest pruning.
- [x] **Raw completion markers:** teach `FlatfileIngestionService` to emit per-day completion records (dataset + run_id + produced partitions) so enrichment jobs know exactly which files to read.
- [x] **Manifest writer:** extend `storage` with a manifest builder that records `dataset`, `date`, `run_id`, output files, hashes, `min_uid`/`max_uid`, and dependency fingerprints (engine version, curve hash, nbbo watermark).

## Derived Enrichment Pipeline (Top Priority)

- [x] **Greeks-only derivation worker:** create a dedicated service (`greeks-enrichment-service`) that consumes raw-day tasks, streams parquet rows, runs GreeksEngine, and writes slim derived parquet keyed by `trade_uid`. (NBBO overlays stay inline with trade ingestion.)
- [x] **Enrichment state machine:** persist `checkpoints/enrichment/<dataset>/<date>.json` with `{Pending, InFlight, Blocked(dep_missing), Published}` plus the active `run_id`.
- [ ] **Manual rerun workflow:** codify the operator-triggered treasury rerun (no automatic NBBO refresh) so flagged days can be rebuilt on demand without editing config by hand.

## Operator Visibility & Alerts (Next After Worker/State Machine)

- [ ] **Progress metrics:** expose current enrichment runs, blocked days, and last successful publish via `metrics`.
- [ ] **TUI progress view:** add panels that show raw/derived progress per day + active run IDs.
- [ ] **Alerting hooks (later):** once derivation is stable, add Prometheus alerts for long-lived blocked days and row-count mismatches.
- [ ] **GC + supersession (later):** when multiple runs per day exist, add a janitor that deletes superseded/expired runs; manual cleanup suffices until then.

## Consumer Migration (Hold)

- [ ] **`data-client` helpers:** add APIs that join raw parquet to Greeks by `trade_uid`, returning Arrow/Polars frames so downstream jobs don’t need to hand-roll merges.
- [ ] **Subsystem upgrades:** update aggregations, classifier consumers, and analytics notebooks to read Greeks from the derived dataset; remove fallback logic that expects inline Greeks.

## Runbooks & Automation (Hold)

- [ ] **Backfill playbooks:** document the end-to-end process for (a) full historical rebuild after the schema cutover, and (b) per-day replays driven by the enrichment state machine.
- [ ] **Operator tooling:** expose a CLI or RPC endpoint to enqueue manual reruns (instead of editing `m17/src/config.rs`), integrating with the state machine so the “Outdated data” panel clears automatically once the rerun publishes.
- [ ] **Partition writer follow-up:** evaluate reintroducing pooled parquet writers or a compaction job so manifests don’t explode file counts now that writes flush one file per batch.
