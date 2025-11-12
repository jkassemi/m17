# Ledger Controller Specification & Migration Plan

## Background & Objectives

- Today: ingestion/merge/greeks pipelines coordinate through bespoke cursors and service-local checkpoints; repairs require ad-hoc rewinds.
- Target: a ledger-centric controller that tracks every minute window for a trading year, acts as the single mutation surface, and triggers downstream materialization when prerequisite data exists.
- Objectives:
  1. Deterministic, replayable record of all produced artifacts per window.
  2. Simplified repair/backfill by clearing and refilling ledger cells.
  3. Built-in dependency orchestration (trade+quote → NBBO → Greeks) without bespoke controllers.
  4. Treasury data stored directly in ledger windows (no separate references).
- Non-goals: building sub-minute resolution, replacing existing storage formats, or redesigning business logic inside NBBO/Greeks calculators.
- Approach: treat this effort as a ground-up replacement; legacy checkpoints/workers will not be dual-written or retrofitted.
- Treat this document as a live checklist. As milestones are completed, mark the corresponding items below to maintain shared progress awareness.

Let's not worry yet about backup/restoration outside of a manual shell script executed by cron. We should be able to run the script to back up the ledger and mapping files. Let's target a configurable directory named "ledger.state" by default for all files. Backup/restore should be as simple as archiving the directory and restoring its contents when needed.

## Ledger Data Model

### Window Space

- Precompute all market-open minute windows for a calendar year (390 windows per full session).
- Introduce a dense symbol axis: each tradable instrument is assigned a `SymbolId` (u16) the first time it appears, capped at 10 000 per active year. The controller maintains a compact `SymbolMap { symbol_id → symbol }` plus the inverse map for lookups.
- Every ledger now stores `num_symbols × 390` rows per trading day. We allocate the entire matrix upfront so each `(symbol_id, minute_idx)` slot has a deterministic offset in the memory-mapped file.
- `window_id = <year>:<session_seq>:<minute_seq>` still exists for bookkeeping, but runtime addressing uses `(symbol_id, minute_idx)` with the knowledge that minute `idx` corresponds to `[start_ts, start_ts + 60s)`.
- Keep active year resident in memory; persist full ledger to disk (RocksDB/SQLite/Parquet) with periodic snapshots. Historical years remain on disk; only hot windows stay in RAM. With 10 000 symbols and 390 minutes, the trade ledger consumes ~600 MiB/day and the enrichment ledger ~900 MiB/day, both acceptable for resident memory.

Note: The 10K cap is more than appropriate given the scope of this project. We'll reevaluate or address migrations/updates more thoroughly when the project's been running for 3 months.

### Row Schema

```
- WindowRow { symbol_id, minute_idx, start_ts, end_ts, schema_version, slots }
- Slots are dense structs padded to 32 or 64 bytes (depending on ledger), so offsets remain constant across the memory-mapped file.

  Slot {
      payload_type: u8,   // enum identifying which mapping file owns the payload
      status: SlotStatus, // u8: Empty | Pending | Filled | Cleared
      payload_id: u32,    // index into that mapping file (0 = empty)
      version: u32,
      checksum: u32,
      last_updated_ns: i64,
  }
```

- Trade ledger rows carry four slots: `rf_rate_ref`, `trade_ref`, `quote_ref`, `aggressor_ref`.
- Enrichment ledger rows carry seven slots: `greeks_ref`, `aggs_1m`, `aggs_5m`, `aggs_10m`, `aggs_30m`, `aggs_1h`, `aggs_4h`.
- Empty slot means `status == Empty` and `payload_id == 0`; clearing a ref resets the struct to zeroed bytes and bumps `last_updated_ns`.
- Slots contain only lightweight metadata (type/id/version/checksum). Payload content lives in type-specific mapping files explained below.

### Payload Indirection & Mapping Tables

- Each payload type owns an append-only mapping file (e.g., `rf_rate.map`, `trade.map`, `quote.map`, `aggressor.map`, `greeks.map`, `aggregate.map`). The mapping stores `{payload_id → PayloadRecord}` where the record can include URIs, inline blobs, or structured metadata as needed by that producer.
- Writers append their payload to the appropriate mapping file, receive a monotonically increasing `payload_id`, and then call the ledger setter with `(payload_type, payload_id)`.
- Checksums stored in the slot cover the dereferenced payload bytes, allowing integrity verification without inlining the payload itself.
- Mapping files can be memory-mapped separately for fast dereferencing and share the same snapshot cadence as the ledgers.

### Ledger Variants

- `TradeLedger` is responsible for sourcing data (treasury, trades, quotes) and the derived aggressor view needed by downstream workers. Each ledger instance contains:
  - `symbol_map`: `SymbolId ↔ symbol`.
  - `window_rows: Vec<[TradeWindowRow; 390]>` grouped by symbol so `rows[symbol_id][minute_idx]` yields the struct instantly.
- `EnrichmentLedger` receives downstream overlays and aggregates that operate on trade ledger payloads. It mirrors the same dense window layout but uses `EnrichmentWindowRow` (seven slots) instead of four.
- Ledgers expose lightweight iterators for a symbol or minute slice so workers can process contiguous ranges efficiently (cache-friendly scans during repairs/backfills).

## Controller Responsibilities

1. **Ledger-specialized APIs**: ingestion/materialization services call explicit setters on the trade or enrichment ledger (`set_rf_rate_ref`, `set_trade_ref`, `set_quote_ref`, `set_aggressor_ref`, `set_greeks_ref`, `set_aggs_ref_*`). Each setter accepts `(symbol_id, minute_idx, payload_type, payload_id, checksum, version)` plus optional expected version for CAS semantics and automatically manages the slot status (`Pending` → `Filled`).
2. **Window-scoped coordination**: workers already know which symbol+minute windows they own while ingesting, backfilling, or repairing. They fetch that row, verify prerequisites locally, and update only the column they own—no central dependency graph, mutation queue, or fan-out triggers.
3. **Durability & restart**: mutations land in the mmap-backed file immediately; the OS flush policy is sufficient durability for the first iteration. Restarts remap the file and rebuild lightweight indexes—there is no WAL or changestream to replay.
4. **Concurrency control**: per-row compare-and-swap guards ensure two writers cannot race on the same slot; the controller returns a conflict error if a caller’s expected version regresses.
5. **Monitoring**: expose metrics such as `window_lag{column}` (minutes since `end_ts` but column empty), mutation throughput, symbol cardinality, and priority-region backlog so operators can spot stuck windows quickly.

## API Surface

All APIs address windows via `(symbol_id, minute_idx)`. Helpers exist to translate `symbol` → `symbol_id` (allocating a new ID if below the 10 000 cap) and `timestamp` → `minute_idx`.

### TradeLedger APIs

- `set_rf_rate_ref(symbol_id, minute_idx, payload_meta)`  
  Stores a risk-free-rate/treasury slice reference for that symbol+minute (used primarily for index/ETF symbols).
- `set_trade_ref(symbol_id, minute_idx, payload_meta)`  
  Writes the trade batch reference (usually an immutable partition ID).
- `set_quote_ref(symbol_id, minute_idx, payload_meta)`  
  Writes the quote/NBBO inputs captured for the minute.
- `set_aggressor_ref(symbol_id, minute_idx, payload_meta)`  
  Stores the aggressor/NBBO output derived from the trade+quote payloads.

Each setter accepts `payload_meta = { payload_type: PayloadType, payload_id: u32, version: u32, checksum: u32, last_updated_ns }`. The controller enforces that the provided `payload_type` matches the slot’s allowed enum.

### EnrichmentLedger APIs

- `set_greeks_ref(symbol_id, minute_idx, payload_meta)`
- `set_aggs_ref_{1m,5m,10m,30m,1h,4h}(symbol_id, minute_idx, payload_meta)`

These setters behave the same as TradeLedger writes but target the enrichment slots. Aggregation windows (5 m, 10 m, etc.) still anchor to the source minute so workers can mark progress once the aggregate covering that minute is complete.

### Common Helpers

- `clear_slot(ledger_kind, slot_kind, symbol_id, minute_idx, cause)` resets a column to `Cleared` (or `Empty` if never touched) and records the operator cause for audit; `last_updated_ns` reflects the clear time for lag analysis.
- `get_trade_row(symbol_id, minute_idx)` / `get_enrichment_row(symbol_id, minute_idx)` return the current row (including slot metadata).
- `iter_symbol(symbol_id, range)` and `iter_minute(minute_idx, symbol_range)` expose cache-friendly iterators for workers.
- `resolve_symbol(symbol) -> SymbolId` allocates IDs lazily until the 10 000-symbol cap is reached (hard-coded guard for now).
- `mark_pending(ledger_kind, slot_kind, symbol_id, minute_idx)` is an optional helper for workers to claim a slot when they enqueue work; it flips status to `Pending` and updates `last_updated_ns` without changing payload metadata.

## Worker Coordination

- Each ingestion or materialization loop maintains its own backlog of `(symbol_id, minute_idx)` entries (e.g., “next minute to ingest”, “range needing repair”). Before starting work, the worker marks the slot `Pending` (via a lightweight `mark_pending` helper) so operators can observe intent; once the payload is written it transitions to `Filled`.
- Priority regions (described below) override the default queue ordering. Workers must consult the shared priority table before selecting the next unit of work and bias toward the highest-priority outstanding region.
- Backfills or repairs simply `clear_slot` before writing replacements; no controller-managed retry/trigger queue exists.
- Because coordination is per-window, workers also own their own concurrency/backpressure limits. The ledger controller only enforces schema + version safety and persists the results.

## Priority Regions

- `PriorityRegion { symbol_id, start_ts, end_ts, priority }` records operator intent (e.g., a TUI selection) to repair or refetch specific intervals.
- Regions are stored in a small shared index (e.g., `BTreeMap<(symbol_id, start_ts), PriorityRegion>`) persisted alongside the ledger. A symbol can own multiple overlapping regions; the controller merges or reorders them by explicit `priority` (higher number = sooner).
- Engines poll the priority store before popping work from their default queues. Implementation guideline:
  1. Pull the highest-priority outstanding region whose `priority > default`.
  2. Expand it into the constituent minutes and push those `(symbol_id, minute_idx)` entries to the front of the worker’s queue.
  3. As each row reaches the desired state (slot filled, checksum verified), mark that minute complete inside the region tracker.
  4. Once all minutes in the interval are healthy, the controller retires the region automatically.
- The TUI (see `interface-thoughts.md`) issues priority regions when an operator selects a symbol+time range; repairs or automated anomaly detectors can do the same with lower priority levels.

## Treasury Cadence Handling

- Treasury ingestion writes `RfRatePayload { effective_ts, curve_id, yields, version }` whenever upstream publishes a new curve; the payload is appended to the `rf_rate.map` file.
- Mapping rule: compute `valid_from = floor_to_minute(effective_ts)` and `valid_to = next_update_effective_ts` (or market close if unknown). The treasury engine walks every symbol that requires the curve (typically all option symbols plus key equities) and writes the new payload ID into each affected row’s `rf_rate_ref` slot.
- Revisions bump `version`, overwrite the slots, and downstream Greeks loops notice the new treasury version when they re-read either ledger. Because the rf-rate payload lives in a mapping file, no inline updates are required—only the slot metadata changes.
- Greeks workers recompute once per window per treasury version by comparing the stored `rf_rate_ref.version` vs `greeks_ref.version`; mismatches cause the worker to rerun that minute.

## Persistence & Memory Strategy

- **Memory-mapped ledger file**: pre-layout the active year of windows into a fixed-size file; `mmap` it read-write so in-memory updates persist via the OS page cache without a separate WAL. Each slot struct carries a version/checksum so partial writes can be detected and corrected on restart.
- **In-process view**: wrap the mapped memory as `&mut [SymbolRows]` where each entry contains the 390-minute array. Addressing `(symbol_id, minute_idx)` becomes a simple double index; no hash lookups are needed at runtime.
- **Snapshots**: use a cron/systemd timer to take hourly reflink/copy-on-write snapshots of both ledger files plus all payload mapping files to a secondary volume, plus a daily off-host backup for DR.
- **Horizon**: keep six months of completed windows (prior to current date) mapped read-write, and pre-map the next six months for upcoming sessions. Older data lives on disk unmapped unless explicitly loaded for audits.

## Observability & Ops

- Metrics: mutation latency, per-column lag, symbol-cardinality usage, priority-region backlog, mapping-file growth, count of cleared refs, repair throughput, and slot-status ratios (Empty vs Pending vs Filled vs Cleared).
- Alerts: `trade_ref` lag > N minutes, `quote_ref` staleness, enrichment backlog, treasury revisions causing repeated recomputes, priority region stuck longer than threshold.
- Introspection tooling: CLI command `ledger window <symbol> <minute>` to print a row; `ledger tail --slot aggressor_ref` (or any slot) to follow changes.

## Security & Access

- Single-binary deployment (orchestrator + controller + engines) running under a single SSH operator; no external gRPC/HTTP surface required.
- Internal modules call controller APIs via in-process handles; audit log still records operator identity and payload checksums for traceability.

## Ingestion Engines & Orchestrator Integration

- Single binary: orchestrator and controller live in one process. Orchestrator builds/loads both ledgers and all payload mapping files, replays the latest snapshot, and hands shared handles (e.g., `Arc<TradeLedger>`, `Arc<EnrichmentLedger>`) to each engine.
- Engines (treasury, flatfile, websocket) run event loops that call the trade-ledger setters. Aggressor workers consume the trade rows once both `trade_ref` and `quote_ref` exist and publish their payload IDs back into the trade ledger.
- Greeks/aggregate engines take two handles: the trade ledger for source refs and the enrichment ledger for their outputs. They dereference payload IDs via the mapping tables as needed, then publish overlays/aggregates via enrichment setters.
- Orchestrator supervises engines: if the ledgers detect backpressure, symbol-cap exhaustion, or priority-region starvation, orchestrator can pause/resume loops, initiate repairs via ledger APIs, or clear/replay ranges.

## Migration Checklist

Use `[ ]` / `[x]` to track progress.

### Design Finalization

- [ ] Ratify schema, typed setter API, and treasury fill semantics.
- [ ] Document payload schemas for each mapping file (`rf_rate`, `trade`, `quote`, `aggressor`, `greeks`, `aggregate`).
- [ ] Define symbol-map lifecycle (allocation, eviction rules, snapshot format).
- [ ] Define orchestrator responsibilities: bootstrap window space, own ledger lifecycle, and hand references to ingestion engines.

### Ledger Service Skeleton

- [ ] Build window space generator to materialize the active-year minute map prior to starting any engines.
- [ ] Implement controller service with in-memory index, memory-mapped persistence, snapshotting, and basic APIs (`set_rf_rate_ref`, `set_trade_ref`, `set_quote_ref`, `set_aggressor_ref`, `set_greeks_ref`, `set_aggs_ref_*`, `get_*_row`).
- [ ] Implement payload mapping stores with append-only allocation, checksum validation, and snapshot hooks.
- [ ] Modify orchestrator startup to construct the ledger instance and inject handles into treasury/flatfile/websocket engines before launching loops.
- [ ] Build CLI/metrics scaffolding.
- [ ] Add unit/integration tests covering mutation + replay.

### Worker Flow Updates

- [ ] Document per-worker contracts (inputs, outputs, failure handling) for trades, quotes, NBBO/aggressor, treasury, and Greeks.
- [ ] Update worker implementations to poll ledger windows directly and publish via typed setters.
- [ ] Soak test by simulating synthetic workloads for a trading day with workers self-scheduling minutes.

### Priority Region Integration

- [ ] Implement the shared `PriorityRegionStore` with persistence and snapshot support.
- [ ] Wire the TUI (and other operator tools) to create/update/delete regions.
- [ ] Update all engines to consult the priority store before dequeuing default work and to report completion status so regions retire automatically.

### Dual-Write Ingestion

- [ ] Update flatfile trade ingest to write both legacy checkpoints and ledger `trade_ref`.
- [ ] Update quote ingest similarly; validate NBBO workers observe the required trade+quote refs via ledger reads.
- [ ] Stand up monitoring dashboards comparing ledger state vs legacy pipeline to ensure parity.

### NBBO & Treasury Adoption

- [ ] Point aggressor/NBBO materializer to trade-ledger getters/setters; ensure artifacts register via `set_aggressor_ref`.
- [ ] Write treasury slices directly into ledger windows and confirm carry-forward rules behave as expected.
- [ ] Run shadow-mode Greeks tasks consuming ledger outputs without publishing.

### Greeks Cutover & Legacy Decommission

- [ ] Switch Greeks publication to ledger-driven overlays.
- [ ] Remove legacy cursor/checkpoint code paths (keep read-only adapters for audit).
- [ ] Update runbooks, alerting, and documentation.

### Cleanup & Optimization

- [ ] Tune snapshot cadence, consider sharding, implement head eviction when stable.
- [ ] Build repair tooling (bulk `clear_*` plus range refill helpers) for ops ergonomics.

- `SlotStatus` is a compact enum encoded as `u8`. Semantics:
  - `Empty`: slot has never been touched or was explicitly cleared with no data available.
  - `Pending`: a worker has claimed the window and is actively sourcing the payload.
  - `Filled`: payload is durable and ready for downstream consumption.
  - `Cleared`: slot intentionally blank pending a refill (operators can distinguish this from never-touched rows).
  Engines transition `Empty → Pending → Filled` for normal ingestion and use `Cleared` when forcing a rewind before re-filling.
