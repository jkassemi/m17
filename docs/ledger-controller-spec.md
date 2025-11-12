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

Development guidance: it is fine to inspect existing crates for reference, but all new work must land inside the crates defined below (`m17`, `ledger`, and the new engines). Legacy crates remain untouched during development and will be deleted once the greenfield stack reaches feature parity.

Let's not worry yet about backup/restoration outside of a manual shell script executed by cron. We should be able to run the script to back up the ledger and mapping files. Let's target a configurable directory named "ledger.state" by default for all files. Backup/restore should be as simple as archiving the directory and restoring its contents when needed.

## Ledger Data Model

### Window Space

- Precompute all market-open minute windows for a calendar year (390 windows per full session).
- Introduce a dense symbol axis: each tradable instrument is assigned a `SymbolId` (u16) the first time it appears, capped at 10 000 per active year. The controller maintains a compact `SymbolMap { symbol_id → symbol }` plus the inverse map for lookups.
- Every ledger now stores `num_symbols × 390` rows per trading day. We allocate the entire matrix upfront so each `(symbol_id, minute_idx)` slot has a deterministic offset in the memory-mapped file.
- `window_id = <year>:<session_seq>:<minute_seq>` still exists for bookkeeping, but runtime addressing uses `(symbol_id, minute_idx)` with the knowledge that minute `idx` corresponds to `[start_ts, start_ts + 60s)`.
- Keep the active year resident in memory while persisting every mmapped file under a configurable `ledger.state/` directory that cron-backed scripts archive and restore. With 10 000 symbols and 390 minutes, the trade ledger consumes ~600 MiB/day and the enrichment ledger ~900 MiB/day, both acceptable for resident memory.

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

#### Payload Schemas

All payloads embed a `schema_version` so consumers can reject incompatible records. Storage format is a packed binary blob with a short header followed by variant-specific data.

- `rf_rate.map`
  - `RfRatePayload { schema_version: u8, effective_ts: i64, curve_id: u16, tenor_bps: [i32; 12], next_effective_ts: i64, source_uri: String }`
  - Tenor array stores the standard 12-point curve, scaled in basis points to avoid floating point in the ledger.
- `trade.map`
  - `TradePayload { schema_version: u8, batch_id: u32, first_ts: i64, last_ts: i64, record_count: u32, compression: CompressionKind, artifact_uri: String }`
  - `CompressionKind` is a `u8` enum (None | Snappy | Zstd) shared with the quote payload.
- `quote.map`
  - `QuotePayload { schema_version: u8, batch_id: u32, first_ts: i64, last_ts: i64, nbbo_sample_count: u32, artifact_uri: String }`
- `aggressor.map`
  - `AggressorPayload { schema_version: u8, minute_ts: i64, trade_payload_id: u32, quote_payload_id: u32, observation_count: u32, artifact_uri: String }`
- `greeks.map`
  - `GreeksPayload { schema_version: u8, minute_ts: i64, rf_rate_payload_id: u32, greeks_version: u32, artifact_uri: String, checksum: u32 }`
- `aggregate.map`
  - `AggregatePayload { schema_version: u8, minute_ts: i64, window: AggregateWindow, source_span: (u32, u32), stats_uri: String }`
  - `AggregateWindow` is a `u8` enum (1m, 5m, 10m, 30m, 1h, 4h). `source_span` stores `(start_symbol_row, end_symbol_row)` indexes so audit tools can rehydrate the contributing rows without recomputing.

Every payload struct ends with a variable-length URI or inline metadata blob, allowing engines to point to Parquet, Arrow, or custom chunk files without expanding the ledger itself.

### Ledger Variants

- `TradeLedger` is responsible for sourcing data (treasury, trades, quotes) and the derived aggressor view needed by downstream workers. Each ledger instance contains:
  - `symbol_map`: `SymbolId ↔ symbol`.
  - `window_rows: Vec<[TradeWindowRow; 390]>` grouped by symbol so `rows[symbol_id][minute_idx]` yields the struct instantly.
- `EnrichmentLedger` receives downstream overlays and aggregates that operate on trade ledger payloads. It mirrors the same dense window layout but uses `EnrichmentWindowRow` (seven slots) instead of four.
- Ledgers expose lightweight iterators for a symbol or minute slice so workers can process contiguous ranges efficiently (cache-friendly scans during repairs/backfills).

### Symbol Map Lifecycle

- `SymbolMap` lives at `ledger.state/symbol-map.bin` (little-endian) and is memory-mapped alongside the ledgers.
- On bootstrap, the controller loads the map, rebuilds the inverse lookup table, and seeds the next `SymbolId` counter.
- `resolve_symbol(symbol)` allocates sequential IDs until the 10 000 cap is reached; IDs are stable for the entire active year.
- No automatic eviction occurs mid-year. When the calendar rolls, operators archive the prior `ledger.state/` directory, create a new one with a fresh map, and optionally replay symbols that should stay hot.
- Manual overrides (e.g., reclaiming IDs) are accomplished by editing `symbol-map.bin` offline before remapping; future tooling can automate this once usage patterns demand it.

## Crate Layout & Worker Naming

- `m17/`: orchestrator crate and primary binary entrypoint. It boots the ledgers, wires engines together, and owns process lifetime.
- `ledger/`: shared library crate hosting the trade/enrichment ledgers, mapping-file abstractions, and controller APIs.
- Worker crates follow the `<domain>-engine` naming convention so their purpose is obvious at a glance:
  - `treasury-engine`: streams treasury curves into `rf_rate_ref` slots.
  - `trade-flatfile-engine`: ingests historical/flatfile trades into `trade_ref` slots.
  - `trade-ws-engine`: ingests realtime websocket trades into `trade_ref` slots.
  - `quote-engine`: ingests quotes/NBBO inputs into `quote_ref` slots.
  - `nbbo-engine`: derives aggressor/NBBO payloads from trade+quote refs and fills `aggressor_ref`.
  - `greeks-engine`: produces greeks overlays into `greeks_ref`.
  - `aggregation-engine`: writes rolling aggregates into the enrichment slots (`aggs_*`).
  - Additional specialized workers (e.g., anomaly detection) should follow the same `*-engine` pattern.
- Each engine crate compiles to its own library with a lightweight runner so the `m17` orchestrator can embed them as modules without external binaries.

## Engine Crate Specifications

Each engine owns a focused scope, exposes a shared `Engine` trait (`start(ctx)`, `stop()`, `health()`, `describe_priority_hooks()`), and consumes ledger handles passed in by `m17`.

### `treasury-engine`

- Inputs: upstream treasury feed client plus `TradeLedger` handle for `rf_rate_ref` writes.
- Behavior: ingest `RfRatePayload` updates, compute `valid_from/valid_to`, and walk all applicable symbols to write the new payload ID into every minute within the validity range. Carries forward rates until superseded.
- Outputs: `rf_rate.map` payloads + slot updates; publishes metrics for curve lag and per-symbol update counts.

### `trade-flatfile-engine`

- Inputs: batch/flatfile trade connectors, symbol resolver, `TradeLedger` handle.
- Behavior: walk historical files, batch trades per minute, append `TradeBatchPayload` records, write `trade_ref` slots, and mark `Pending` while processing. Provides hooks for repair/rewind via `clear_slot`.
- Outputs: trade payload IDs, `trade_ref` slot transitions, ingestion metrics (latency, record count, file offsets).

### `trade-ws-engine`

- Inputs: realtime websocket feeds, incremental checkpoint state, `TradeLedger` handle.
- Behavior: stream trades minute-by-minute, emit `TradeBatchPayload` artifacts (or inline buffers) once the minute closes, write `trade_ref` slots immediately so downstream consumers see near-real-time updates.
- Outputs: trade payload IDs, real-time ingestion metrics (lag, dropped messages, reconnect counters).

### `quote-engine`

- Inputs: quote/NBBO feeds, `TradeLedger` handle for `quote_ref` slots.
- Behavior: batch quotes per minute, persist `QuotePayload`, and write slots once both payload and checksum are ready. Maintains small local cache to align with trade batches for NBBO readiness.
- Outputs: quote payload IDs, slot updates, metrics for quote lag and NBBO coverage.

### `nbbo-engine`

- Inputs: read-only `TradeLedger` view (trade+quote refs) plus write access to `aggressor_ref` slots.
- Behavior: scan windows where both trade and quote refs are `Filled`, compute aggressor/NBBO payloads, append `AggressorPayload`, and store references. Retries when dependencies clear.
- Outputs: aggressor payload IDs, slot updates, dependency lag metrics.

### `greeks-engine`

- Inputs: `TradeLedger` (for rf/trade/quote/aggressor context) and `EnrichmentLedger` write handle.
- Behavior: recompute greeks whenever treasury version or upstream payloads change, append `GreeksPayload`, and fill `greeks_ref`. Uses CAS to avoid stomping newer writes.
- Outputs: greeks payload IDs, slot transitions, recompute counters keyed by treasury version.

### `aggregation-engine`

- Inputs: read-only `EnrichmentLedger` + `TradeLedger` (for source data) and write access to enrichment aggregate slots.
- Behavior: for each minute `T`, compute rolling aggregates per configured window (1m/5m/10m/30m/1h/4h), append `AggregatePayload` entries, and write the results to the slot whose `minute_idx` equals the window start (`T`, `T+5m`, …). Re-entrant runs only touch windows assigned to the current invocation.
- Outputs: aggregate payload IDs, slot transitions, metrics for window coverage and backlog.

## Legacy Crate Inventory & Retirement Plan

Existing workspace crates (e.g., `orchestrator/`, `treasury-ingestion-service/`, `flatfile-ingestion-service/`, `realtime-ws-ingestion-service/`, `greeks-enrichment-service/`, `nbbo-cache/`, `core-types/`, `metrics/`, `storage/`, `options-universe-ingestion-service/`, `aggregations/`, `classifier/`, `data-client/`) remain useful as design references, but the new implementation must not depend on or extend them. Once the `m17` orchestrator, `ledger`, and all `*-engine` crates satisfy the functionality outlined here, the legacy crates will be deleted from the workspace to avoid split-brain execution paths.

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

These setters behave the same as TradeLedger writes but target the enrichment slots. Aggregation windows are rolling: a `5m` aggregate stored at minute `T` covers `[T, T+5m)` and the aggregation engine writes to every `minute_idx` it evaluates. For example, when invoked for minute `T` it fills `T, T+5m, T+10m, …`; when invoked at `T+1m` it writes `T+1m, T+6m, …` without mutating previously written rows. Downstream readers always interpret `minute_idx` as “window start”.

### Common Helpers

- `clear_slot(ledger_kind, slot_kind, symbol_id, minute_idx, cause)` resets a column to `Cleared` (or `Empty` if never touched) and records the operator cause for audit; `last_updated_ns` reflects the clear time for lag analysis.
- `get_trade_row(symbol_id, minute_idx)` / `get_enrichment_row(symbol_id, minute_idx)` return the current row (including slot metadata).
- `iter_symbol(symbol_id, range)` and `iter_minute(minute_idx, symbol_range)` expose cache-friendly iterators for workers.
- `resolve_symbol(symbol) -> SymbolId` allocates IDs lazily until the 10 000-symbol cap is reached (hard-coded guard for now).
- `mark_pending(ledger_kind, slot_kind, symbol_id, minute_idx)` is an optional helper for workers to claim a slot when they enqueue work; it flips status to `Pending` and updates `last_updated_ns` without changing payload metadata.

## Worker Coordination

- Each `*-engine` crate maintains its own backlog of `(symbol_id, minute_idx)` entries (e.g., “next minute to ingest”, “range needing repair”). Before starting work, the worker marks the slot `Pending` (via a lightweight `mark_pending` helper) so operators can observe intent; once the payload is written it transitions to `Filled`.
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
- **Snapshots**: bundle `scripts/archive-ledger.sh`, a cron-invoked helper that tars and checksums the entire `ledger.state/` directory (ledgers, mapping files, symbol map). Restores untar the archive into place before remapping.
- **Horizon**: keep six months of completed windows (prior to current date) mapped read-write, and pre-map the next six months for upcoming sessions. Older data lives on disk inside `ledger.state/` but remains unmapped unless explicitly loaded for audits.

## Observability & Ops

- Metrics: mutation latency, per-column lag, symbol-cardinality usage, priority-region backlog, mapping-file growth, count of cleared refs, repair throughput, and slot-status ratios (Empty vs Pending vs Filled vs Cleared).
- Alerts: `trade_ref` lag > N minutes, `quote_ref` staleness, enrichment backlog, treasury revisions causing repeated recomputes, priority region stuck longer than threshold.
- Introspection: expose metrics/log hooks first; defer a standalone CLI until operators request one. Future tooling can call controller APIs or reuse the `m17` process once requirements solidify.

## Security & Access

- Single-binary deployment (orchestrator + controller + engines) running under a single SSH operator; no external gRPC/HTTP surface required.
- Internal modules call controller APIs via in-process handles; audit log still records operator identity and payload checksums for traceability.

## Ingestion Engines & Orchestrator Integration

- Single binary: the `m17` orchestrator crate hosts the ledgers and controller in-process. It builds/loads both ledgers plus all payload mapping files, replays the latest snapshot, and hands shared handles (e.g., `Arc<TradeLedger>`, `Arc<EnrichmentLedger>`) to each engine module.
- Engines (treasury, trade-flatfile, trade-ws, quote, nbbo, greeks, aggregation) run event loops that call the ledger setters defined in the `ledger` crate. Aggressor workers consume the trade rows once both `trade_ref` and `quote_ref` exist and publish their payload IDs back into the trade ledger.
- Greeks/aggregation engines take two handles: the trade ledger for source refs and the enrichment ledger for their outputs. They dereference payload IDs via the mapping tables as needed, then publish overlays/aggregates via enrichment setters.
- Orchestrator supervises engines: if the ledgers detect backpressure, symbol-cap exhaustion, or priority-region starvation, `m17` can pause/resume loops, initiate repairs via ledger APIs, or clear/replay ranges.

## Greenfield Build Plan

Use `[ ]` / `[x]` to track progress.

### Design Finalization

- [x] Ratify schema, typed setter API, treasury fill semantics, and rolling aggregation behavior (see Row Schema, API Surface, Treasury Cadence, and EnrichmentLedger sections).
- [x] Document payload schemas for each mapping file (`rf_rate`, `trade`, `quote`, `aggressor`, `greeks`, `aggregate`).
- [x] Define symbol-map lifecycle (allocation, eviction rules, snapshot format).
- [x] Finalize crate boundaries (`m17`, `ledger`, each `*-engine`) and shared traits the orchestrator uses to drive engines (see Crate Layout & Worker Naming plus Ingestion Engines & Orchestrator Integration).

### Ledger & Controller

- [x] Build window space generator to materialize the active-year minute map prior to starting any engines.
- [x] Implement controller service with in-memory index, memory-mapped persistence, snapshotting, and basic APIs (`set_rf_rate_ref`, `set_trade_ref`, `set_quote_ref`, `set_aggressor_ref`, `set_greeks_ref`, `set_aggs_ref_*`, `get_*_row`).
- [x] Implement payload mapping stores with append-only allocation, checksum validation, and snapshot hooks.
- [x] Add unit/integration tests covering mutation, replay, and CAS contention semantics.

### First Engine Crate

#### `trade-flatfile-engine`

- [ ] Scaffold crate focused on historical/flatfile inputs and a batching pipeline keyed by `(symbol_id, minute_idx)`.
- [ ] Append `TradeBatchPayload` records, write `trade_ref` slots, and expose repair helpers (`clear_slot`, rewind hooks) for backfills.
- [ ] Instrument ingestion latency, record counts, file offsets, and retry counters.

### Orchestrator (`m17`)

- [ ] Construct the `m17` crate that boots ledgers, loads snapshots, and registers engines as modules.
- [ ] Define engine traits (start/stop, health probes, priority-region hooks) and shared instrumentation wiring.
- [ ] Implement supervision logic for backpressure, repairs, and graceful shutdown.

### Remaining Engine Crates

#### `treasury-engine`

- [ ] Scaffold crate with engine trait implementation, config structs, and ingestion entrypoint.
- [ ] Implement treasury feed client (or adapters) plus carry-forward logic that writes `rf_rate_ref` slots per symbol/minute.
- [ ] Emit metrics/logs for curve lag, version churn, and per-symbol update counts.

#### `treasury-engine`

- [ ] Scaffold crate with engine trait implementation, config structs, and ingestion entrypoint.
- [ ] Implement treasury feed client (or adapters) plus carry-forward logic that writes `rf_rate_ref` slots per symbol/minute.
- [ ] Emit metrics/logs for curve lag, version churn, and per-symbol update counts.

#### `trade-flatfile-engine`

- [ ] Scaffold crate focused on historical/flatfile inputs and a batching pipeline keyed by `(symbol_id, minute_idx)`.
- [ ] Append `TradeBatchPayload` records, write `trade_ref` slots, and expose repair helpers (`clear_slot`, rewind hooks) for backfills.
- [ ] Instrument ingestion latency, record counts, file offsets, and retry counters.

#### `trade-ws-engine`

- [ ] Scaffold crate for realtime websocket ingestion with reconnect/backpressure handling.
- [ ] Buffer minute windows, append `TradeBatchPayload` artifacts (or inline buffers) as minutes close, then write `trade_ref` slots promptly.
- [ ] Instrument stream lag, reconnects, dropped messages, and per-minute publish latency.

#### `quote-engine`

- [ ] Scaffold crate with quote/NBBO feed adapters and per-minute batching.
- [ ] Persist `QuotePayload` entries, write `quote_ref` slots, and coordinate with NBBO readiness (e.g., notify `nbbo-engine` when prerequisites exist).
- [ ] Track quote lag, sample counts, and dependency coverage in metrics.

#### `nbbo-engine`

- [ ] Build worker that scans trade+quote slots for `Filled` windows, computes aggressor/NBBO payloads, and appends to `aggressor.map`.
- [ ] Write `aggressor_ref` slots with CAS semantics and handle retries when dependencies change mid-flight.
- [ ] Emit metrics for dependency lag and recompute/backfill throughput.

#### `greeks-engine`

- [ ] Implement greeks computation loop that watches treasury versions and upstream slot changes.
- [ ] Persist `GreeksPayload` outputs, write `greeks_ref`, and guard against stale rewrites with version checks.
- [ ] Surface recompute counters per treasury version and slot-lag metrics.

#### `aggregation-engine`

- [ ] Build rolling aggregation runners for 1m/5m/10m/30m/1h/4h windows, keyed by minute start.
- [ ] Append `AggregatePayload` entries per window and write the corresponding enrichment slots without touching previously written minutes.
- [ ] Track coverage/lateness per window size plus backlog size for operator visibility.

### Priority Region Integration

- [ ] Implement the shared `PriorityRegionStore` with persistence and snapshot support.
- [ ] Wire the TUI (and other operator tools) to create/update/delete regions.
- [ ] Ensure all engines consult the priority store before dequeuing default work and report completion so regions retire automatically.

### Cleanup & Optimization

- [ ] Tune snapshot cadence, consider sharding, implement head eviction when stable.
- [ ] Build repair tooling (bulk `clear_*` plus range refill helpers) for ops ergonomics.

- `SlotStatus` is a compact enum encoded as `u8`. Semantics:
  - `Empty`: slot has never been touched or was explicitly cleared with no data available.
  - `Pending`: a worker has claimed the window and is actively sourcing the payload.
  - `Filled`: payload is durable and ready for downstream consumption.
  - `Cleared`: slot intentionally blank pending a refill (operators can distinguish this from never-touched rows).
  Engines transition `Empty → Pending → Filled` for normal ingestion and use `Cleared` when forcing a rewind before re-filling.

## Notes

How a trade engine writes a trade_ref - early concept:

  Trade engine pseudocode

  let minute_idx = controller
      .minute_idx_for_timestamp(window_start_ts)
      .expect("window in range");

  let trade_batch = TradeBatchPayload {
      schema_version: 1,
      minute_ts: window_start_ts,
      batch_id: next_batch_id(),
      first_trade_ts,
      last_trade_ts,
      record_count,
      artifact_uri: trade_artifact_uri.clone(),
      checksum,
  };

  let payload_id = {
      let mut stores = controller.payload_stores();
      stores.trades.append(trade_batch)?
  };

  let meta = PayloadMeta::new(PayloadType::Trade, payload_id, version, checksum);
  controller.set_trade_ref(symbol, minute_idx, meta, None)?;
