# window-space

`window-space` owns the rolling trade/enrichment ledgers that every engine in
`m17` writes to. Engines mount the controller, claim slots, persist artifacts
into the mapping stores, then publish payload metadata back into the window rows.

### Bootstrapping

```rust
use std::sync::Arc;
use window_space::{WindowSpaceConfig, WindowSpaceController};

let config = WindowSpaceConfig::new(state_dir, window_space);
let (controller, storage_report) = WindowSpaceController::bootstrap(config)?;
```

Bootstrap does four things:

1. Builds/loads the window schedule (`WindowSpace`) and symbol map.
2. Opens/initializes the memory-mapped trade/enrichment ledgers.
3. Replays the payload mapping tables (`*.map`) so payload IDs remain stable.
4. Reconstructs slot metrics for observability.

Hold the returned controller in an `Arc`—every engine clones it.

### Writing slots

The typical engine loop for a `(symbol, window_idx)` looks like:

1. `controller.resolve_symbol(symbol)` to make sure the symbol ID exists.
2. `controller.mark_pending(symbol, window_idx, SlotKind::Trade(...))` to claim the slot.
3. Stream input rows, write the artifact (usually Parquet) somewhere under `state_dir`.
4. Append a payload entry to the mapping store:
   ```rust
   let payload_id = {
       let mut stores = controller.payload_stores();
       stores.trades.append(payload)?
   };
   ```
5. Build a `PayloadMeta::new(payload_type, payload_id, version, checksum)` and
   call the typed setter (`set_option_trade_ref`, `set_greeks_ref`, etc).
6. On failure, call `controller.clear_slot(...)` so the retry path can reclaim it.

Slots transition `Empty → Pending → Filled`. Operators can also force
`Cleared` or `Retire` via admin tooling. Engines should respect version conflicts:
if `set_*_ref` returns `WindowSpaceError::SlotWrite(SlotWriteError::VersionConflict)`,
someone else already filled that slot—just skip it.

### Reading rows

Engines rarely need the full window buffer, but you can:

```rust
let row = controller.get_trade_row("SPY", window_idx)?;
if row.option_trade_ref.status == SlotStatus::Filled {
    // use row.option_trade_ref.payload_id to locate the payload
}
```

`controller.payload_stores()` exposes every mapping table (trades, quotes,
aggressor, greeks, aggregations). The stores live behind a `Mutex`, so keep the
guard short—load payload metadata, clone it, then drop the guard before you do
any I/O.

### Window utilities

* `controller.window_idx_for_timestamp(ts)` ↦ window start index.
* `controller.window_meta(window_idx)` ↦ `WindowMeta` (start timestamp, duration, schema version).
* `controller.next_unfilled_window(symbol, start_idx, slot_kind)` ↦ first window whose slot isn’t `Filled`.
* `controller.slot_metrics()` ↦ live Prometheus-compatible counts per slot/status.

### Enrichment slots

Trade slots cover rf-rate, option/underlying trades & quotes, and aggressor
overlays. Enrichment slots currently include:

1. `Greeks`
2. `Aggregation` (10m SPY worker)

They’re addressed via `SlotKind::Enrichment(EnrichmentSlotKind::Greeks)` etc.
The workflow is the same as trade slots: claim via `mark_pending`, write your
artifact + mapping entry, then call `set_greeks_ref` or `set_aggregation_ref`.

### Storage layout

```
ledger.state/
├── trade-ledger.dat          # mmap’d trade rows
├── enrichment-ledger.dat     # mmap’d enrichment rows
├── symbol-map.json           # symbol ↔ id map
├── trades.map                # payload metadata appended by engines
├── quotes.map
├── aggressor.map
├── rf_rate.map
├── greeks.map
└── aggregations.map
```

Window rows track only payload IDs + checksums. Actual data (Parquet, JSON,
text) lives elsewhere under `state_dir`; the mapping tables let consumers
resolve `payload_id → artifact_uri`.

### Engine checklist

* Clone the controller (`Arc<WindowSpaceController>`).
* Claim windows with `mark_pending` before doing work.
* Persist artifacts atomically and compute checksums.
* Append payload metadata to the proper mapping store.
* Fill slots via the typed setter.
* Clear or retire slots when rewinding.

Following that pattern keeps every engine restart-safe and makes GC/ops tooling
aware of your status automatically.
