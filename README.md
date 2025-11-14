# Trade Capture M17

In numerology, 17 symbolizes success, spiritual insight, and business acumen.

**Copyright (c) James Kassemi, SC, US. All rights reserved.**

## Overview

This system captures trades with NBBO snapshots, classifies aggressors, calculates greeks, stores data in Parquet, and provides a dashboard for realtime application of trading algorithms and strategies.

```bash

export POLYGONIO_KEY=
export POLYGONIO_ACCESS_KEY_ID=
export POLYGONIO_SECRET_ACCESS_KEY=

./m17 prod # For production server
./m17 dev # For development server
```

### Replaying 2025-11-10 data with `wstool`

1. Build the replay tool: from `wstool/`, run `cargo build --release`.
2. Serve the canned session: `./target/release/wstool serve --in-dir recording.2025-11-10-close --bind 127.0.0.1:9000`.
3. Point `m17` at the local server by exporting:
   ```bash
   export M17_STOCKS_WS_URL=ws://127.0.0.1:9000/stocks
   export M17_OPTIONS_WS_URL=ws://127.0.0.1:9000/options
   # Optional: export M17_REST_BASE_URL=http://127.0.0.1:9000 if you stand up a REST shim.
   ```
4. Launch `m17 dev` (after the usual `POLYGONIO_*` secrets) and it will bind to the replayed websocket feeds instead of Massive’s live endpoints.

### Websocket metrics

`m17` exposes Prometheus metrics on `http://127.0.0.1:9095/metrics` in dev. The new `trade_ws_*` gauges track websocket health:

- `trade_ws_events_total`: monotonic count of option-trade events received from the websocket.
- `trade_ws_subscribed_contracts`: how many option contracts we’re currently subscribed to.
- `trade_ws_seconds_since_contract_refresh`: seconds since the most-active snapshot (contract refresh) succeeded; `-1` means no refresh yet.

Example: `curl -s localhost:9095/metrics | grep trade_ws_`.

#### Quick metrics check

```bash
curl -s http://127.0.0.1:9095/metrics | grep windowspace_
```

### Aggregation artifacts

The initial 10m SPY aggregation worker writes a plain-text artifact for each finished window under
`ledger.state/aggregations/manual/symbol=SPY/window=10m/dt=YYYY-MM-DD/<window_start_ns>.txt` and appends the same
summary lines to `OUTPUT.txt`. Operators can tail `OUTPUT.txt` for a live report or open the per-window files for archival review.

### Windowspace diagnostics

`cargo run -p m17 --bin windowspace_diag <env> [--symbol=SPY]` prints slot-status counts for every trade/enrichment column. Use it when metrics suggest the ledger is empty (e.g., 100 % `Empty` slots) to confirm whether data actually landed and to find the first non-empty window per slot. Examples:

```bash
# Aggregate view across all symbols in dev
cargo run -p m17 --bin windowspace_diag dev

# Focus on SPY trade + enrichment slots in prod
cargo run -p m17 --bin windowspace_diag prod --symbol=SPY
```

Each line reports total windows plus a `sample` field (`symbol@window_idx`) pointing to the first non-empty slot the tool saw, so you can immediately inspect that window with other tooling if needed.

### Windowspace layout

`window-space` manages two mmap’d ledgers under `ledger.state/`:

```
ledger.state/
├── trade-ledger.dat          # rows per symbol × minute (rf_rate, option/underlying trades + quotes, aggressor slots)
├── enrichment-ledger.dat     # rows for enrichment slots (greeks, aggregations)
├── *.map                     # append-only payload mapping tables (rf_rate, trades, quotes, aggressor, greeks, aggregations)
└── aggregations/manual/...   # example of derived artifacts written by newer engines
```

Slot kinds:

| Ledger      | Slot kinds                                                                 |
|-------------|-----------------------------------------------------------------------------|
| Trade       | `rf_rate`, `option_trade`, `option_quote`, `underlying_trade`, `underlying_quote`, `option_aggressor`, `underlying_aggressor` |
| Enrichment  | `greeks`, `aggregation`                                                     |

All typed setters live on `WindowSpaceController` (e.g., `set_option_trade_ref`, `set_greeks_ref`, `set_aggregation_ref`). Writers append payload metadata to the matching `*.map` file, then call the setter with a `PayloadMeta` that includes `payload_id` and checksum. Artifacts live wherever the engine chooses (usually under `ledger.state/<engine>/…`); the mapping entry’s `artifact_uri` tells downstream readers where to find the bytes.

Quick check for file mtimes:

```bash
python - <<'PY'
from pathlib import Path
from datetime import datetime, timezone
base = Path("ledger.state")
if not base.exists():
    raise SystemExit(f"{base} not found")
for path in sorted(base.iterdir()):
    if path.is_file():
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        print(f"{path.name:25}  {mtime.isoformat()}")
PY
```

## Notes

Why I question the mid:

### 1. Underlying price signal

Let **(S_t)** be the underlying price signal a participant uses.

It can be:

* Last trade:
  (S_t = S^{\text{trade}}_t)

* Bid / ask / mid quote:
  (S_t = S^{\text{bid}}_t), (S^{\text{ask}}_t), or (S^{\text{mid}}_t)

---

### 2. Quoting function

Define the option quote as:

[ Q_t = f(S_t, \sigma_t, \text{other inputs}) ]

---

### 3. Relationship

The option quote is a *derivative* of the underlying price signal when:

[ \frac{\partial Q_t}{\partial S_t} \neq 0 ]

---

### 4. Interpretation

* If (S_t = S^{\text{trade}}_t), the option quote is driven by the **trade**.
* If (S_t = S^{\text{quote}}_t), the option quote is driven by the **quote**.

---
