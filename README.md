# Trade Capture M17

## Overview
This system captures trades with NBBO snapshots, classifies aggressors, calculates greeks, stores data in Parquet, and provides a dashboard for realtime application of trading algorithms and strategies.

```bash
cargo build
cargo run  # Starts orchestrator with WS stub and metrics at http://localhost:9090/metrics
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

## Architecture
- `core-types`: Shared schemas and config.
- `ws-source`: WebSocket ingestion.
- `orchestrator`: Main runtime.
