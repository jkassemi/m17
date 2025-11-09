# ws-source

**Copyright (c) James Kassemi, SC, US. All rights reserved.**

WebSocket ingestion module for real-time trade and NBBO data. Includes connection management and subscription scheduling.

## Build and Run
```bash
cargo build
cargo test
```

## Dependencies
- tokio-tungstenite for WS connections.
- See SPEC.md for detailed contracts.
