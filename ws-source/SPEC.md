# ws-source + scheduler Specification (v0.1)

**Copyright (c) James Kassemi, SC, US. All rights reserved.**

## Overview
The ws-source module handles WebSocket ingestion for real-time data from Massive (or similar providers). It includes a connection manager for multiple resource types (options_quotes, options_trades, equity_quotes, equity_trades), shard-aware design to support up to N connections per type (currently enforced to 1 per type by Massive), and a subscription scheduler that prioritizes instruments based on decayed message rate/volume. The scheduler reserves capacity for top-N instruments and allocates the remainder to randomized exploration with hysteresis to avoid churn. Resilience features include ping/pong health checks, jittered backoff reconnection, subscription resumption, and backpressure handling. Emits DataBatch<T> streams with metadata for trades and NBBO.

## Acceptance Criteria
- Maintain exactly shard_count connections per resource type (default 1); reconnect with exponential backoff on failure; resume subscriptions on reconnect.
- Subscription scheduler: Update priorities every rebalance_interval_s; top_n instruments get reserved slots; exploration_fraction of remaining capacity randomized; hysteresis prevents frequent evictions.
- Emit DataBatch with correct source=Ws, quality=Prelim, watermark_ts_ns advancing on heartbeats/idle.
- Handle rate limits and backpressure: Shed low-priority instruments if processing lags beyond staleness limits; metrics for subs_active, evictions, reconnects.
- No data fabrication; fail-fast on invalid payloads; log errors with context.
- Performance: Sustain 10k+ msg/s per connection without exceeding 100ms classification staleness; memory usage < 1GB for 10k active instruments.

## Detailed API Contracts
### Structs and Enums
- `pub struct WsWorker { url: Url, shard_id: usize, resource_type: ResourceType }`
  - `resource_type`: Enum { OptionsQuotes, OptionsTrades, EquityQuotes, EquityTrades }
- `pub struct SubscriptionScheduler { top_n: usize, exploration_fraction: f64, rebalance_interval_s: u64, hysteresis: f64, priorities: HashMap<String, f64> }`
  - Methods:
    - `fn new(config: &SchedulerConfig) -> Self`
    - `fn update_priorities(&mut self, rates: HashMap<String, f64>)` // Decayed rates from metrics
    - `fn allocate_slots(&self, total_slots: usize) -> HashMap<String, bool>` // Returns active subs
- `pub struct ConnectionManager { workers: Vec<WsWorker>, scheduler: SubscriptionScheduler }`
  - Methods:
    - `fn new(config: &WsConfig) -> Self`
    - `async fn run(&self) -> Pin<Box<dyn Stream<Item = DataBatch<TradeOrNbbo>> + Send>>` // TradeOrNbbo is enum for OptionTrade|EquityTrade|Nbbo

### Traits
- `pub trait ShardAware { fn shard_count(&self) -> usize { 1 } }` // Implemented on ConnectionManager

### Key Functions
- `async fn connect_and_subscribe(worker: &WsWorker, subs: Vec<String>) -> Result<WebSocketStream, WsError>`
  - Handles auth, subscribe messages, ping/pong.
- `fn parse_payload(payload: &[u8]) -> Result<Vec<DataBatch<TradeOrNbbo>>, ParseError>`
  - Validates SIP timestamps, emits batches with meta.

## Test Vectors
### Unit Test: Scheduler Allocation
- Input: top_n=5, exploration_fraction=0.2, total_slots=10, priorities={"AAPL": 100.0, "TSLA": 90.0, ..., "XYZ": 10.0}
- Expected: Top 5 active, 2 random from remaining active; hysteresis ensures stability across calls.

### Integration Test: Reconnection
- Simulate disconnect after 5s; verify backoff (1s, 2s, 4s), reconnect, resume subs; emit empty batches during downtime.

### Property Test: Backpressure
- Input: High-rate stream (100k msg/s); verify shedding when queue > capacity; staleness < 200ms.

### Golden File: Payload Parsing
- Input: Canned Massive JSON for AAPL trade.
- Output: DataBatch with rows=[EquityTrade{...}], meta={source: Ws, quality: Prelim, watermark: 1234567890}
