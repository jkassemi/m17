# Project overview and specification (v0.1)

**Copyright (c) James Kassemi, SC, US. All rights reserved.**

## License notice
- This software and all derived products must be licensed with James Kassemi, currently of SC, US.
- Include this header in all source files:
  - Copyright (c) James Kassemi, SC, US. All rights reserved.

## Goals
- Capture every trade with the closest valid NBBO snapshot using SIP event-time, classify aggressor with explicit, defensible rules.
- Real-time first; T-1 and historical paths fill and finalize; never fabricate data.
- Single binary, Tokio runtime; resilient to drops and rate limits; observable; idempotent updates.

## Summary of goals and constraints
- Capture each trade with the closest valid NBBO snapshot; classify aggressor only with defensible rules; never fabricate.
- Modes: real-time (WS), T-1 gap (REST throttled/sampled), historical (flatfiles).
- Single binary, local SSD (1 TB) for a few months retention.
- Fail fast when expected data is missing; strong observability and idempotent processing.
- One WS connection per resource type; options quotes limited to 1,000 contracts per connection.
- Rust + Tokio; no organizational constraints; free tech only.
- Prefer Arrow/Parquet/Polars; avoid heavy SQL; DuckDB OK for ad-hoc, but not a dependency.
- We can compute per-trade in real time; batched processing used only where it helps I/O.

## Teams
- core-types: All shared schemas, enums, error types, feature flags.
- data-client: DataClient trait and router; implementations for:
  - flatfile-source: S3-compatible bucket reader for Parquet/Arrow datasets.
  - rest-source: Massive REST client with rate limiting, retries, deltas.
  - ws-source: WS connection manager + subscription scheduler (1k options/conn).
- nbbo-cache: In-memory NBBO store with staleness policies; per-instrument ring buffers; snapshot query API.
- classifier: Aggressor stage, per-trade, deterministic; locked/crossed policies; tick-size handling; finalizer (T+1).
- storage: Parquet writer/reader, partitioning, compaction; optional DuckDB (ad-hoc only).
- orchestrator: Tokio runtime, pipelines, watermarks, idempotent upserts, recovery; “fail hard” checks, market calendar.
- metrics: Prometheus + tracing integration; per-connection stats, NBBO staleness histograms, 429 counters.
- tui: User interaction and control plane; symbol selection, algorithms, status.
- replayer (optional): Offline replay of flatfiles through the RT pipeline for testing.

## Top-level architecture (single binary, modular crates)
- core-types
  - Shared structs/enums, schema versions, error types, feature flags.
  - Normalized row schemas for trades and NBBO; row-level source/quality/watermark.
- data-client
  - Uniform DataClient trait with get_option_trades, get_equity_trades, get_nbbo.
  - Router for modes: Realtime (WS), T-1 (REST), Historical (flatfile).
- ws-source
  - WebSocket connection manager(s), subscription scheduler, shard-aware (options quotes/trades).
  - Emits DataBatch<Nbbo|Trade> with meta.
- rest-source
  - Async HTTP client with per-host rate limiting, retries, per-instrument watermarks, optional sampled quotes diagnostics.
- flatfile-source
  - S3-compatible reader for Parquet/Arrow datasets; fail-fast by market calendar.
- nbbo-cache
  - In-memory NBBO computation and store; quote-state tracking (Normal/Locked/Crossed); adaptive staleness; allowed lateness.
- classifier
  - Stateless, deterministic aggressor classification for each trade; locked/crossed policies; tick-size handling; finalizer (T+1).
- storage
  - Parquet writer/reader; partitioning, compaction; idempotent upserts; dedup keys.
- orchestrator
  - Tokio runtime, pipelines, watermarks, health, backpressure; configuration; TUI control plane.
- metrics
  - Prometheus exporter; tracing integration; predefined metrics and alerts.
- tui
  - Live control and status; select symbols; adjust policies; show metrics.

## Normalization and schemas
Common enums
- Source: Flatfile, Rest, Ws
- Quality: Prelim, Enriched, Final
- Completeness: Complete, Partial, Unknown
- InstrumentType: Equity, Option
- AggressorSide: Buyer, Seller, Unknown
- ClassMethod: NbboTouch, NbboAtOrBeyond, TickRule, Unknown
- NbboState: Normal, Locked, Crossed
- TickSizeMethod: FromRules, InferredFromQuotes, DefaultFallback

### Shared metadata
- Watermark: watermark_ts_ns (i64), completeness (Completeness), hints (String optional)
- Schema version: u16 stored in Arrow schema metadata, mirrored in files

### Option trade row
- contract (OPRA string)
- contract_direction (‘C’ or ‘P’)
- strike_price (f64)
- underlying (string)
- trade_ts_ns (i64, SIP event-time)
- price (f64)
- size (u32)
- conditions (Vec<i32>)
- exchange (i32)
- expiry_ts_ns (i64)
- aggressor_side (AggressorSide)
- class_method (ClassMethod)
- aggressor_offset_mid_bp (Option<i32>)
- aggressor_offset_touch_ticks (Option<i32>)
- nbbo_bid, nbbo_ask (Option<f64>)
- nbbo_bid_sz, nbbo_ask_sz (Option<u32>)
- nbbo_ts_ns (Option<i64>)
- nbbo_age_us (Option<u32>)
- nbbo_state (Option<NbboState>)
- tick_size_used (Option<f64>)
- greeks: { delta, gamma, vega, theta, iv } (Option<f64> each), greeks_flags (u32)
- source (Source), quality (Quality), watermark_ts_ns (i64)

### Equity trade row
- symbol (string)
- trade_ts_ns (i64, SIP event-time)
- price (f64)
- size (u32)
- conditions (Vec<i32>)
- exchange (i32)
- aggressor_side, class_method, offsets, nbbo_* fields, nbbo_state, tick_size_used
- source, quality, watermark_ts_ns

### NBBO row (derived from consolidated stream)
- instrument_id (OPRA or symbol)
- quote_ts_ns (i64, SIP)
- bid, ask (f64)
- bid_sz, ask_sz (u32)
- state (NbboState)
- condition (Option<i32>)
- best_bid_venue, best_ask_venue (Option<i32>)
- source, quality, watermark_ts_ns

## Key invariants
- All classification uses SIP event-time only.
- No classification when NBBO state is Locked or Crossed unless a specific policy is configured; default is Unknown.
- No “closest side” heuristics; only touch or at/beyond, else Unknown or TickRule if explicitly allowed.
- Row-level source/quality/watermark are persisted; in-memory batches may carry meta for performance but must materialize on write.
- Deduplication by unique keys; idempotent upserts.

## Interfaces (shared)
- DataClient
  - async fn get_option_trades(scope: QueryScope) -> Stream<DataBatch<OptionTrade>>
  - async fn get_equity_trades(scope: QueryScope) -> Stream<DataBatch<EquityTrade>>
  - async fn get_nbbo(scope: QueryScope) -> Stream<DataBatch<Nbbo>>
- QueryScope
  - instruments (InstrumentSet), time (TimeRange), mode (Realtime|T1|Historical), quality_target (Prelim|Final)
- DataBatch<T>
  - rows (Vec<T>), meta (Arc<DataBatchMeta>)
- DataBatchMeta
  - source, quality, watermark (Watermark), schema_version
- NbboStore
  - put(quote: &Nbbo)
  - get_best_before(id: &str, ts_ns: i64, max_staleness_us: u32) -> Option<Nbbo>
  - get_state_before(id: &str, ts_ns: i64) -> Option<NbboState>
  - adaptive_params(id) -> StalenessParams
- Classifier
  - classify_trade(trade: &mut TradeLike, nbbo: &NbboStore, params: &ClassParams)
- ClassParams
  - use_tick_rule_fallback (bool)
  - epsilon_price (f64)
  - allowed_lateness_ms (u32) for event-time finalization; used by orchestrator

## NBBO ground truth policy
- Compute NBBO from consolidated quote stream per instrument.
- Maintain best bid/ask across venues; mark state: Normal/Locked/Crossed.
- Classification is allowed only when:
  - A quote exists at or before trade_ts within max_staleness_us for that instrument, and state == Normal.
- Adaptive staleness per instrument:
  - Maintain sliding window of observed nbbo_age_us (trade_ts - nbbo_ts for trades seen).
  - Max staleness = clamp(p95..p99, per-asset bounds).
  - Bounds defaults:
    - Options: 10–150 ms
    - Equities: 5–50 ms
  - Expose metrics and live updates; store the value used at classification time.
- Allowed lateness (event-time):
  - Keep per-instrument high-watermark by event-time.
  - Global watermark = min_i(HWM_i − allowed_lateness).
  - Finalize classifications once global watermark passes; pending trades within lateness window can be upgraded if earlier quotes arrive.

## Locked/crossed handling
- If NBBO state is Locked or Crossed at the chosen quote, do not classify; mark Unknown and nbbo_state accordingly.
- Optionally defer classification within allowed lateness to await resolution; if resolved, classify; else Unknown.

## Tick-size correctness
- Maintain per-class tick rules (Penny Pilot/thresholds). Prefer definitive rules from contract metadata or exchange class if available.
- Fallback inference: estimate from recent quote increments; mark tick_size_method = InferredFromQuotes.
- As a last resort, use a conservative default and mark DefaultFallback; never use inferred sizes without marking.
- tick_size_used persisted with each classified trade.

## Aggressor classification rules (deterministic)
- Find NBBO where quote_ts <= trade_ts and age <= max_staleness_us; state must be Normal.
- If price >= ask − epsilon_price → Buyer (NbboTouch or NbboAtOrBeyond if price > ask).
- If price <= bid + epsilon_price → Seller (NbboTouch or NbboAtOrBeyond if price < bid).
- Else Unknown.
- If no valid NBBO and tick-rule fallback is enabled (RT default off, T-1 configurable on):
  - Compare to last trade price for instrument by event-time: up → Buyer, down → Seller, equal → Unknown.
- Persist:
  - class_method, aggressor_side, offsets, nbbo_* snapshot, nbbo_age_us, nbbo_state, tick_size_used.
- Never revise raw trade fields; only aggressor and derived columns may be updated by a finalizer.

## WebSocket strategy
- Connections
  - Resource types: options_quotes, options_trades, equity_quotes, equity_trades.
  - Sharded design: support N connections per type, each up to 1,000 option contracts (quotes) or implementation-specific limits for trades.
  - If Massive enforces single connection per type, the scheduler must operate with capacity=1; code should handle both via a configurable shard_count.
- Subscription scheduler
  - Maintain a priority score per contract: decayed message rate and/or volume.
  - Reserve capacity for top-N; allocate remainder to randomized exploration with hysteresis to avoid churn.
  - Rate-limit subscribe/unsubscribe ops; back off on errors; metrics for subs_active, evictions.
- Resilience
  - Ping/pong health checks; reconnect with jittered backoff; resume subscriptions; replay gaps via REST only if configured.
  - Backpressure: if incoming rate exceeds processing, increase flush frequency and consider shedding low-priority instruments; never exceed classification staleness limits.

## T-1 gap policy (REST)
- Options: trades-only by default; quotes sampled for diagnostics only.
- Underlyings: trades + quotes allowed due to small universe.
- Classification: Unknown (or tick-rule if explicitly on).
- Overnight finalizer reclassifies with flatfile quotes; upgrades aggressor and dependent metrics; marks quality Final.

## Historical (flatfiles)
- S3-compatible flatfiles; fail-fast if a market day is missing (use embedded NYSE calendar with overrides).
- Process in event-time order; compute NBBO and classify or mark Unknown if policy requires; produce Final quality.

## Event-time fields and deduplication
- Use SIP timestamps for alignment. Record participant_ts and arrival_ts separately for diagnostics; not used for classification.
- Dedup keys:
  - If trade_id present: (instrument, trade_id).
  - Else: (instrument, trade_ts_ns, price, size, exchange, seq if present).
- Dedup at ingestion (in-memory LRU Bloom or hash set per instrument) and on write (stable sort + unique by key).

## Storage and retention
- Format: Parquet with Arrow schemas; zstd compression.
- Raw zone (append-only):
  - options_trades, options_nbbo, equity_trades, equity_nbbo
  - Partition: dt=YYYY-MM-DD, instrument_type, prefix (symbol/opra prefix).
  - Sort keys: [instrument_id, ts].
  - Row group: 128–256k rows; target files 64–256 MB.
- Derived zone:
  - Same rows with aggressor fields; later: add frames/aggregations.
  - Finalizer writes column patch files affecting only aggressor_* and greeks_*, quality.
- Volume budget:
  - Quotes dominate. Persist only NBBO deltas (state or price/size changes) to reduce footprint.
  - Retention: keep raw trades and NBBO deltas for recent weeks; offload older to S3; optional deletion of raw quotes after finalization.

## Greeks budget
- Compute per trade only when:
  - |log_moneyness| < threshold (configurable) or notional above threshold; else defer.
- Use bounded CPU pool; backpressure when queue exceeds budget; persist greeks_flags for exceptions (below intrinsic, above max).

## Watermarks and finalization
- Per-instrument HWM by event-time for trades and quotes; global watermark = min(HWM − allowed_lateness).
- WS: emit heartbeats to advance HWM when idle.
- Finalize all trades with trade_ts <= global watermark; re-open if late data arrives only via explicit reprocess.
- T+1 finalizer: reclassify using flatfile NBBO; produce upgrades; idempotent (same input → identical output).

## Observability and alerts
- Prometheus metrics (examples)
  - classify_unknown_rate{instrument_type}
  - nbbo_age_us histogram
  - ws_connection_up{type, shard}, ws_reconnects_total, ws_subs_active, ws_shards
  - rest_requests_total{endpoint,status}, rest_429_total, rest_retry_total
  - dedup_dropped_total, staleness_budget_violations_total
  - finalizer_upgrades_total
  - storage_compaction_bytes, disk_headroom_percent
- Tracing
  - structured spans for WS events, classification, writes; sample at rate; error logs with context.
- Alerting (recommendations)
  - Unknown rate high (threshold per asset class and time-of-day)
  - NBBO age p95 breaches staleness bound
  - WS shards down > X minutes
  - 429 spikes sustained
  - Disk headroom < 10%
  - Finalizer backlog > 1 day

## Resilience and rate limits
- Token-bucket rate limiter per host; inspect 429 and Retry-After; exponential backoff with jitter.
- Coalesce duplicate requests; cache empty responses with TTL.
- Persist deltas frequently (per minute) to survive retries without refetch.

## Configuration
- Single TOML/ENV config; hot-reload where safe.
- Key knobs:
  - ws.shards.{options_quotes, options_trades, equity_quotes, equity_trades}
  - staleness.bounds, staleness.quantile (p95..p99), allowed_lateness_ms
  - classifier.use_tick_rule_rt (default false), classifier.use_tick_rule_t1 (default true)
  - greeks.moneyness_threshold, greeks.notional_threshold, greeks.pool_size
  - storage.paths, row_group_target, compaction thresholds, retention windows
  - rest.rate_limits and retry policy
  - scheduler.top_n, exploration_fraction, rebalance_interval_s, hysteresis

## Coding standards and best practices
- Rust 2021 stable. Enforce rustfmt and clippy (pedantic) in CI; deny warnings in CI gates.
- Error handling: thiserror + anyhow; no unwrap/expect in production code; propagate context with anyhow::Context.
- Concurrency: Tokio; prefer async channels (tokio::mpsc) with bounded capacity; avoid blocking in async.
- Time: use i64 ns for event-time fields; tokio time for wall-clock; never mix. Convert once at ingress.
- Floating point: f64 for prices/greeks; epsilon comparisons documented; avoid NaN propagation; validate inputs.
- Data structures: dict-of-arrays inside batches to minimize allocations; build Arrow arrays once per write.
- Logging: tracing with fields; no PII; rate-limit noisy logs.
- Config-driven behavior; sensible defaults checked into repo.

## Testing requirements
- Unit tests
  - Classifier decision table coverage: touch, at/beyond, inside, locked, crossed, tick-rule, epsilon boundaries.
  - NBBO cache: order, staleness selection, state transitions, adaptive staleness updates.
  - Tick-size inference and rule application; method flags set correctly.
- Property tests (proptest)
  - Monotone quote/trade streams; out-of-order within allowed lateness; ensure no classification with stale quotes beyond bound.
  - Locked/crossed sequences that resolve; classification transitions are consistent and idempotent.
- Integration tests
  - WS replay harness reading canned Massive payloads; ensure stable outputs and watermarks.
  - REST/T-1 path delta fetch, 429 handling, retries, dedup.
  - Finalizer idempotence: reruns produce identical outputs; only aggressor/greeks mutated.
- Golden files
  - Known days and symbols; commit small golden parquet for CI to verify schema and decisions.
- Performance tests
  - Throughput and latency under realistic message rates; backpressure triggers; flush timing.
- Soak tests
  - Long-running WS sessions with induced disconnects; memory bounds, leak checks.
- Determinism
  - Replay the same inputs yields identical outputs including order within a partition; version changes bump schema_version and fixtures.

- WS connections per resource type
  - Massive enforces a single connection per type, shard_count must be 1; keep scheduler/shard abstraction to enable future scaling if limits change.

- Default adaptive staleness
  - Initial bounds and quantile:
    - Options: clamp 10–150 ms at p99
    - Equities: clamp 5–50 ms at p99
    - Open vs midday profiles optional; start static, revisit after data.
- Tick-rule fallback
  - RT default off; T-1 default on; both configurable.
- Retention policy
  - Keep trades and NBBO deltas for N weeks locally; offload older data to S3; drop old raw quotes after finalization.

## Delivery milestones
- M1: core-types, storage, metrics, orchestrator skeleton, config, calendar, license headers in repo.
- M2: ws-source for equities (trades + NBBO), nbbo-cache, classifier, parquet persistence, Prometheus exporter, TUI minimal.
- M3: ws-source for options with shard abstraction, scheduler, adaptive staleness, locked/crossed handling, tick-size rules.
- M4: rest-source for T-1 trades, sampled quotes diagnostics, per-instrument watermarks, retries/backoff.
- M5: flatfile-source and finalizer; column patch writes; idempotent upgrades; retention and compaction jobs.
- M6: performance and soak tests; polish metrics and alerts; optional DuckDB for ad-hoc queries.

## Appendix: exemplar Rust signatures (abbreviated)

- Enums and structs
  - See schema lists above; derive Serialize/Deserialize, Arrow, and Parquet writers; schema_version in metadata.

- DataClient trait
  - async fn get_option_trades(&self, scope: QueryScope) -> impl Stream<Item = DataBatch<OptionTrade>>;
  - async fn get_equity_trades(&self, scope: QueryScope) -> impl Stream<Item = DataBatch<EquityTrade>>;
  - async fn get_nbbo(&self, scope: QueryScope) -> impl Stream<Item = DataBatch<Nbbo>>;

- NbboStore
  - fn put(&self, q: &Nbbo);
  - fn get_best_before(id: &str, ts_ns: i64, max_staleness_us: u32) -> Option<Nbbo>;
  - fn get_state_before(id: &str, ts_ns: i64) -> Option<NbboState>;

- Classifier
  - fn classify_trade(&self, trade: &mut TradeLike, nbbo: &NbboStore, params: &ClassParams);

- Dedup key
  - trait DedupKey { fn key(&self) -> DedupKeyHash; }

## Operational notes
- Never halt the entire pipeline on partial data; mark completeness=Partial, continue, and alert.
- Emit reason codes in metadata hints when degrading to Unknown (e.g., stale_nbbo, locked_book).
- Keep an explicit “policy_id” string in config; write it to parquet metadata per run for auditability.

