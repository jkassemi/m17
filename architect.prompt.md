<command>cat README.md</command>
<command>cat SPEC.md</command>

<command>python context.py</command>

1) I'd like to get started with ingestion of flat file data from massive to fill the configured time windows (and to maintain the loop that updates as new data becomes available). We need the historical data (we can't rely on seeing a trade with the websocket). While I'd like to get started with equities, we need to consider the pipeline here and make sure the code we use can utilized by the WS service as well.
<command>cat docs/massive/flat-files-equity-trades.md</command>

2) Please consider the specification and assess against the in-progress implementation.

3. Break remaining tasks into a series of small steps.

--- LLM ---
Great — here’s a concrete plan to get equities flat-file ingestion from Massive going, keep it running to fill configured windows, and make it reusable by the WS pipeline.

1) Minimal, reusable design for Massive flat-file equities ingestion

What we’re ingesting

- Dataset: Massive us_stocks_sip/trades_v1 (equity trades).
- Cadence: One daily file per symbol-universe (published EOD; your plan indicates previous day is finalized/available by ~11a ET).
- Columns we’ll use immediately: ticker, sip_timestamp, price, size, exchange, conditions, id, sequence_number, participant_timestamp, correction, tape, trf_id, trf_timestamp.
- Mapping to our model:
  - ticker → EquityTrade.symbol
  - sip_timestamp → EquityTrade.trade_ts_ns (SIP event-time, nanoseconds)
  - price → EquityTrade.price
  - size → EquityTrade.size (u32; clamp/validate)
  - exchange → EquityTrade.exchange
  - conditions → EquityTrade.conditions (parse comma-separated ints)
  - id → EquityTrade.trade_id (add this field; see gaps below)
  - sequence_number → EquityTrade.seq (add optional field)
  - participant_timestamp → EquityTrade.participant_ts_ns (add optional field)
  - correction → EquityTrade.correction (add optional field)
  - tape → EquityTrade.tape (add optional field)
  - trf_id → EquityTrade.trf_id (add optional field)
  - trf_timestamp → EquityTrade.trf_ts_ns (add optional field)
- Classification fields (aggressor_*and nbbo_*) will remain Unknown/unset until we have NBBO. That’s OK for step one (we’re building the historical trade lake first).

Key ingestion components

- Planner (date-range aware):
  - Input: FlatfileConfig.date_ranges (already present).
  - Expands to a per-day plan.
  - Determines object keys for each day (configurable key template because Massive’s exact S3 key convention may vary).
  - Uses HEAD to decide whether a day is present/final and if the local cache is current (ETag/last-modified).
- Object store client:
  - Simple trait: head(key), list(prefix/date), get_stream(key) returning an async stream of bytes.
  - Implementations:
    - s3 (aws-sdk-s3) for Massive’s S3.
    - file:// (local files) for offline dev and tests.
    - Optional http(s) if Massive provides signed URLs.
- Reader/Parser:
  - Supports CSV/TSV gz (start with CSV + gzip; make format pluggable).
  - Uses a bounded async pipeline:
    - Download stream → decompressor → CSV parser → row normalizer → batcher.
  - Emits Stream<DataBatch<EquityTrade>> (e.g., batches of 50k rows).
  - Each batch includes DataBatchMeta:
    - source=Flatfile, quality=Final for dt < today; Prelim/Enriched if you decide to push current day (optional later).
    - watermark.watermark_ts_ns set to the max trade_ts_ns seen in the batch; completeness=Complete for finalized days.
    - schema_version set from core-types.
- Storage sink:
  - write_equity_trades(batch): partitions by dt=YYYY-MM-DD derived from trade_ts_ns; idempotent dedup (trade_id available).
  - Row group target ~128–256k rows; ZSTD compression.
- Metrics and status:
  - Counters: flatfile_download_bytes_total, flatfile_days_discovered_total, trades_rows_ingested_total, ingest_errors_total.
  - Gauges: flatfile_backlog_days, latest_day_finalized_ts, last_successful_ingest_ns.
  - Existing TUI already polls a status string; extend FlatfileSource to set metrics.set_flatfile_status with per-day state (discovered/downloading/ingested/finalized).

Scheduling and update loop

- For historical backfill:
  - Iterate all configured ranges and ingest missing days.
- For “maintenance”:
  - Revisit the most recent 2–3 days on a schedule:
    - Day D-1: check after 11a ET; if newer ETag/size, re-download.
    - Day D: optional; default off until we want near-real-time flatfiles.
- Idempotence:
  - For each day, either:
    - Write to a temp path and atomically move into place, or
    - Write new Parquet “part” files and rely on dedup keys upon read/compaction.
- Reuse by WS pipeline:
  - The output of the flatfile Reader/Parser is DataBatch<EquityTrade>, same as the stream shape we expect from the WS worker.
  - That lets the orchestrator feed the exact same downstream steps (storage, NBBO/classifier when we add them).

Configuration you’ll need to add

- config.flatfile.s3:
  - region, bucket, trades_prefix, endpoint (optional), concurrency, local_cache_path, format=csv|parquet, compression=gz|none.
- config.flatfile.days_to_recheck=2 (how many recent days to probe for updates).
- config.flatfile.batch_rows=50000.
- config.policy.historical_trades_quality=Final (default).
- config.policy.historical_allow_current_day=false.

2) Spec vs current implementation: gaps and fixes

High-impact gaps to address first for equities flat-file ingestion

- Types/schema
  - EquityTrade is missing: trade_id, seq, participant_ts_ns, tape, correction, trf_id, trf_ts_ns. Also the spec wants participant_ts and arrival_ts for diagnostics. Add optional fields so we can map Massive fields and dedup by trade_id as per spec.
  - core-types/schema.rs is placeholder; storage writers will not match. We need complete Arrow schema for equity trades and nbbo.
  - Add a trait DedupKey as in spec; implement for EquityTrade to use (symbol, trade_id) primarily; else fallback to composite key.
- Storage
  - equity_trades_to_record_batch currently returns an empty batch; needs a real implementation aligned with the new schema.
  - Partitioning currently uses watermark_ts_ns instead of per-row event-time; replace with per-row partitioning (group rows by day).
  - The instrument_type detection in write_partitioned is incorrect; simplify by table param or remove that concept for now.
  - Conditions currently serialized to string; we should store as Arrow List<Int32>.
- Flatfile source
  - run() is a no-op status loop. Needs a planner + S3 client + reader + parser + stream + metrics described above.
  - Provide get_equity_trades(scope) → Stream<DataBatch<EquityTrade>> that the DataClientRouter can use for Historical mode.
- DataClientRouter
  - Currently returns empty streams. It should route by scope.mode to flatfile-source for Historical and T-1.
- Orchestrator
  - Doesn’t build QueryScope or run a pipeline. For MVP, spawn a task:
    - Build scope for each day in ranges; consume stream; write to storage; update metrics; honor backpressure.
  - Config loading is currently gated on polygon keys. Since we’re starting with flatfiles, make those keys optional or only validated if polygon features are enabled. Right now, missing keys cause load() to return Err and the orchestrator falls back to defaults (dropping your flatfile date_ranges).
- NBBO/classifier
  - Both are stubs. That’s OK for step one (we’ll persist trades; classification to come once NBBO flatfiles are wired).
- Metrics/TUI
  - Metrics exist but aren’t updated by flatfile-source. Add updates for download/ingest progress and errors.
- Testing
  - No unit/integration tests for parser/ingest/storage. Add golden-file based tests per spec.

Lower-priority or follow-on gaps

- S3-compatible reader for Parquet/Arrow datasets (we’ll begin with CSV+GZ if that’s what Massive serves; support Parquet if available).
- Dedup on write and compaction pass.
- Configurable retries/backoff on S3 429/5xx with token-bucket throttle.
- Watermarks and finalization semantics across sources (for flatfiles, use batch max ts and completeness=Complete).

3) Concrete, small steps to completion

- A6. DataClientRouter and orchestrator wiring
  - Implement DataClient for FlatfileSource.get_equity_trades(scope).
  - In orchestrator: for each date range day, call get_equity_trades and pipe batches to Storage.write_equity_trades. Use a simple concurrency of 1–2 days at a time.
  - Acceptance: Running cargo run ingests a local sample, writes parquet under data/equity_trades/dt=YYYY-MM-DD, and metrics reflect ingestion.

- A7. Metrics and TUI polish
  - Update Metrics with counters and set_flatfile_status per day; TUI shows progress.
  - Acceptance: curl localhost:9090/metrics shows ingest counters moving; TUI updates.

Phase B: Productionize Massive S3 and scheduling

- B1. S3 client (aws-sdk-s3)
  - Implement S3 adapter; support region, bucket, prefix; HEAD and GET streaming; retries and backoff.
  - Acceptance: HEAD and GET succeed against Massive bucket using credentials.

- B2. Planner + maintenance loop
  - Expand Planner to periodically re-check last N days and discover new days; schedule downloads if newer ETag or missing locally.
  - Acceptance: if a day’s ETag changes, re-ingest and dedup; status reflects “updated”.

- B3. Robustness
  - Token-bucket per-host; retry on 429/5xx with jitter backoff; persist partial downloads to temp and resume or overwrite safely.
  - Acceptance: fault-injection test with forced retries completes successfully.

Phase C: Prepare reuse by WS and future classification

- C1. Make the flatfile output path identical to WS path
  - DataBatch shape and storage code are already shared; document the contract in core-types so WS can emit the same rows.
- C2. NBBO flatfile ingestion (equities)
  - Mirror A steps for NBBO dataset (if Massive provides it). Fill NbboStore and persist nbbo deltas.
- C3. Offline classifier
  - Implement the classifier with SIP event-time joining via NbboStore; run a “finalizer” pass over stored trades once NBBO is available; write upgrades as per spec.
- C4. Tests and golden days
  - Check in a tiny golden parquet for trades and NBBO; verify classification results repeat.

Technical notes and recommendations

- Object key template: make it a config string with date subst, e.g., s3://{bucket}/{prefix}/dt={YYYY-MM-DD}/equity_trades.csv.gz or path that matches Massive’s layout. You can support both “one file per day” and “many files per day” by listing under a date prefix and ingesting all parts.
- Memory and concurrency: keep batches around 50k rows, with a bounded channel between parser and writer. Use spawn_blocking for CSV parsing and decompression to avoid blocking Tokio.
- Timestamps: parse integers as ns; guard against invalid or zero; drop or mark completeness=Partial with reason hints if input is corrupted (and emit a metric).
- Quality and watermark: for historical finalized days, quality=Final, completeness=Complete. Watermark for each batch = max(trade_ts_ns) emitted in that batch; this is mostly diagnostic for flatfiles.
- Idempotence: if re-ingesting a day, write a new parquet part and rely on dedup keys in reads/compaction, or atomically replace the partition after successful write.

Immediate next actions

- Adjust AppConfig::load to not require polygon keys so we can use config.toml as-is.
- Add missing fields to EquityTrade and define the full Arrow schema for equities.
- Implement equity_trades_to_record_batch with real arrays and per-row partitioning by dt.
- Implement FlatfileSource CSV gz reader + local file adapter; wire DataClientRouter and orchestrator to stream → storage.
- Add metrics increments and TUI status updates.
- Validate with a small local sample (copy a Massive sample file to a local path configured in flatfile.s3 with the file:// adapter) and confirm parquet output.

Once we have this running for equities, we can replicate the pattern for NBBO and options, and later let WS share the same normalization and storage pipeline.
