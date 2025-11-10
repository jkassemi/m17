<command>cat README.md</command>
<command>cat SPEC.md</command>

<command>python context.py</command>

```
┌Ingestion Progress──────────────────────────────────────────────────────────────────┐
│██████████████████████████████████████Days 8/8 █████████████████████████████████████│
└────────────────────────────────────────────────────────────────────────────────────┘
┌Dashboard───────────────────────────────────────────────────────────────────────────┐
│Component Status Dashboard                                                          │
│                                                                                    │
│Metrics Server: Running on 127.0.0.1:9090                                           │
│Last request: Never                                                                 │
│Ingested day: 2025-11-07 (35389 batches, 35388896 rows)                             │
│Last config reload: 27397420 ms ago                                                 │
│Batches processed: 111952                                                           │
│Rows ingested: 111951896                                                            │
│                                                                                    │
│Press 'q' to quit.                                                                  │

```

1. Please consider the specification and assess against the in-progress implementation.
2. I'm currently running the trade ingestion from Massive on my server. It seems like this should take much less time than it is, but they are large files. Is it possible to stream the compressed content and process the trades as we're doing so, displaying progress along the way?
3. Break remaining tasks into a series of small steps. At a high level, I'd like to get options data from the datafiles first, then bs calculation, and then I need to get aggregations in place - I'd like these to run these at configurable window sizes with config.toml settings, but start with (1m, 5m, 15m, and 30m). The real time aggregations will target a single symbol (also config.toml). The aggregations will need to have the following in addition to ticker and timestamp information, where dadvv is delta adjusted dollar value volume, and gadvv is gamma adjusted dollar value volume. I'd like to use the black_scholes crate. It's critical I get to the following as soon as possible:

FEATURE_NAMES = [
    "underlying_price_open",
    "underlying_price_high",
    "underlying_price_low",
    "underlying_price_close",
    "underlying_dollar_value_total", # total (sum of all trades within the distribution)
    "underlying_dollar_value_minimum",
    "underlying_dollar_value_maximum",
    "underlying_dollar_value_mean",
    "underlying_dollar_value_stddev",
    "underlying_dollar_value_skew",
    "underlying_dollar_value_kurtosis",
    "underlying_dollar_value_iqr",
    "underlying_dollar_value_mad",
    "underlying_dollar_value_cv",
    "underlying_dollar_value_mode",
    "underlying_dollar_value_bc",
    "underlying_dollar_value_dip_pval",
    "underlying_dollar_value_kde_peaks",
    "puts_below_intrinsic_pct",
    "puts_above_intrinsic_pct",
    "puts_dadvv_total",
    "puts_dadvv_minimum",
    "puts_dadvv_maximum",
    "puts_dadvv_mean",
    "puts_dadvv_stddev",
    "puts_dadvv_skew",
    "puts_dadvv_kurtosis",
    "puts_dadvv_iqr",
    "puts_dadvv_mad",
    "puts_dadvv_cv",
    "puts_dadvv_mode",
    "puts_dadvv_bc",
    "puts_dadvv_dip_pval",
    "puts_dadvv_kde_peaks",
    "puts_gadvv_total",
    "puts_gadvv_minimum",
    "puts_gadvv_maximum",
    "puts_gadvv_mean",
    "puts_gadvv_stddev",
    "puts_gadvv_skew",
    "puts_gadvv_kurtosis",
    "puts_gadvv_iqr",
    "puts_gadvv_mad",
    "puts_gadvv_cv",
    "puts_gadvv_mode",
    "puts_gadvv_bc",
    "puts_gadvv_dip_pval",
    "puts_gadvv_kde_peaks",
    "calls_dollar_value",
    "calls_above_intrinsic_pct",
    "calls_dadvv_total",
    "calls_dadvv_minimum",
    "calls_dadvv_maximum",
    "calls_dadvv_mean",
    "calls_dadvv_stddev",
    "calls_dadvv_skew",
    "calls_dadvv_kurtosis",
    "calls_dadvv_iqr",
    "calls_dadvv_mad",
    "calls_dadvv_cv",
    "calls_dadvv_mode",
    "calls_dadvv_bc",
    "calls_dadvv_dip_pval",
    "calls_dadvv_kde_peaks",
    "calls_gadvv_total",
    "calls_gadvv_minimum",
    "calls_gadvv_q1",
    "calls_gadvv_q2",
    "calls_gadvv_q3",
    "calls_gadvv_maximum",
    "calls_gadvv_mean",
    "calls_gadvv_stddev",
    "calls_gadvv_skew",
    "calls_gadvv_kurtosis",
    "calls_gadvv_iqr",
    "calls_gadvv_mad",
    "calls_gadvv_cv",
    "calls_gadvv_mode",
    "calls_gadvv_bc",
    "calls_gadvv_dip_pval",
    "calls_gadvv_kde_peaks",
]

**Then**, as a lowest priority, aggression categorization, which will ultimately result in the following:

    "calls_dadvv_agg_buy_total",  # Calls @ Ask (Aggressive Bullish)
    "calls_dadvv_agg_buy_minimum",
    "calls_dadvv_agg_buy_q1",
    "calls_dadvv_agg_buy_q2",
    "calls_dadvv_agg_buy_q3",
    "calls_dadvv_agg_buy_maximum",
    "calls_dadvv_agg_buy_mean",
    "calls_dadvv_agg_buy_stddev",
    "calls_dadvv_agg_buy_skew",
    "calls_dadvv_agg_buy_kurtosis",
    "calls_dadvv_agg_buy_iqr",
    "calls_dadvv_agg_buy_mad",
    "calls_dadvv_agg_buy_cv",
    "calls_dadvv_agg_buy_mode",
    "calls_dadvv_agg_buy_bc",
    "calls_dadvv_agg_buy_dip_pval",
    "calls_dadvv_agg_buy_kde_peaks",
    "calls_dadvv_agg_sell_total",  # Calls @ Bid (Aggressive Bearish)
    "calls_dadvv_agg_sell_minimum",
    "calls_dadvv_agg_sell_q1",
    "calls_dadvv_agg_sell_q2",
    "calls_dadvv_agg_sell_q3",
    "calls_dadvv_agg_sell_maximum",
    "calls_dadvv_agg_sell_mean",
    "calls_dadvv_agg_sell_stddev",
    "calls_dadvv_agg_sell_skew",
    "calls_dadvv_agg_sell_kurtosis",
    "calls_dadvv_agg_sell_iqr",
    "calls_dadvv_agg_sell_mad",
    "calls_dadvv_agg_sell_cv",
    "calls_dadvv_agg_sell_mode",
    "calls_dadvv_agg_sell_bc",
    "calls_dadvv_agg_sell_dip_pval",
    "calls_dadvv_agg_sell_kde_peaks",
    "puts_dadvv_agg_buy_total",  # Puts @ Ask (Aggressive Bearish)
    "puts_dadvv_agg_buy_minimum",
    "puts_dadvv_agg_buy_q1",
    "puts_dadvv_agg_buy_q2",
    "puts_dadvv_agg_buy_q3",
    "puts_dadvv_agg_buy_maximum",
    "puts_dadvv_agg_buy_mean",
    "puts_dadvv_agg_buy_stddev",
    "puts_dadvv_agg_buy_skew",
    "puts_dadvv_agg_buy_kurtosis",
    "puts_dadvv_agg_buy_iqr",
    "puts_dadvv_agg_buy_mad",
    "puts_dadvv_agg_buy_cv",
    "puts_dadvv_agg_buy_mode",
    "puts_dadvv_agg_buy_bc",
    "puts_dadvv_agg_buy_dip_pval",
    "puts_dadvv_agg_buy_kde_peaks",
    "puts_dadvv_agg_sell_total",  # Puts @ Bid (Aggressive Bullish)
    "puts_dadvv_agg_sell_minimum",
    "puts_dadvv_agg_sell_q1",
    "puts_dadvv_agg_sell_q2",
    "puts_dadvv_agg_sell_q3",
    "puts_dadvv_agg_sell_maximum",
    "puts_dadvv_agg_sell_mean",
    "puts_dadvv_agg_sell_stddev",
    "puts_dadvv_agg_sell_skew",
    "puts_dadvv_agg_sell_kurtosis",
    "puts_dadvv_agg_sell_iqr",
    "puts_dadvv_agg_sell_mad",
    "puts_dadvv_agg_sell_cv",
    "puts_dadvv_agg_sell_mode",
    "puts_dadvv_agg_sell_bc",
    "puts_dadvv_agg_sell_dip_pval",
    "puts_dadvv_agg_sell_kde_peaks",

--- LLM ---
Here’s a concise plan and set of concrete fixes to move you forward fast.

1) Assessment vs SPEC (what’s good, what’s missing)

What’s good

- Crate layout matches the spec: core-types, data-client, flatfile-source, nbbo-cache, classifier, storage, orchestrator, metrics, tui, ws-source, replayer.
- Orchestrator/TUI/metrics wire up and run. The flatfile equities ingestion path works end-to-end with progress visible in TUI.
- Storage writes Parquet and has a basic dedup pass. Tests cover equity trade serialization and dedup.
- Flatfile source can pull from S3-compatible Massive flatfiles and from local fixtures.

Urgent gaps vs spec

- Streaming: process_equity_trades_stream collects the entire compressed file into memory before parsing; this blocks for large files and hides progress. The spec wants real-time-first with strong backpressure and streaming.
- Parquet file sizing: write_partitioned writes a brand-new Parquet file per batch. You’re generating tens of thousands of small files; this is a major performance killer. Spec asks for 64–256 MB targets and 128–256k row groups.
- Schemas: core-types/schema::option_trade_schema is a stub (single “contract” column) while storage::option_trades_to_record_batch builds 30+ arrays. This will break once you write options. Equity schema is more aligned but still needs a versioned metadata approach per spec.
- Classifier/NBBO cache: stubs. Spec requires deterministic classification on SIP time, locked/crossed policies, adaptive staleness, allowed lateness and finalizer. None of that exists yet.
- Options: no options ingestion yet; Greeks fields exist on OptionTrade but aren’t computed. Spec calls out when/where to compute Greeks and flags.
- Watermarks/quality: partial. The orchestrator doesn’t set per-instrument HWMs or do allowed-lateness finalization; metadata is not persisted through upgrades (finalizer not implemented).
- Observability: metrics exist but lack key counters in spec (staleness histograms, ws/429, unknown rate by instrument_type). TUI shows day progress, but not per-file progress.
- REST/WS: stubs; subscription scheduler, resilience/backpressure not implemented.

2) Faster ingestion: stream compressed content, process on the fly, show progress

Yes. Do these three changes:

A) Stream parse CSV from an AsyncRead without buffering the whole file

- Change FlatfileSource to expose an AsyncRead (+ content length), not a Stream<Bytes>. For local files, File implements AsyncRead; for S3, resp.body.into_async_read() does too.
- Wrap the AsyncRead in a gzip decoder on the fly (async-compression’s GzipDecoder<AsyncRead>).
- Parse using csv_async::AsyncReader to iterate records without buffering everything.
- Emit DataBatch as you go, honoring BATCH_SIZE and backpressure (bounded mpsc::Sender).

B) Track and display progress

- Wrap the AsyncRead in a CountingReader that increments an AtomicU64 with each read.
- For remote, fetch Content-Length via head_object to get compressed_length; for local, get file metadata len.
- Add metrics fields: current_file, current_file_bytes_total, current_file_bytes_read, and a setter to update a float ratio and throughput_mb_s (using a simple moving average).
- TUI: add a second Gauge labeled “Current file” showing ratio and “X MB / Y MB, Z MB/s”.

C) Fix Parquet writer to avoid tiny files

- Implement a pooled writer per partition path (e.g., base/table/dt=YYYY-MM-DD/prefix=XX). Keep an ArrowWriter open and append record batches until you hit:
  - row_group_target (config), and
  - file size target (e.g., 128–256 MB)
- Rotate to a new file when thresholds are reached.
- Add a periodic flush and a shutdown flush. This alone will drastically improve throughput.

Sketch of the necessary changes

- flatfile-source Cargo.toml: add csv-async = "1", and keep async-compression.
- FlatfileSource: replace get_stream with get_reader
  - For local: open File, reader = BufReader<File>, total = file.metadata().len()
  - For S3: head_object for content_length; get_object().body.into_async_read()
  - Wrap with CountingReader(AsyncRead), then if .gz, wrap CountingReader with GzipDecoder
  - Return (Box<dyn AsyncRead + Unpin + Send>, Option<u64>, Arc<AtomicU64>)
- process_equity_trades_stream to process_equity_trades_streaming:
  - Use csv_async::AsyncReader::from_reader(decoder)
  - Loop read_record().await; build EquityTrade rows; once rows.len == BATCH_SIZE, send DataBatch
  - Every N records or every 250 ms, compute progress = bytes_read / total and update metrics
- metrics: add
  - set_current_file(name: String)
  - set_current_file_progress(pct: f64)
  - set_current_file_throughput(mbps: f64)
  - TUI: display another gauge and small stats line
- storage:
  - Add a WriterManager with HashMap<PartitionKey, PartitionWriter>
  - PartitionWriter: { file, ArrowWriter, rows_in_group, bytes_written, created_at }
  - write_equity_trades: route rows by date -> get writer -> build RecordBatch -> writer.write() -> rotate if thresholds exceeded
  - Implement flush_all() and call it at end/shutdown
  - Use config.storage.row_group_target to decide when to create a new row group; target file size ~128–256 MB via bytes_written
- Optional: increase concurrent days from 2 to 4–8 if IO bound; keep bounded write queue to avoid unbounded memory.

Why this will be much faster

- No full-file buffering; constant memory.
- Immediate processing; progress updates accurate.
- Far fewer, much larger Parquet files with large row groups; massive reduction in filesystem and parquet metadata overhead.

3) Next steps, broken into small, high-impact tasks

Phase 0: unblock throughput and visibility (highest ROI)

- Flatfile streaming as described above; add progress gauge and MB/s in TUI.
- Storage writer pooling with row_group_target and file size rotation.
- Fix option_trade_schema to match OptionTrade fields and add schema_version in Arrow metadata. Do the same sanity check for equity schema.
- Add config knobs:
  - ingest.batch_size (default 20k)
  - ingest.concurrent_days (default 4)
  - storage.row_group_target (default 128k), storage.file_size_mb_target (default 128)
  - tui.progress_update_ms (default 250)
- Deliverable: ingestion of large days completes much faster with clear progress; Parquet files sized correctly.

Phase 1: options ingestion from flatfiles (your priority)

- Add FlatfileSource::get_option_trades with streaming CSV parse. Use correct Massive options flatfile paths (e.g., us_options_opra/trades_v1/YYYY/MM/YYYY-MM-DD.csv.gz or similar; confirm actual bucket layout).
- Map CSV columns to OptionTrade, including:
  - contract (OPRA), contract_direction, strike_price, expiry_ts_ns, underlying
  - trade_ts_ns (SIP), price, size, conditions, exchange
  - greeks placeholders (None initially)
  - source=Flatfile, quality=Prelim
- Persist options_trades Parquet with correct schema and partitioning. Use the pooled writer.
- TUI shows per-file progress for options too.
- Deliverable: options trades ingested with correct schema, dedup, and large Parquet files.

Phase 2: Greeks via black_scholes (CPU bounded pool, flags)

- Add dependency on black_scholes crate and a greeks module (new crate or in classifier/greeks).
- Inputs needed per option trade:
  - S (underlying price at trade time): Pull from NBBO for underlying best mid at or before trade_ts within staleness; if unavailable, leave greeks None and set greeks_flags.
  - K (strike), T (years to expiry) = max((expiry_ts_ns - trade_ts_ns)/ns_per_year, 0), r (risk-free; config), q (dividend yield; config), sigma (IV).
- IV: initially optional. If IV not provided, either:
  - Compute using NBBO mid option price via root-finding (bisection or Brent) on Black–Scholes price; rate-limit this; or
  - Leave IV None and set greeks None; do it in T+1 finalizer once you also ingest NBBO deltas for options.
- Compute delta/gamma/vega/theta and store in OptionTrade; set greeks_flags bitfield on misses/timeouts.
- Run calculations in a bounded pool (tokio::task::spawn_blocking or rayon) with config.greeks.pool_size. Backpressure if queue grows.
- Deliverable: greeks columns populated for trades with usable inputs; flags for exceptions.

Phase 3: windowed aggregations (historical, then realtime for a single symbol)

- Add a new crate aggregations with:
  - Config: aggregations.windows = ["1m","5m","15m","30m"], aggregations.symbol = "SPY" (or any symbol), aggregations.contract_size = 100, aggregations.risk_free_rate, aggregations.dividend_yield.
  - A per-window streaming aggregator that ingests two streams:
    - Underlying trades for the target symbol (for OHLC and underlying_dollar_value_*).
    - Options trades filtered by underlying == aggregations.symbol (for dADVV and gADVV).
- Definitions
  - underlying_dollar_value = price * size
  - option notional_per_trade = price *size* contract_size
  - dADVV per option trade = abs(delta) *underlying_price* size * contract_size
  - gADVV per option trade = abs(gamma) *S^2* size * contract_size (gamma exposure dollars; standard GEX form)
    - Make these formulas configurable in config.toml so we can tweak without code changes.
- Stats per window
  - O/H/L/C: straightforward from underlying trade prices in the window.
  - total, min, max, mean, stddev, CV, IQR, MAD: implement streaming stats
    - Use Welford’s algorithm for mean/variance.
    - Use a t-digest or P² quantile estimator for median/IQR and robust stats. Crate: tdigest or quantiles.
    - MAD: maintain a small sketch or compute approximately from residuals vs running median using t-digest clones per window; or compute exact after window closes if you keep the small set (trade volume per minute can be large; start with approximate).
  - skew and kurtosis: streaming formulas exist; add with Welford-style higher moments.
  - mode: approximate with a histogram with adaptive bin width (Freedman–Diaconis rule from IQR). Keep a compact count-min sketch if memory matters.
  - bc (Box–Cox): estimate lambda via grid search per window on the underlying metric; pick maximizing log-likelihood; keep the result; this can be done once per window on close. Start with a small grid.
  - dip_pval: integrate a dip test crate if available; otherwise, fallback to computing dip statistic via isotonic regression and approximate p-values via a small bootstrap. Mark it experimental initially.
  - kde_peaks: after window close, run a small KDE (Gaussian kernel; bandwidth via Silverman) over the metric and count local maxima. For large N, sub-sample to bound cost.
- Produce one row per window with FEATURE_NAMES columns you listed. Save to Parquet as derived zone with partitioning [dt, symbol, window].
- Historical pass: consume from flatfiles; Realtime pass: attach to WS pipeline and compute only for aggregations.symbol with the same windows; emit periodic results to Parquet and optionally to a channel for the TUI.

Phase 4: aggression categorization (lowest priority)

- Finish Classifier and NbboStore to spec:
  - Maintain NBBO from quote deltas with state Normal/Locked/Crossed.
  - get_best_before(id, ts_ns, max_staleness_us) using adaptive staleness (p99 clamped within per-asset bounds).
  - Deterministic classification rules using epsilon_price; default Unknown if locked/crossed.
  - Tick-size inference flags.
- Populate aggressor fields in trades; then split dADVV stats into buyer/seller buckets to produce the requested calls_dadvv_agg_*and puts_dadvv_agg_* features per window.
- T+1 finalizer: reclassify with flatfile NBBO and rewrite aggressor/greeks columns only, upgrading to Final.

Immediate code changes to queue now

- Flatfile streaming and progress
  - Replace get_stream with get_reader returning (reader, total_len, bytes_read_counter).
  - Use csv_async::AsyncReader; parse in chunks; update metrics.
- Storage writer pooling
  - Implement WriterManager; rotate files by row_group_target and file_size_mb_target.
  - Add flush_all at shutdown.
- Fix schemas
  - Build full option_trade_schema to match OptionTrade fields (types aligned with how you serialize: conditions must be a List<Int32> not a string; enums serialized as Utf8 or preferably dictionary-encoded Utf8; optional fields nullable).
  - Add schema_version in Arrow schema metadata and test it.
- Config additions
  - Add section [aggregations] with windows = ["1m","5m","15m","30m"], symbol = "SPY", contract_size = 100, risk_free_rate, dividend_yield.
  - Add [ingest] knobs for batch_size and concurrent_days; [storage] knob for file_size_mb_target.
- TUI
  - Add a “Current file” gauge with “X MB / Y MB, Z MB/s”.
  - Show flatfile status plus current file name.

Notes on correctness and data availability for Greeks/aggregations

- You need an underlying price at the option trade’s event-time. If the underlying NBBO stream is not available, compute Greeks later in a T+1 finalizer when you ingest quotes. For realtime, consider “greeks_only_if_underlying_mid_available” to avoid fabricating.
- IV availability: if Massive flatfiles have mid option prices and you ingest option quotes as NBBO, you can derive IV. If not, you can start with a config default IV per class as a placeholder, but mark greeks_flags and avoid using such Greeks in gamma/delta features until upgraded by finalizer.
- Define contract_size in config; default 100.

Potential speed bottlenecks and fixes

- CSV parsing is CPU bound. Use spawn_blocking with a bounded semaphore to avoid starving the runtime if you keep csv (sync) parsing, or use csv_async to stay async. Either approach is fine; streaming and writer pooling are the biggest wins.
- Gzip decompression is single-threaded. Running several files concurrently helps saturate cores. Keep memory bounded with batch_size.
- Avoid per-batch file creation. The writer pool is critical.
- Use zstd in your own output (you already do); keep row groups large.

Open questions for you to confirm

- Exact Massive options flatfile path layout and column mapping.
- Whether you want dADVV/gADVV formulas exactly as:
  - dADVV = |delta| *S* contracts * contract_size
  - gADVV = |gamma| *S^2* contracts * contract_size
  If not, please specify your preferred definitions.
- For dip test and KDE peaks, OK to start with “computed on window close” and mark them experimental until we nail libs and performance?

If you want, I can draft the minimal diffs for:

- FlatfileSource: get_reader + CountingReader + streaming CSV with csv_async + metrics updates.
- Storage: WriterManager with rotation.
- Metrics/TUI: new progress fields and display.

Once those are in, we can wire options, then Greeks, then aggregations in tight iterations.

License note

- All new files and code snippets should include: “Copyright (c) James Kassemi, SC, US. All rights reserved.” and carry the license notice from SPEC.md.

--- USER ---

Thank you. Let's work on phase 0 to unblock throughput. I'm working with an LLM assistant tool called aider. I'd like to give aider a prompt to tackle phase 0. Can you please write one that includes all the necessary details (including where to find context)?

--- LLM ---
Here’s a ready-to-paste aider prompt to implement Phase 0. It includes the context, exact files to modify, concrete tasks, and acceptance criteria.

---
PROJECT CONTEXT

- Repo layout (Rust workspace): see Cargo.toml at root and these crates: core-types, flatfile-source, storage, orchestrator, metrics, tui, nbbo-cache, classifier, ws-source, rest-source, data-client, replayer.
- Spec: SPEC.md (license notice and requirements). All new/changed files must keep header:
  // Copyright (c) James Kassemi, SC, US. All rights reserved.
- Entry points:
  - orchestrator/src/main.rs runs ingestion and TUI.
  - flatfile-source/src/lib.rs handles Massive flatfiles (equities done; options not yet).
  - storage/src/lib.rs writes Parquet (currently many small files).
  - metrics/src/lib.rs exposes Prometheus and holds state.
  - tui/src/lib.rs renders dashboard from Metrics.
- Current pain points:
  - Flatfile ingestion collects entire compressed files into memory before parsing; we need true streaming to reduce latency and memory, and to show progress.
  - Parquet writer creates a new file per batch, leading to tens of thousands of tiny files. We need large row groups and rotated files by size.
  - OptionTrade Arrow schema is a stub; it should reflect OptionTrade fields even if not yet used in Phase 0.
- Build/test:
  - cargo build
  - cargo clippy -- -D warnings
  - cargo fmt --check
  - cargo test

HIGH-LEVEL GOALS (Phase 0)

1) Stream compressed CSV parsing end-to-end (no full buffering), emitting batches as we go.
2) Track per-file progress (bytes read vs total) and throughput; surface in Metrics and TUI.
3) Storage writer pooling: keep ArrowWriter open per partition and rotate files by target size; align row groups to batch size.
4) Fix option_trade_schema to match OptionTrade struct.
5) Add config knobs for ingestion batch size, concurrent day tasks, progress update cadence, and parquet target file size.

DO NOT change overall architecture or break existing tests unless updated accordingly. Keep license notice in all files.

EDITS REQUIRED

A) flatfile-source: stream parse CSV and report progress

Files:
- flatfile-source/Cargo.toml
- flatfile-source/src/lib.rs

Tasks:

1. Add dependency:
   - csv-async = "1"

2. Update SourceTrait to expose total object length (compressed) so we can display progress.
   - Add to trait:
     async fn object_len(&self, path: &str) -> Option<u64>;
   - Implement for:
     - FlatfileSource:
       - For local path branch: use tokio::fs::metadata and return len.
       - For S3 branch: call head_object().content_length.
     - LocalFileSource (in tests): return local file metadata len.

3. Replace full-file buffering in process_equity_trades_stream with true streaming:
   - Instead of try_fold into Vec<u8>, wrap the incoming Stream<Bytes> with tokio_util::io::StreamReader to get an AsyncRead.
   - If path.ends_with(".gz"), wrap with async_compression::tokio::bufread::GzipDecoder.
   - Use csv_async::AsyncReaderBuilder to parse records from the AsyncRead.
   - Emit DataBatch as you go with a configurable BATCH_SIZE (see config additions below).
   - Remove the blocking Reader::from_reader(cursor) and spawn_blocking for CSV parsing.

4. Progress tracking:
   - Create a lightweight CountingReader wrapper around the AsyncRead that increments an Arc<AtomicU64> of bytes_read per poll_read.
   - Query total_len = source.object_len(path).await at the start; if Some, call metrics.set_current_file(path.to_string(), total_len) and periodically update metrics.set_current_file_read(bytes_read).
   - Update progress every progress_update_ms (from config; see below) or every N records.
   - On completion, set current_file_read to total_len (if known).

5. Wire metrics into FlatfileSource:
   - Add an optional metrics: Option<Arc<metrics::Metrics>> field to FlatfileSource.
   - Change FlatfileSource::new(config: Arc<FlatfileConfig>, metrics: Option<Arc<Metrics>>) -> Self.
   - When parsing a file, if metrics.is_some(), call set_current_file(...) and set_current_file_read(...).
   - Keep existing tests compiling by providing None where appropriate.

6. Make batch size configurable:
   - Change process_equity_trades_stream signature to accept batch_size: usize and progress_update_ms: u64.
   - Update all call sites in this crate and orchestrator/tests to pass batch_size and progress_update_ms from config (see config changes below).
   - Remove the const BATCH_SIZE = 1000 and use the argument.

Notes:
- Maintain the current filtering by instruments and building EquityTrade exactly as before.
- Preserve DataBatchMeta building logic.

B) metrics: add current file progress state and getters

Files:
- metrics/src/lib.rs

Tasks:

1. Add fields:
   - current_file_name: Arc<Mutex<Option<String>>>
   - current_file_total: AtomicU64
   - current_file_read: AtomicU64
   - current_file_started_ts_ns: AtomicU64

2. Add methods:
   - fn set_current_file(&self, name: String, total: u64) { sets name, total, read=0, started_ts_ns=now }
   - fn set_current_file_read(&self, read: u64)
   - Getters:
     - current_file_name() -> Option<String>
     - current_file_progress() -> Option<(u64 read, u64 total, i64 started_ns)>

3. No breaking changes to existing metrics API. Keep serve() as-is.

C) TUI: display streaming progress (second gauge)

Files:
- tui/src/lib.rs

Tasks:

1. In ui(), add a second Gauge below the existing “Ingestion Progress”:
   - Label: “Current file”
   - If metrics.current_file_name() is Some and total > 0:
     - ratio = read as f64 / total as f64
     - label: “{file_name}  {read_mb:.1} / {total_mb:.1} MB  {throughput_mb_s:.1} MB/s”
       - throughput = (read bytes) / (now - started_ts_ns) in MB/s
   - If unknown total (None), display a marquee label with read so far and “Unknown total”.

2. Keep the rest of the TUI intact.

D) storage: pooled writers and rotation by size

Files:
- storage/src/lib.rs

Tasks:

1. Add a writer pool to Storage:
   - Extend Storage with: writers: std::sync::Mutex<HashMap<String, PartitionWriter>>
     - Key can be a partition path (full directory where file lives).
   - Add config knob storage.file_size_mb_target (see config changes). Default 128 MB.

2. PartitionWriter struct:
   - file_path: PathBuf
   - file: std::fs::File
   - writer: parquet::arrow::ArrowWriter<std::fs::File>
   - bytes_written: u64

3. Change write_equity_trades(&mut self, batch: &DataBatch<EquityTrade>) to:
   - Group trades by date (as today).
   - For each date group:
     - Dedup as today.
     - Convert to RecordBatch as today.
     - Compute partition dir (same as today) and prefix.
     - get_or_create_writer(partition_dir) that:
       - Creates dir if needed; opens or rotates to a new parquet file like data_<uuid>.parquet with ZSTD.
       - Stores writer in self.writers.
     - Write the RecordBatch via ArrowWriter::write(&batch).
     - Update bytes_written via file.metadata().len() after write; if >= target size, close writer and remove from pool so next call opens a new file.
   - Note: Don’t create one file per batch; instead reuse writers until rotation.

4. Keep write_option_trades and write_nbbo signatures; you can leave them as-is or add TODO to migrate similarly later.

5. Keep tests passing. The dedup test should still assert total rows == 1 even if batches end up in the same file.

E) schemas: fix option_trade_schema to match struct

Files:
- core-types/src/schema.rs
- storage/src/lib.rs (make sure arrays align with schema types)

Tasks:

1. Implement a complete Arrow schema for OptionTrade that matches core_types::types::OptionTrade fields and types:
   - contract: Utf8 (required)
   - contract_direction: Utf8 or FixedSizeBinary/Utf8 (nullable false preferred); use Utf8 for simplicity.
   - strike_price: Float64
   - underlying: Utf8
   - trade_ts_ns: Int64
   - price: Float64
   - size: UInt32
   - conditions: List<Int32>
   - exchange: Int32
   - expiry_ts_ns: Int64
   - aggressor_side: Utf8
   - class_method: Utf8
   - aggressor_offset_mid_bp: Int32 (nullable)
   - aggressor_offset_touch_ticks: Int32 (nullable)
   - nbbo_bid/ask: Float64 (nullable)
   - nbbo_bid_sz/ask_sz: UInt32 (nullable)
   - nbbo_ts_ns: Int64 (nullable)
   - nbbo_age_us: UInt32 (nullable)
   - nbbo_state: Utf8 (nullable)
   - tick_size_used: Float64 (nullable)
   - delta/gamma/vega/theta/iv: Float64 (nullable)
   - greeks_flags: UInt32 (nullable false or true; pick false if always present)
   - source: Utf8
   - quality: Utf8
   - watermark_ts_ns: Int64

2. Align storage::option_trades_to_record_batch with the schema types:
   - conditions should be a List<Int32> not stringified JSON.
   - Use UInt32Array/Option<UInt32> for sizes (nbbo_bid_sz etc), not Int64Array mapping.
   - Use StringArray for enums (aggressor_side, class_method, nbbo_state, source, quality).
   - greeks_flags as UInt32.

Note: Equity schema already uses List<Int32> for conditions; mirror that for options.

F) config: add ingestion and file size knobs, and use them

Files:
- core-types/src/config.rs
- config.toml
- orchestrator/src/main.rs

Tasks:

1. Add struct IngestConfig to AppConfig:
   - batch_size: usize (default 20000)
   - concurrent_days: usize (default 4)
   - progress_update_ms: u64 (default 250)

2. Add to StorageConfig:
   - file_size_mb_target: u64 (default 128)

3. Use them:
   - orchestrator: set semaphore to IngestConfig.concurrent_days instead of hardcoded 2.
   - When spawning per-day ingestion, pass ingest.batch_size and ingest.progress_update_ms to flatfile-source’s process_equity_trades_stream.
   - storage writer rotation uses storage.file_size_mb_target.

4. Update config.toml with example values (optional; defaults okay):
   [ingest]
   batch_size = 20000
   concurrent_days = 4
   progress_update_ms = 250

   [storage]
   file_size_mb_target = 128

ACCEPTANCE CRITERIA

- cargo build, cargo fmt --check, cargo clippy -- -D warnings, and cargo test all pass.
- For equities flatfile ingestion:
  - No full-file buffering; memory use stays bounded.
  - DataBatch is emitted repeatedly while file is being read; not only at end.
  - Metrics expose current file name, total size, bytes read; TUI shows:
    - A new “Current file” gauge with ratio and label “<file> X/Y MB Z MB/s”.
- On a run that ingests many batches for a single day, the number of Parquet files in partition dt=YYYY-MM-DD is small (rotated by target size) instead of one per batch.
- OptionTrade Arrow schema is complete and consistent with OptionTrade struct (even if not yet used in Phase 0).
- Keep existing storage tests passing; dedup still ensures only 1 row total for the duplicate batch test.

NOTES AND CONSTRAINTS

- Do not introduce blocking operations on Tokio runtime threads; use csv_async on AsyncRead, not spawn_blocking.
- Keep license header comments at the top of any new or modified source files.
- Avoid changing public APIs unless necessary; if you must, update all call sites and tests.
- Prefer minimal diffs that clearly separate:
  - streaming ingestion + metrics progress
  - storage writer pooling
  - schema fix
  - config wiring

OPTIONAL (nice-to-have if time permits)

- Add a simple unit test for CountingReader to validate byte counting.
- Add a simple metrics unit test to ensure set_current_file and set_current_file_read are exposed to TUI.

FILES LIKELY TO BE TOUCHED

- flatfile-source/Cargo.toml
- flatfile-source/src/lib.rs
- metrics/src/lib.rs
- tui/src/lib.rs
- storage/src/lib.rs
- core-types/src/schema.rs
- core-types/src/config.rs
- config.toml
- orchestrator/src/main.rs
- flatfile-source/src/lib.rs tests section (to adapt to new signatures)

---
