# Runbook: Forcing a Treasury-Lagged Rebuild

When Massive’s `/fed/v1/treasury-yields` endpoint lags the trade date, the orchestrator reuses the newest curve on disk so ingestion can keep moving. Those runs are now flagged in the TUI under **“Outdated data”** with the dataset/day and the curve date that was substituted. Follow this runbook whenever an entry appears.

## 1. Capture the context

1. Note the dataset (`options_trades`) and trade date reported in the TUI block.
2. Confirm the dependency date/lag so you can verify the fix later.

## 2. Quiesce the orchestrator

1. Stop the running binary (Ctrl+C in the orchestrator terminal or `systemctl stop orchestrator` if managed as a service).
2. Wait for the TUI to exit to ensure parquet writers flushed.

## 3. Clean the stale artifacts

From the repo root:

```bash
rm -rf data/options_trades/dt=<DATE>/   # raw parquet for the flagged day
rm -rf data/nbbo/dt=<DATE>/            # optional if you want a clean overlay
rm -f checkpoints/options_trades/<DATE>.parquet
```

Replace `<DATE>` with the ISO date (e.g., `2025-02-14`).

## 4. Scope the replay window

Edit `config.toml` and temporarily set `flatfile.date_ranges` to a single window that covers only the flagged day, for example:

```toml
[[flatfile.date_ranges]]
start_ts = "2025-02-14T00:00:00Z"
end_ts   = "2025-02-14T23:59:59Z"
```

Leave other ranges commented out to avoid rehydrating unrelated days.

## 5. Rehydrate the day

1. Restart the orchestrator (`cargo run -p orchestrator --release` or your usual launcher).
2. Watch the TUI: once options replay finishes with a fresh treasury curve, the “Outdated data” entry disappears automatically.

## 6. Restore the normal schedule

1. Revert `flatfile.date_ranges` to the long-running configuration.
2. Start the orchestrator again if you stopped it in the previous step.

## 7. Verification

1. TUI shows no outdated entries.
2. `orchestrator.log` contains `treasury curve for <DATE>` lines without errors.
3. Optional: spot-check the rebuilt parquet (e.g., `parquet-tools meta`) to ensure `treasury_curve_ts` columns reflect the target date.

If the entry persists, confirm the Massive API has published the missing curve and repeat steps 3–5. If treasury data is still unavailable, leave the flagged entry in place so operators know the day is pending.
