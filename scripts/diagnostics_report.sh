#!/usr/bin/env bash
# Collects two snapshots of app-focused diagnostics five minutes apart

set -euo pipefail

METRICS_URL="${METRICS_URL:-http://localhost:9095/metrics}"
SLEEP_DURATION="${SLEEP_DURATION:-300}"
M17_ENV="${M17_ENV:-dev}"
WINDOWSPACE_SYMBOL="${WINDOWSPACE_SYMBOL:-}"
REPORT_PATH="$(realpath "${1:-diagnostics_report_$(date +%Y%m%dT%H%M%S).log}")"

collect_snapshot() {
    local label="$1"

    {
        echo "===== Snapshot: ${label} ====="
        echo "Timestamp: $(date -Iseconds)"
        echo "Environment: ${M17_ENV}"
        if [ -n "${WINDOWSPACE_SYMBOL}" ]; then
            echo "Symbol filter: ${WINDOWSPACE_SYMBOL}"
        fi
        echo

        echo "--- Ledger state timing ---"
        python3 <<'PY'
from pathlib import Path
from datetime import datetime, timezone
base = Path("ledger.state")
if not base.exists():
    print("ledger.state directory missing")
else:
    entries = sorted(base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    for entry in entries[:8]:
        ts = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
        kind = "dir " if entry.is_dir() else "file"
        print(f"{kind} {entry}  {ts.isoformat()}")
PY
        echo

        echo "--- Metrics (${METRICS_URL}) ---"
        if curl -fsS "$METRICS_URL"; then
            :
        else
            echo "ERROR: Unable to fetch metrics from ${METRICS_URL}"
        fi
        echo

        echo "--- Windowspace slot summary ---"
        if run_windowspace_diag; then
            :
        else
            echo "ERROR: windowspace_diag failed (see stderr above)"
        fi
        echo

        echo "--- RF slot status summary ---"
        if run_rf_diag; then
            :
        else
            echo "ERROR: debug_rf_slots failed (see stderr above)"
        fi
        echo
    } >>"$REPORT_PATH"
}

run_windowspace_diag() {
    local args=("$M17_ENV")
    if [ -n "${WINDOWSPACE_SYMBOL}" ]; then
        args+=("--symbol=${WINDOWSPACE_SYMBOL}")
    fi
    cargo run -q -p m17 --bin windowspace_diag -- "${args[@]}"
}

run_rf_diag() {
    cargo run -q -p m17 --bin debug_rf_slots -- "$M17_ENV"
}

main() {
    mkdir -p "$(dirname "$REPORT_PATH")"
    echo "Writing diagnostics to ${REPORT_PATH}"

    collect_snapshot "initial"
    echo "Sleeping for ${SLEEP_DURATION} seconds..."
    sleep "$SLEEP_DURATION"
    collect_snapshot "follow-up"

    echo "Diagnostics complete. Report saved to ${REPORT_PATH}"
    echo
    echo "==== Report Preview ===="
    tail -n 100 "$REPORT_PATH"
}

main "$@"
