#!/usr/bin/env bash
# Runs stress_reconnect_test.php RUNS times in a row and tallies how many
# rounds crashed the server process vs. survived clean - the same kind of
# "N clean runs is meaningful signal, not proof" evidence bankapp's
# scripts/stress_transfer.sh produced live (see notifications-service.php's
# header comment: 20/40 rounds crashed on the original code).
#
# Usage: run_stress_many.sh [RUNS] [ROUNDS_PER_RUN] [THREADS]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RUNS="${1:-10}"
ROUNDS_PER_RUN="${2:-60}"
THREADS="${3:-}"

CRASHED=0
CLEAN=0

for i in $(seq 1 "$RUNS"); do
    echo "--- run $i/$RUNS ---"
    if php "$SCRIPT_DIR/stress_reconnect_test.php" "$ROUNDS_PER_RUN" "$THREADS"; then
        CLEAN=$((CLEAN + 1))
    else
        CRASHED=$((CRASHED + 1))
        echo "*** run $i CRASHED/FAILED ***"
    fi
done

echo ""
echo "=== SUMMARY: $CLEAN/$RUNS runs clean, $CRASHED/$RUNS runs crashed or failed assertions ==="
[ "$CRASHED" -eq 0 ]
