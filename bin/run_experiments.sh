#!/bin/sh
# 4モードを交互に実行し、生ログとモード別統計を1つのTXTへ保存する。
# 端末には各試行の生ログを表示せず、最後の統計結果だけを表示する。
# Usage: ./run_experiments.sh [runs] [transitionProbability] [startSeed]

set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUN_CLIENT="$SCRIPT_DIR/run_client.sh"

RUNS=${1:-10}
TRANSITION_PROBABILITY=${2:-0.2}
START_SEED=${3:-1}

case "$RUNS" in
    ''|*[!0-9]*)
        echo "[ERROR] runs must be a positive integer: $RUNS" >&2
        exit 1
        ;;
esac
if [ "$RUNS" -le 0 ]; then
    echo "[ERROR] runs must be greater than zero: $RUNS" >&2
    exit 1
fi

seed_digits=$START_SEED
case "$seed_digits" in
    -*) seed_digits=${seed_digits#-} ;;
esac
case "$seed_digits" in
    ''|*[!0-9]*)
        echo "[ERROR] startSeed must be an integer: $START_SEED" >&2
        exit 1
        ;;
esac

if [ ! -x "$RUN_CLIENT" ]; then
    echo "[ERROR] run_client.sh is not executable: $RUN_CLIENT" >&2
    exit 1
fi

RESULT_DIR="$PROJECT_ROOT/results"
mkdir -p "$RESULT_DIR"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
RESULT_FILE="$RESULT_DIR/interleaved_${TIMESTAMP}.txt"
STATS_FILE=$(mktemp "${TMPDIR:-/tmp}/lsh_oram_stats.XXXXXX") || exit 1
trap 'rm -f "$STATS_FILE"' EXIT HUP INT TERM

extract_value() {
    log_text=$1
    marker=$2
    key=$3
    printf '%s\n' "$log_text" | awk -v marker="$marker" -v key="$key" '
        $1 == marker {
            for (i = 2; i <= NF; i++) {
                split($i, pair, "=")
                if (pair[1] == key) {
                    print pair[2]
                    exit
                }
            }
        }
    '
}

write_header() {
    {
        echo "[BATCH-START] timestamp=$TIMESTAMP runs=$RUNS transitionProbability=$TRANSITION_PROBABILITY startSeed=$START_SEED"
        echo "[BATCH-ORDER] odd=lsh-cache,baseline-cache,lsh-no-cache,baseline-no-cache"
        echo "[BATCH-ORDER] even=baseline-no-cache,lsh-no-cache,baseline-cache,lsh-cache"
        echo "[BATCH-RESULT-FILE] $RESULT_FILE"
        echo
    } > "$RESULT_FILE"
}

run_one() {
    run_number=$1
    seed=$2
    mode=$3

    {
        echo "[BATCH-RUN-START] run=$run_number seed=$seed mode=$mode"
        echo "[BATCH-COMMAND] $RUN_CLIENT $mode $seed $TRANSITION_PROBABILITY"
    } >> "$RESULT_FILE"

    if output=$("$RUN_CLIENT" "$mode" "$seed" "$TRANSITION_PROBABILITY" 2>&1); then
        status=0
    else
        status=$?
    fi
    printf '%s\n' "$output" >> "$RESULT_FILE"
    echo "[BATCH-RUN-END] run=$run_number seed=$seed mode=$mode status=$status" >> "$RESULT_FILE"
    echo >> "$RESULT_FILE"

    if [ "$status" -ne 0 ]; then
        error_message="[ERROR] experiment failed: run=$run_number seed=$seed mode=$mode"
        echo "$error_message" >> "$RESULT_FILE"
        echo "$error_message" >&2
        exit "$status"
    fi

    total_ms=$(extract_value "$output" "[LATENCY]" "totalMs")
    hit_rate=$(extract_value "$output" "[SUMMARY]" "stashHitRate")

    if [ -z "$total_ms" ] || [ -z "$hit_rate" ]; then
        error_message="[ERROR] failed to parse experiment output: run=$run_number seed=$seed mode=$mode"
        echo "$error_message" >> "$RESULT_FILE"
        echo "$error_message" >&2
        exit 1
    fi

    printf '%s\t%s\t%s\t%s\n' \
        "$mode" "$seed" "$total_ms" "$hit_rate" \
        >> "$STATS_FILE"
}

append_mode_statistics() {
    target_mode=$1
    awk -F '\t' -v target="$target_mode" '
        $1 == target {
            n++
            total += $3; total_sq += $3 * $3
            hit += $4; hit_sq += $4 * $4
            if (n == 1 || $3 < total_min) total_min = $3
            if (n == 1 || $3 > total_max) total_max = $3
            if (n == 1 || $4 < hit_min) hit_min = $4
            if (n == 1 || $4 > hit_max) hit_max = $4
        }
        END {
            if (n == 0) exit
            total_avg = total / n
            hit_avg = hit / n
            total_sd = n > 1 ? sqrt((total_sq - n * total_avg * total_avg) / (n - 1)) : 0
            hit_sd = n > 1 ? sqrt((hit_sq - n * hit_avg * hit_avg) / (n - 1)) : 0
            printf "[BATCH-SUMMARY] mode=%s runs=%d\n", target, n
            printf "[BATCH-LATENCY] totalMsAverage=%.3f totalMsMin=%.3f totalMsMax=%.3f totalMsStdDev=%.3f\n", total_avg, total_min, total_max, total_sd
            printf "[BATCH-HIT] stashHitRateAverage=%.4f stashHitRateMin=%.4f stashHitRateMax=%.4f stashHitRateStdDev=%.4f stashHitPercentAverage=%.2f stashHitPercentMin=%.2f stashHitPercentMax=%.2f stashHitPercentStdDev=%.2f\n", hit_avg, hit_min, hit_max, hit_sd, hit_avg * 100, hit_min * 100, hit_max * 100, hit_sd * 100
        }
    ' "$STATS_FILE" | tee -a "$RESULT_FILE"
}

write_header

run_number=1
while [ "$run_number" -le "$RUNS" ]; do
    seed=$((START_SEED + run_number - 1))
    if [ $((run_number % 2)) -eq 1 ]; then
        modes="lsh-cache baseline-cache lsh-no-cache baseline-no-cache"
    else
        modes="baseline-no-cache lsh-no-cache baseline-cache lsh-cache"
    fi

    for mode in $modes; do
        run_one "$run_number" "$seed" "$mode"
    done
    run_number=$((run_number + 1))
done

echo "[BATCH-STATISTICS-START]" | tee -a "$RESULT_FILE"
append_mode_statistics "lsh-cache"
append_mode_statistics "baseline-cache"
append_mode_statistics "lsh-no-cache"
append_mode_statistics "baseline-no-cache"
echo "[BATCH-STATISTICS-END]" | tee -a "$RESULT_FILE"
echo "[BATCH-END] resultFile=$RESULT_FILE" | tee -a "$RESULT_FILE"
