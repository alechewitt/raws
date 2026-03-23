#!/bin/bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROMPT_DIR="$SCRIPT_DIR"
LOG_DIR="$PROJECT_DIR/agent_logs"

mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"

# Ctrl+C kills immediately — recover_if_dirty will clean up next run
trap 'echo ""; echo "Interrupted."; exit 1' INT TERM

# Determine which prompt to use:
# - If progress/summary.json doesn't exist, this is the first run (initializer)
# - Otherwise, use the coding prompt
get_prompt_file() {
    if [ ! -f "$PROJECT_DIR/progress/summary.json" ]; then
        echo "$PROMPT_DIR/initializer_prompt.md"
    else
        echo "$PROMPT_DIR/coding_prompt.md"
    fi
}

# Reset repo to last clean commit if there are uncommitted changes
recover_if_dirty() {
    if ! git diff --quiet HEAD 2>/dev/null || ! git diff --cached --quiet HEAD 2>/dev/null; then
        echo "WARNING: Repo has uncommitted changes from interrupted session."
        echo "Stashing changes to ensure clean state..."
        git stash push -m "auto-stash: interrupted agent session $(date +%Y%m%d_%H%M%S)"
        echo "Changes stashed. Continuing with clean repo."
    fi
}

SESSION=1

while true; do
    # Clean up any mess from a previous interrupted session
    recover_if_dirty

    PROMPT_FILE="$(get_prompt_file)"
    PROMPT_NAME="$(basename "$PROMPT_FILE" .md)"
    TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
    COMMIT="$(git rev-parse --short=6 HEAD 2>/dev/null || echo 'no-commit')"
    LOGFILE="$LOG_DIR/${TIMESTAMP}_s${SESSION}_${PROMPT_NAME}_${COMMIT}.log"

    echo "=== Session $SESSION ==="
    echo "Prompt: $PROMPT_FILE"
    echo "Log:    $LOGFILE"
    echo "Started: $(date)"
    echo ""

    claude --dangerously-skip-permissions \
           --print "$(cat "$PROMPT_FILE")" &> "$LOGFILE" || true

    echo "Finished: $(date)"
    echo "---"

    SESSION=$((SESSION + 1))
done
