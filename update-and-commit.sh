#!/usr/bin/env bash
#
# Update movie index and commit changes to git
# This script runs the Python indexer and handles version control
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/config.yaml}"

# Function to read YAML config values
get_config_value() {
    local key="$1"
    python3 -c "
import yaml, sys
try:
    with open('${CONFIG_FILE}', 'r') as f:
        config = yaml.safe_load(f)
    value = config
    for k in '${key}'.split('.'):
        value = value[k]
    print(value)
except Exception as e:
    print(f'Error reading config: {e}', file=sys.stderr)
    sys.exit(1)
"
}

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Function to handle errors
error_exit() {
    log "ERROR: $1"
    exit 1
}

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    if [[ -f "${SCRIPT_DIR}/config.example.yaml" ]]; then
        error_exit "Config file not found at $CONFIG_FILE. Please copy config.example.yaml to config.yaml and configure it."
    else
        error_exit "Config file not found at $CONFIG_FILE"
    fi
fi

# Read configuration
BACKUP_DIR=$(get_config_value "output.backup_dir")
GIT_COMMIT_MESSAGE=$(get_config_value "git.commit_message")
AUTO_PULL=$(get_config_value "git.auto_pull")
AUTO_PUSH=$(get_config_value "git.auto_push")
LOG_FILE=$(get_config_value "logging.log_file")
CSV_FILENAME=$(get_config_value "output.csv_filename")
MD_FILENAME=$(get_config_value "output.markdown_filename")

# Resolve relative paths
if [[ "$BACKUP_DIR" != /* ]]; then
    BACKUP_DIR="${SCRIPT_DIR}/${BACKUP_DIR}"
fi

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Ensure we're in the correct directory
cd "$SCRIPT_DIR"

# Setup logging
exec > >(tee -a "$LOG_FILE")
exec 2>&1

log "Starting movie index update..."
log "Using config file: $CONFIG_FILE"
log "Backup directory: $BACKUP_DIR"

# Check for UV or Python
PYTHON_CMD=""
if command -v uv &> /dev/null; then
    PYTHON_CMD="uv run python"
    log "Using UV to run Python"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
    log "Using python3 directly"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
    log "Using python directly"
else
    error_exit "Neither UV nor Python is installed or in PATH"
fi

# Check if backup directory is in a git repo - this is where we want to commit
if [[ -d "$BACKUP_DIR/.git" ]] || git -C "$BACKUP_DIR" rev-parse --git-dir > /dev/null 2>&1; then
    REPO_DIR="$BACKUP_DIR"
    log "Using git repository at: $REPO_DIR"
else
    error_exit "Backup directory $BACKUP_DIR is not in a git repository"
fi

# Pull latest changes from remote if configured
if [[ "$AUTO_PULL" == "True" ]]; then
    log "Pulling latest changes from remote..."
    if ! git -C "$REPO_DIR" pull --rebase; then
        log "WARNING: Failed to pull from remote, continuing anyway..."
    fi
fi

# Run the Python script to update indexes
log "Running Python indexer..."
export CONFIG_FILE
if $PYTHON_CMD update_movies_index.py; then
    log "Index generation completed successfully"
    
    # Check if there are changes to commit
    cd "$REPO_DIR"
    
    # Since REPO_DIR is always BACKUP_DIR now, files are relative to repo root
    MD_PATH="$MD_FILENAME"
    CSV_PATH="$CSV_FILENAME"
    
    if git diff --quiet HEAD -- "$MD_PATH" "$CSV_PATH" 2>/dev/null; then
        log "No changes detected in index files"
        exit 0
    fi
    
    # Stage the changed files
    log "Staging changes..."
    git add "$MD_PATH" "$CSV_PATH"
    
    # Commit the changes
    log "Committing changes..."
    git commit -m "$GIT_COMMIT_MESSAGE" \
        -m "Updated: $(date '+%Y-%m-%d %H:%M:%S')" \
        -m "Automated update via cron job" \
        -m "Backup directory: $BACKUP_DIR"
    
    # Push to remote if configured
    if [[ "$AUTO_PUSH" == "True" ]]; then
        log "Pushing to remote..."
        if git push; then
            log "Successfully pushed changes to remote"
        else
            error_exit "Failed to push changes to remote"
        fi
    else
        log "Auto-push disabled, skipping push to remote"
    fi
    
    log "Movie index update completed successfully"
else
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 1 ]; then
        log "No changes detected in movie library"
        exit 0
    else
        error_exit "Python script failed with exit code $EXIT_CODE"
    fi
fi