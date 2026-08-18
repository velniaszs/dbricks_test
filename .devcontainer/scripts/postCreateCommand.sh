#!/bin/bash
# Post-create command for devcontainer setup
# This script runs after the container is created

set -e

echo "🚀 Setting up Lakehouse development environment..."

# Ensure uv is in PATH
export PATH="$HOME/.local/bin:$PATH"

# Mark workspace as safe for git
git config --global --add safe.directory /workspace

# --- Mount mode: fix .venv volume ownership (disabled by default) -----------
# Uncomment this block when the "mounts" option in devcontainer.json is active.
# Docker volumes are initialised as root:root; this corrects ownership so the
# non-root 'vscode' user can write into the mount before uv sync runs.
# When the mount is NOT used this block is harmless but unnecessary.
# if [ -d .venv ] && [ "$(stat -c '%U' .venv)" != "$(id -un)" ]; then
#     echo "🔑 Fixing .venv ownership (currently owned by $(stat -c '%U' .venv))..."
#     sudo chown -R "$(id -u):$(id -g)" .venv
# fi
# ---------------------------------------------------------------------------

# Check if .venv is healthy: directory exists, bin/ present, python runs
venv_is_healthy() {
    [ -d .venv ] && \
    [ -d .venv/bin ] && \
    [ -x .venv/bin/python ] && \
    .venv/bin/python --version &>/dev/null
}

# Remove .venv and recreate it via uv sync
rebuild_venv() {
    echo "🗑️  Removing stale .venv..."
    # Use find rather than rm -rf .venv so we only clear the contents and leave
    # the directory itself intact — required when .venv is a Docker volume mount point
    # (rm -rf on a mount point returns EBUSY and causes set -e to abort the script).
    find .venv -mindepth 1 -delete 2>/dev/null || true
    echo "📦 Installing project dependencies..."
    uv sync --dev -v
}

# Pre-sync: remove any broken .venv so uv starts fresh
if [ -d .venv ] && ! venv_is_healthy; then
    echo "⚠️  Detected broken .venv (missing bin/ or non-functional Python)..."
    rebuild_venv
else
    # Normal first-run or up-to-date venv
    echo "📦 Installing project dependencies..."
    uv sync --dev -v
fi

# Post-sync: verify the venv is actually healthy; retry once with a full clean if not
if ! venv_is_healthy; then
    echo "⚠️  Virtual environment still unhealthy after sync — attempting full rebuild..."
    rebuild_venv
fi

# Final guard: abort with a clear message if the venv is still broken
if ! venv_is_healthy; then
    echo "❌ Failed to create a working virtual environment. Check uv output above."
    exit 1
fi

echo "✅ Virtual environment is healthy: $(.venv/bin/python --version)"

# Install pre-commit hooks
echo "🔧 Installing pre-commit hooks..."
uvx pre-commit install

# Verify installation
echo ""
echo "🔍 Verifying installation..."
echo "Python: $(uv run python --version)"
echo "uv: $(uv --version)"
echo "Databricks CLI: $(databricks --version 2>/dev/null || echo 'not found in PATH — available after shell reload')"

# Check Databricks authentication
echo ""
if databricks auth describe 2>/dev/null; then
    echo "✅ Databricks authentication configured"
else
    echo "⚠️  Databricks authentication not configured"
    echo "   Run: databricks configure --profile DEFAULT"
fi

echo ""
echo "✨ Development environment ready!"
echo ""
echo "Quick commands:"
echo "  uv run pytest          - Run tests"
echo "  uv run ruff check      - Lint code"
echo "  uv run mkdocs serve    - Preview documentation"
echo "  databricks bundle deploy -t dev  - Deploy to Databricks"
