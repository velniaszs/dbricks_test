# AAS Doors Lakehouse

[![CI/CD](https://github.com/velniaszs/dbricks_test/actions/workflows/ci.yml/badge.svg)](https://github.com/velniaszs/dbricks_test/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Databricks Runtime](https://img.shields.io/badge/DBR-18.0+-red.svg)](https://docs.databricks.com/release-notes/runtime/)

Lakehouse pipelines for AAS Doors requirement extracts on Databricks: bronze ingest, SCD2 silver, and gold serving layers. Built from the Bosch lakehouse project template.

## Features

- **⚡ Lightning Fast**: Powered by `uv` - the fastest Python package manager
- **🔷 Databricks Ready**: Pre-configured `databricks-connect` and `databricks-sdk`
- **🏭 Production Quality**: Ruff linting, pytest coverage, pre-commit hooks
- **📚 Auto Documentation**: MkDocs Material with API reference generation
- **🚀 CI/CD Pipeline**: GitHub Actions for testing and deployment
- **🔧 Zero Config**: Works immediately after cloning

## Requirements

| Component | Version |
|-----------|---------|
| Python | 3.12 |
| Databricks Runtime | 18.0+ |
| uv | Latest |

## Quick Start

```bash
# Clone and install
git clone https://github.com/velniaszs/dbricks_test.git
cd dbricks_test
uv sync --dev

# Run tests
uv run pytest

# Start docs server
uv run mkdocs serve
```

## Project Structure

```
├── src/aas_doors_lakehouse/          # Source code (V2, importable modules)
├── tests/                            # Test files
├── docs/                             # Documentation (MkDocs)
├── resources/                        # Databricks Asset Bundle job definitions
├── typings/                          # Type stubs for Databricks globals
├── databricks_v1/                    # FROZEN V1 PoC — reconciliation baseline, do not edit
├── databricks_V2/                    # V2 architecture docs + AUTO CDC spike
├── input/                            # Local source extracts (git-ignored, never commit)
├── .vscode/                          # VS Code settings & extensions
├── pyproject.toml                    # Project configuration
├── databricks.yml                    # Databricks Asset Bundles config
└── mkdocs.yml                        # Documentation config
```

## Databricks Setup

### 1. Configure Workspace

Edit `databricks.yml` with your workspace URL:

```yaml
targets:
  dev:
    workspace:
      host: https://your-workspace.azuredatabricks.net
```

### 2. Authentication

```bash
# Install Databricks CLI
brew install databricks/tap/databricks

# Configure authentication
databricks auth login --host https://your-workspace.azuredatabricks.net
```

Or use `.env` file:

```bash
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your-personal-access-token
```

### 3. Verify Connection

```bash
uv run python -c "from databricks.connect import DatabricksSession; print(DatabricksSession.builder.getOrCreate())"
```

## Development Commands

```bash
uv sync --dev          # Install dependencies
uv run pytest          # Run tests
uv run ruff check      # Lint code
uv run ruff format     # Format code
uv run mkdocs serve    # Serve docs locally
uv run mkdocs build    # Build documentation
```

## Dev Container

For a consistent, pre-configured development environment:

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Install VS Code [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
3. Open folder → Click "Reopen in Container" when prompted

The dev container includes:

- Ubuntu 22.04 with Python 3.12
- `uv`, Databricks CLI, Azure CLI pre-installed
- All VS Code extensions configured
- Persistent authentication via mounted `~/.databrickscfg` and `~/.azure`

## VS Code Integration

Recommended extensions are auto-suggested when opening this project:

- **Databricks** - Workspace browser and notebook sync
- **Ruff** - Fast Python linting and formatting
- **Python/Pylance** - IntelliSense and debugging
- **Jupyter** - Interactive notebooks

The `.vscode/settings.json` is pre-configured for:

- Databricks notebook cell markers (`# COMMAND ----------`)
- Auto-format on save with Ruff
- Environment file support (`.env`)

## Documentation

Full documentation: https://velniaszs.github.io/dbricks_test

Architecture and design notes live outside the published docs site:

| Document | Scope |
|----------|-------|
| `databricks_v1/docs/architecture.md` | V1 PoC design (frozen) |
| `databricks_V2/docs/architecture.md` | V2 framework design |
| `databricks_V2/docs/scaling-to-a-framework.md` | Rationale, open questions, vendor verification |

## Repository Conventions

- **New code is plain Python modules** under `src/aas_doors_lakehouse/`, not Databricks notebooks. Jobs invoke them via Asset Bundle wheel tasks.
- **`databricks_v1/` is frozen.** It is the reconciliation baseline (289 versions / 165 current after a clean rebuild); changing it invalidates the comparison. It is excluded from Ruff.
- **Never commit source extracts.** `input/` is git-ignored and contains customer data.
