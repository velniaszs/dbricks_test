# BEDI Lakehouse

[![CI/CD](https://github.com/velniaszs/dbricks_test/actions/workflows/ci.yml/badge.svg)](https://github.com/velniaszs/dbricks_test/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Databricks Runtime](https://img.shields.io/badge/DBR-18.0+-red.svg)](https://docs.databricks.com/release-notes/runtime/)

Multi-source lakehouse framework on Databricks: bronze ingest, SCD2 silver, and gold serving layers. Built from the Bosch lakehouse project template. AAS Doors is the first source system onboarded.

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
├── src/bedi_lakehouse/               # Source code (V2, importable modules)
├── tests/                            # Test files
├── docs/                             # Documentation (MkDocs)
├── design/                           # Architecture and design records (not published)
├── config/                           # Source, mapping and environment declarations
├── resources/                        # Databricks Asset Bundle job definitions
├── typings/                          # Type stubs for Databricks globals
├── databricks_v1/                    # FROZEN V1 PoC — reconciliation baseline, do not edit
├── databricks_V2/                    # Notebook-era V2 design record + AUTO CDC spike
├── input/                            # Local source extracts (git-ignored, never commit)
├── .vscode/                          # VS Code settings & extensions
├── pyproject.toml                    # Project configuration
├── databricks.yml                    # Databricks Asset Bundles config
└── mkdocs.yml                        # Documentation config
```

## Databricks Setup

### 1. Workspace and Targets

The workspace URL is already set in `databricks.yml`. Two deployment targets are defined:

| Target | Default | Purpose |
|--------|---------|---------|
| `local` | yes | Personal sandbox. Used when `-t` is omitted. |
| `dev` | no | Shared development environment. Requires `-t dev`. |

Both run in `mode: development`, so deployed resources are prefixed `[dev <username>]`, schedules are paused, and each target deploys to its own path under `/Workspace/Users/<you>/.bundle/bedi-lakehouse/<target>/`.

### 2. Authentication

The Databricks CLI is pre-installed in the dev container. Outside it, see the [installation docs](https://docs.databricks.com/dev-tools/cli/install.html).

```bash
databricks auth login --host https://adb-7405617899344789.9.azuredatabricks.net
```

The CLI prompts for a profile name; this project uses `aas-doors`. It opens a browser for OAuth and writes the profile to `~/.databrickscfg`. No secrets are stored in the repo.

```bash
databricks auth profiles          # list configured profiles
databricks -p aas-doors current-user me
```

Commands pick up the profile automatically when its `host` matches the target in `databricks.yml`; otherwise pass `-p aas-doors`.

### 3. Verify Connection

```bash
databricks bundle validate     # resolve bundle config against the workspace
databricks current-user me     # confirm the profile authenticates
uv run python -c "from databricks.connect import DatabricksSession; print(DatabricksSession.builder.getOrCreate().sql('select current_user()').collect())"
```

The last command needs two things the dev container supplies: `DATABRICKS_CONFIG_PROFILE=aas-doors` in `containerEnv`, and `serverless_compute_id = auto` in the profile. Without the former the SDK falls back to an empty `[DEFAULT]` section; without the latter Databricks Connect has no compute to attach to and you must call `.serverless(True)` explicitly.

### 4. Deploy

```bash
databricks bundle deploy            # -> local target
databricks bundle deploy -t dev     # -> dev target
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

- Debian with Python 3.12
- `uv`, Databricks CLI, Azure CLI pre-installed
- All VS Code extensions configured
- Persistent authentication via mounted `~/.databrickscfg` and `~/.azure`

Those two mounts bind from your **host** home directory, so credentials survive a rebuild. Create them on the host before first launch, otherwise Docker substitutes an empty directory and every CLI call fails:

```bash
touch ~/.databrickscfg
mkdir -p ~/.azure
```

The mount uses `${localEnv:HOME}`, which is empty on Windows hosts — change it to `${localEnv:USERPROFILE}` in `.devcontainer/devcontainer.json` if you develop on Windows.

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
