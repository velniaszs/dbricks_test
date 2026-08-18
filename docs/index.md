# AAS Doors Lakehouse

Lakehouse pipelines for **AAS Doors** requirement extracts on Databricks.

## Features

- **⚡ Lightning Fast** - `uv` package manager for instant dependency resolution
- **🔷 Databricks Ready** - Pre-configured `databricks-connect` and `databricks-sdk`
- **🏭 Production Quality** - Ruff linting, pytest coverage, pre-commit hooks
- **📚 Auto Documentation** - MkDocs Material with API reference generation
- **🚀 CI/CD Pipeline** - GitHub Actions for testing and deployment

## Quick Start

1. **Clone the repository**
2. **Open in Dev Container** — Reopen in Container via VS Code Command Palette
3. **Authenticate** — Sign in to Databricks via the VS Code extension (OAuth)
4. **Start developing** — `uv run pytest` to verify everything works

See [Installation](getting-started/installation.md) and [Quick Start](getting-started/quick-start.md) for details.

## Repository Conventions

- New code is **plain Python modules** under `src/aas_doors_lakehouse/`, not Databricks notebooks.
- `databricks_v1/` is a **frozen** reference implementation and reconciliation baseline; it is excluded from linting.
- `input/` holds local source extracts and is git-ignored. Never commit it.

## Requirements

| Component | Version |
|-----------|---------|
| Python | 3.12 |
| Databricks Runtime | 18.0+ |
| uv | Latest |

## Documentation

### Getting Started

- [Installation](getting-started/installation.md) - Prerequisites and setup
- [Quick Start](getting-started/quick-start.md) - Your first steps
- [Configuration](getting-started/configuration.md) - Configuration files reference
- [Repository Setup](getting-started/repository-setup.md) - GitHub configuration

### Development

- [Testing](development/testing.md) - Testing with pytest
- [Code Quality](development/code-quality.md) - Ruff linting and formatting
- [CI/CD](development/ci-cd.md) - GitHub Actions pipeline
- [Documentation](development/documentation.md) - MkDocs setup

### Reference

- [API Reference](reference/index.md) - Auto-generated API documentation
- [Coverage Report](coverage.md) - Test coverage analysis
