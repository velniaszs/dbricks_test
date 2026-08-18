# Configuration

Configuration files reference for the AAS Doors Lakehouse project.

## Overview

| File | Purpose |
|------|---------|
| `pyproject.toml` | Project metadata, dependencies, tool configuration |
| `databricks.yml` | Databricks Asset Bundles configuration |
| `mkdocs.yml` | Documentation site configuration |
| `.pre-commit-config.yaml` | Pre-commit hooks |
| `.env` | Environment variables (not committed) |

## pyproject.toml

Main project configuration following Python packaging standards.

### Project Metadata

```toml
[project]
name = "aas-doors-lakehouse"
version = "1.0.0"
description = "Lakehouse pipelines for AAS Doors requirement extracts on Databricks."
requires-python = ">=3.12,<3.13"
```

### Dependencies

```toml
dependencies = [
    "databricks-connect>=18.0",
    "databricks-sdk[notebook]>=0.43",
    "pyyaml>=6.0",
    "python-dotenv>=1.0.1",
]

[dependency-groups]
dev = [
    "ruff>=0.1.0",
    "pre-commit>=3.0.0",
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "mkdocs>=1.6.1",
    "mkdocs-material>=9.6.15",
    # ... more dev dependencies
]
```

### Ruff Configuration

```toml
[tool.ruff]
line-length = 120
indent-width = 4
builtins = ["display", "displayHTML", "dbutils", "table", "sql", "udf", "getArgument", "sc", "sqlContext", "spark"]

[tool.ruff.lint]
select = ["E", "F", "I", "N", "PLR"]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

The `builtins` setting prevents Ruff from flagging Databricks globals as undefined.

### Pytest Configuration

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = "src"
python_files = ["test_*.py", "*_test.py"]
addopts = ["--strict-markers", "--strict-config", "--verbose"]
```

### Coverage Configuration

```toml
[tool.coverage.run]
source = ["src"]
branch = true

[tool.coverage.report]
fail_under = 80

[tool.coverage.html]
directory = "build/coverage"
```

## databricks.yml

Databricks Asset Bundles configuration for deploying workflows.

```yaml
bundle:
  name: aas-doors-lakehouse

include:
  - resources/*.yml

targets:
  local:
    mode: development
    default: true
    workspace:
      host: https://your-workspace.azuredatabricks.net
    presets:
      tags:
        team: "your-team"
        instance: "local"

  dev:
    mode: development
    workspace:
      host: https://your-workspace.azuredatabricks.net
```

### Key Settings

| Setting | Description |
|---------|-------------|
| `bundle.name` | Bundle identifier |
| `include` | Additional YAML files for resources |
| `targets` | Deployment environments |
| `workspace.host` | Databricks workspace URL |
| `presets.tags` | Resource tags |

## Environment Variables (.env)

Create a `.env` file for local development:

```bash
# Databricks workspace
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net

# Optional: Databricks cluster for connect
DATABRICKS_CLUSTER_ID=your-cluster-id
```

!!! tip
    Authentication is handled via the **Databricks VS Code extension** using OAuth. You do not need to store tokens in `.env`. See [Installation](installation.md#3-authenticate) for details.

!!! warning
    Never commit `.env` to version control. It's already in `.gitignore`.

## VS Code Settings

Pre-configured in `.vscode/settings.json`:

```json
{
  "python.envFile": "${workspaceRoot}/.env",
  "databricks.python.envFile": "${workspaceFolder}/.env",
  "jupyter.interactiveWindow.cellMarker.default": "# COMMAND ----------"
}
```

## Type Stubs (typings/)

The `typings/__builtins__.pyi` file provides IDE support for Databricks globals:

```python
spark: SparkSession
dbutils: ...
display: ...
```

This enables autocomplete and type checking for Databricks-specific functions.
