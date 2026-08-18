# Testing Guide

Testing with pytest in the AAS Doors Lakehouse project.

## Overview

The template includes:

- **pytest** - Modern Python testing framework
- **pytest-cov** - Coverage reporting
- **80% coverage target** - Configured in pyproject.toml

## Running Tests

### Basic Commands

```bash
# Run all tests
uv run pytest

# Verbose output
uv run pytest -v

# Stop on first failure
uv run pytest -x
```

### With Coverage

```bash
# Terminal coverage report
uv run pytest --cov=src/

# HTML coverage report
uv run pytest --cov=src/ --cov-report=html:build/coverage

# View HTML report
open build/coverage/index.html
```

### Specific Tests

```bash
# Run specific file
uv run pytest tests/test_import.py

# Run specific test
uv run pytest tests/test_import.py::test_package_import

# Run tests matching pattern
uv run pytest -k "import"
```

## Test Structure

```
tests/
├── __init__.py           # Package initialization
└── test_import.py        # Import and metadata tests
```

## Writing Tests

### Basic Test

```python
def test_example():
    """Test description."""
    result = my_function()
    assert result == expected
```

### Test Class

```python
class TestMyFeature:
    """Tests for MyFeature."""

    def test_basic(self):
        assert True

    def test_edge_case(self):
        assert True
```

### Fixtures

```python
import pytest

@pytest.fixture
def sample_data():
    """Provide sample data for tests."""
    return {"key": "value"}

def test_with_fixture(sample_data):
    assert sample_data["key"] == "value"
```

## Testing with Databricks

### Mock Spark Session

```python
from unittest.mock import MagicMock, patch

def test_spark_query():
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = [{"col": "value"}]

    with patch("my_module.spark", mock_spark):
        result = my_function()
        assert result == expected
```

### Integration Tests

For tests that require a real Databricks connection:

```python
import pytest
from databricks.connect import DatabricksSession

@pytest.fixture(scope="session")
def spark():
    """Create Databricks session for integration tests."""
    return DatabricksSession.builder.getOrCreate()

@pytest.mark.integration
def test_databricks_query(spark):
    df = spark.sql("SELECT 1 as value")
    assert df.collect()[0]["value"] == 1
```

Run integration tests separately:

```bash
uv run pytest -m integration
```

## Coverage Configuration

From `pyproject.toml`:

```toml
[tool.coverage.run]
source = ["src"]
branch = true

[tool.coverage.report]
fail_under = 80

[tool.coverage.html]
directory = "build/coverage"
```

## CI Integration

Tests run automatically in GitHub Actions on push/PR. See [CI/CD](ci-cd.md) for details.
