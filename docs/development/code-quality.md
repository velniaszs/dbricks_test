# Code Quality

Maintaining high code quality standards with automated tools and best practices.

## Overview

The Python Project Template enforces code quality through:

- **Ruff**: Fast linting and formatting
- **Pre-commit hooks**: Automated quality checks
- **Type hints**: Static type checking support
- **Docstrings**: Comprehensive documentation
- **Code standards**: Consistent style and conventions

## Ruff Configuration

### Linting Rules

The template uses these ruff rule categories:

```toml title="pyproject.toml"
[tool.ruff.lint]
select = ["E", "F", "I", "N", "PLR"]
```

- **E**: pycodestyle errors
- **F**: Pyflakes errors
- **I**: isort import sorting
- **N**: pep8-naming conventions
- **PLR**: Pylint refactor suggestions

### Formatting Standards

```toml title="pyproject.toml"
[tool.ruff]
line-length = 120
indent-width = 4

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

### Per-file Ignores

```toml title="pyproject.toml"
[tool.ruff.lint.per-file-ignores]
"tests/*" = ["PLR2004"]  # Allow magic values in tests
```

## Running Code Quality Checks

### Linting

```bash
# Check for issues
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix

# Check specific files
uv run ruff check src/aas_doors_lakehouse/core.py

# Show all issues (including fixed)
uv run ruff check --show-fixes
```

### Formatting

```bash
# Format all code
uv run ruff format

# Check formatting without changes
uv run ruff format --check

# Format specific files
uv run ruff format src/aas_doors_lakehouse/core.py
```

### Combined Workflow

```bash
# Fix and format in one go
uv run ruff check --fix && uv run ruff format
```

## Pre-commit Hooks

### Configuration

```yaml title=".pre-commit-config.yaml"
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.6
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
        types_or: [python, pyi, jupyter]

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-toml
      - id: check-merge-conflict
      - id: check-added-large-files
      - id: mixed-line-ending
```

### Usage

```bash
# Install hooks (run once)
uvx pre-commit install

# Run hooks manually
uvx pre-commit run --all-files

# Update hook versions
uvx pre-commit autoupdate

# Skip hooks (not recommended)
git commit --no-verify
```

## Code Style Guidelines

### Python Style

#### Naming Conventions

```python
# Good: Follow PEP 8 naming conventions
class UserManager:
    """Manages user operations."""

    def __init__(self):
        self.active_users = []

    def get_user_by_id(self, user_id: int) -> User:
        """Get user by ID."""
        pass

# Bad: Inconsistent naming
class userManager:
    def __init__(self):
        self.ActiveUsers = []

    def getUserByID(self, userID):
        pass
```

#### Function Documentation

```python
def add_numbers(a: int, b: int) -> int:
    """
    Add two numbers together.

    Args:
        a: First number to add.
        b: Second number to add.

    Returns:
        The sum of a and b.

    Example:
        >>> add_numbers(2, 3)
        5
    """
    return a + b
```

#### Type Hints

```python
from typing import List, Optional, Dict, Any

def process_data(
    items: List[str],
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, int]:
    """Process a list of items with optional configuration."""
    if config is None:
        config = {}

    result: Dict[str, int] = {}
    for item in items:
        result[item] = len(item)

    return result
```

### Import Organization

Ruff automatically organizes imports according to PEP 8:

```python
# Standard library imports
import os
import sys
from pathlib import Path

# Third-party imports
import requests
from click import command, option

# Local imports
from aas_doors_lakehouse.core import hello_world
from aas_doors_lakehouse.utils import helper_function
```

### Line Length and Formatting

```python
# Good: Within 120 character limit
def long_function_name(
    parameter_one: str,
    parameter_two: int,
    parameter_three: Optional[bool] = None,
) -> Dict[str, Any]:
    """Function with many parameters."""
    return {
        "param_one": parameter_one,
        "param_two": parameter_two,
        "param_three": parameter_three,
    }

# Good: Long strings
message = (
    "This is a very long message that would exceed the line length limit "
    "if written on a single line, so we break it up for readability."
)
```

## Documentation Standards

### Docstring Format

Use Google-style docstrings:

```python
def calculate_area(length: float, width: float) -> float:
    """
    Calculate the area of a rectangle.

    This function multiplies length by width to calculate the area
    of a rectangle. Both parameters must be positive numbers.

    Args:
        length: The length of the rectangle in units.
        width: The width of the rectangle in units.

    Returns:
        The area of the rectangle in square units.

    Raises:
        ValueError: If length or width is negative or zero.

    Example:
        >>> calculate_area(5.0, 3.0)
        15.0

        >>> calculate_area(10, 2.5)
        25.0
    """
    if length <= 0 or width <= 0:
        raise ValueError("Length and width must be positive")

    return length * width
```

### Class Documentation

```python
class DataProcessor:
    """
    Process and analyze data from various sources.

    This class provides methods to load, clean, and analyze data
    from different file formats and sources.

    Attributes:
        data_source: The source of the data being processed.
        processed_count: Number of records processed.

    Example:
        >>> processor = DataProcessor("data.csv")
        >>> processor.load_data()
        >>> results = processor.analyze()
    """

    def __init__(self, data_source: str):
        """
        Initialize the data processor.

        Args:
            data_source: Path to the data source file.
        """
        self.data_source = data_source
        self.processed_count = 0
```

## Error Handling

### Exception Handling

```python
def safe_divide(a: float, b: float) -> float:
    """
    Safely divide two numbers.

    Args:
        a: Numerator.
        b: Denominator.

    Returns:
        Result of a divided by b.

    Raises:
        ValueError: If b is zero.
    """
    if b == 0:
        raise ValueError("Cannot divide by zero")

    return a / b
```

### Logging

```python
import logging

logger = logging.getLogger(__name__)

def process_file(filename: str) -> bool:
    """Process a file and return success status."""
    try:
        logger.info(f"Processing file: {filename}")
        # File processing logic here
        logger.info(f"Successfully processed: {filename}")
        return True
    except FileNotFoundError:
        logger.error(f"File not found: {filename}")
        return False
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")
        return False
```

## Testing Code Quality

### Test Code Standards

```python
class TestDataProcessor:
    """Test class for DataProcessor."""

    def test_initialization(self):
        """Test DataProcessor initialization."""
        processor = DataProcessor("test.csv")
        assert processor.data_source == "test.csv"
        assert processor.processed_count == 0

    @pytest.mark.parametrize(
        "a,b,expected",
        [
            (10, 2, 5.0),
            (15, 3, 5.0),
            (7, 2, 3.5),
        ],
    )
    def test_safe_divide_success(self, a, b, expected):
        """Test successful division operations."""
        result = safe_divide(a, b)
        assert result == expected

    def test_safe_divide_zero_division(self):
        """Test division by zero raises ValueError."""
        with pytest.raises(ValueError, match="Cannot divide by zero"):
            safe_divide(10, 0)
```

## IDE Integration

The project includes pre-configured `.vscode/settings.json` with Ruff format-on-save and organize-imports enabled. See [Configuration](../getting-started/configuration.md#vs-code-settings) for details.

## Quality Metrics

### Code Quality Indicators

- **Linting**: Zero ruff violations
- **Formatting**: Consistent code style
- **Coverage**: 100% test coverage
- **Documentation**: All public APIs documented
- **Type hints**: All function signatures typed

### Monitoring Quality

```bash
# Generate quality report
uv run ruff check --output-format=json > quality-report.json

# Check coverage
uvx pytest --cov=src/ --cov-report=term-missing

# Validate documentation
uvx mkdocs build --strict
```

## Best Practices

### Code Review Checklist

- [ ] Code follows PEP 8 style guidelines
- [ ] All functions have type hints
- [ ] All public functions have docstrings
- [ ] Tests cover new functionality
- [ ] No linting violations
- [ ] Code is properly formatted
- [ ] Error handling is appropriate
- [ ] Performance considerations addressed

### Refactoring Guidelines

1. **Make small, incremental changes**
2. **Run tests after each change**
3. **Update documentation as needed**
4. **Maintain backward compatibility**
5. **Use descriptive commit messages**

---

**Next**: Learn about [Documentation](documentation.md) to create comprehensive project documentation.
