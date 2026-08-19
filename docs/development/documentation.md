# Documentation

Complete guide to creating and maintaining documentation with MkDocs and Material theme.

## Overview

The Python Project Template includes a comprehensive documentation system:

- **MkDocs**: Static site generator for project documentation
- **Material Theme**: Modern, responsive design
- **API Reference**: Auto-generated from docstrings
- **Coverage Integration**: Test coverage reports
- **Search**: Full-text search functionality
- **GitHub Pages**: Automated deployment

## Documentation Structure

```
docs/
├── index.md                    # Homepage
├── getting-started/           # Getting started guides
│   ├── installation.md       # Installation instructions
│   ├── quick-start.md        # Quick start guide
│   └── configuration.md      # Configuration details
├── development/              # Development guides
│   ├── setup.md             # Development setup
│   ├── testing.md           # Testing guide
│   ├── code-quality.md      # Code quality standards
│   └── documentation.md     # This file
├── reference/               # API reference
│   └── index.md            # Auto-generated API docs
├── coverage/               # Coverage reports (symlink)
└── includes/               # Reusable content
```

## MkDocs Configuration

### Basic Settings

```yaml title="mkdocs.yml"
site_name: Python Project Template
site_description: A reusable Python project template with best practices for development
site_url: https://velniaszs.github.io/dbricks_test
site_author: Developer

# Repository
repo_name: velniaszs/dbricks_test
repo_url: https://github.com/velniaszs/dbricks_test
edit_uri: blob/main/docs/
```

### Theme Configuration

```yaml title="mkdocs.yml"
theme:
  name: material
  language: en
  features:
    - navigation.instant          # Fast page loading
    - navigation.instant.prefetch # Prefetch on hover
    - navigation.tracking         # Update URL with active anchor
    - navigation.tabs            # Top-level tabs
    - navigation.sections        # Collapsible sections
    - navigation.expand          # Expand all sections
    - navigation.indexes         # Section index pages
    - navigation.top             # Back to top button
    - search.highlight           # Highlight search terms
    - search.share              # Share search results
    - search.suggest            # Search suggestions
    - content.code.copy         # Copy code button
    - content.code.select       # Select code button
    - content.tabs.link         # Link content tabs
```

### Navigation Structure

```yaml title="mkdocs.yml"
nav:
  - Home: index.md
  - Getting Started:
    - Installation: getting-started/installation.md
    - Quick Start: getting-started/quick-start.md
    - Configuration: getting-started/configuration.md
  - Development:
    - Setup: development/setup.md
    - Testing: development/testing.md
    - Code Quality: development/code-quality.md
    - Documentation: development/documentation.md
  - Reference:
    - API Reference: reference/
  - Coverage Report: coverage/
```

## Writing Documentation

Use [MkDocs Material reference](https://squidfunnel.github.io/mkdocs-material-reference/) for Markdown syntax, admonitions, code blocks, content tabs, and other formatting features.

## API Documentation

### Docstring Integration

The template uses mkdocstrings to generate API documentation from docstrings:

```python title="src/bedi_lakehouse/core.py"
def add_numbers(a: int, b: int) -> int:
    """
    Add two numbers together.

    This function takes two integers and returns their sum.
    It's a simple example of a documented function.

    Args:
        a: The first number to add.
        b: The second number to add.

    Returns:
        The sum of a and b.

    Example:
        >>> add_numbers(2, 3)
        5

        >>> add_numbers(-1, 1)
        0
    """
    return a + b
```

### API Reference Pages

```markdown title="docs/reference/index.md"
# API Reference

Auto-generated API documentation for the Python Project Template.

## Core Module

::: bedi_lakehouse.core
    options:
      show_source: true
      show_root_heading: true
      show_root_toc_entry: true
      show_object_full_path: false
      show_category_heading: true
```

### Mkdocstrings Configuration

```yaml title="mkdocs.yml"
plugins:
  - mkdocstrings:
      handlers:
        python:
          options:
            docstring_style: google
            show_source: true
            show_root_heading: true
            show_root_toc_entry: true
            show_object_full_path: false
            show_category_heading: true
```

## Building Documentation

### Local Development

```bash
# Serve documentation with live reload
uv run mkdocs serve

# Build static documentation
uv run mkdocs build

# Build with strict mode (fail on warnings)
uv run mkdocs build --strict
```

### Accessing Documentation

When serving locally:
- **URL**: http://127.0.0.1:8000
- **Live reload**: Automatically refreshes on changes
- **Search**: Full-text search available
- **Navigation**: Use sidebar or top navigation

### Build Output

```bash
# View built site structure
ls -la site/
# Output:
# index.html
# getting-started/
# development/
# reference/
# coverage/
# assets/
# search/
```

## Coverage Integration

### Linking Coverage Reports

The template integrates test coverage reports into documentation:

```bash
# Generate coverage report
uvx pytest --cov=src/ --cov-report=html:build/coverage tests/

# Create symbolic link in docs
ln -sf ../build/coverage docs/coverage
```

### Coverage in Navigation

```yaml title="mkdocs.yml"
nav:
  - Coverage Report: coverage/
```

This creates a direct link to the HTML coverage report in the documentation navigation.

## Deployment

### GitHub Pages

#### Automatic Deployment

```bash
# Deploy to GitHub Pages
uv run mkdocs gh-deploy --force
```

### Custom Domain

Add `CNAME` file to `docs/` directory:

```text title="docs/CNAME"
docs.yourproject.com
```

## Advanced Features

### Custom CSS

```css title="docs/stylesheets/extra.css"
/* Custom styles for your documentation */
.md-header {
    background-color: #2196f3;
}

.md-nav__item--active > .md-nav__link {
    color: #2196f3;
}
```

Add to mkdocs.yml:

```yaml title="mkdocs.yml"
extra_css:
  - stylesheets/extra.css
```

### Custom JavaScript

```javascript title="docs/javascripts/extra.js"
// Custom JavaScript for your documentation
document.addEventListener('DOMContentLoaded', function() {
    console.log('Documentation loaded');
});
```

Add to mkdocs.yml:

```yaml title="mkdocs.yml"
extra_javascript:
  - javascripts/extra.js
```

### Social Cards

```yaml title="mkdocs.yml"
plugins:
  - social:
      cards_layout_options:
        background_color: "#2196f3"
        color: "#ffffff"
```

### Version Management

```yaml title="mkdocs.yml"
extra:
  version:
    provider: mike
    default: latest
```

## Best Practices

### Writing Guidelines

1. **Clear Structure**: Use consistent heading hierarchy
2. **Concise Content**: Keep explanations focused and actionable
3. **Code Examples**: Include working code examples
4. **Cross-references**: Link related sections
5. **Regular Updates**: Keep documentation current with code changes

### Organization Tips

1. **Logical Flow**: Organize content from basic to advanced
2. **Consistent Naming**: Use consistent file and section names
3. **Reusable Content**: Use includes for repeated content
4. **Search Optimization**: Use descriptive headings and keywords

### Maintenance

1. **Regular Reviews**: Review documentation during code reviews
2. **Link Checking**: Verify internal and external links
3. **Accuracy**: Ensure examples work with current code
4. **Feedback**: Collect and act on user feedback

## Troubleshooting

### Common Issues

#### Build Failures

```bash
# Check for syntax errors
uv run mkdocs build --strict

# Validate YAML configuration
python -c "import yaml; yaml.safe_load(open('mkdocs.yml'))"
```

#### Missing Dependencies

```bash
# Install missing plugins
uv add mkdocs-material mkdocstrings[python] pymdown-extensions

# Sync dependencies
uv sync --dev
```

#### Broken Links

```bash
# Check for broken internal links
uv run mkdocs build --strict

# Use link checker plugin
uv add mkdocs-linkcheck
```

### Performance Optimization

#### Fast Builds

```bash
# Build only changed files
uv run mkdocs build --dirty

# Serve with dirty reload
uv run mkdocs serve --dirtyreload
```

#### Large Sites

```yaml title="mkdocs.yml"
theme:
  features:
    - navigation.prune  # Reduce navigation size
    - navigation.indexes  # Use section indexes
```

---

**Next**: Explore the [API Reference](../reference/index.md) to see auto-generated documentation from your code.
