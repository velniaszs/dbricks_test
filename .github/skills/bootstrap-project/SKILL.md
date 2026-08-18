---
name: bootstrap-project
description: Bootstrap a new Lakehouse project from this template. Customizes package name, author info, GitHub URLs, Databricks workspace, and renames source directories. Trigger phrases - bootstrap project, customize template, setup new project, initialize project, rename package, configure for my organization, personalize template, create project from template, scaffold project.
---

# Bootstrap New Lakehouse Project

This skill helps customize the Lakehouse Project Template for a new project.

## Required Information

Ask the user for:

1. **Project name** (kebab-case, e.g., `my-lakehouse-project`)
2. **Package name** (snake_case, e.g., `my_lakehouse_project`) - derived from project name
3. **Description** - One-line project description
4. **Author name** - Full name
5. **Author email** - Email address
6. **GitHub organization/user** - e.g., `my-org` or `my-username`
7. **Databricks workspace URL** - e.g., `https://adb-1234567890.azuredatabricks.net`
8. **Version** - the template ships an inconsistent version; confirm one value and apply it to
   `pyproject.toml`, `src/{package_name}/__init__.py` and the assert in `tests/test_import.py`.

Also check with the user before publishing docs: the CI pipeline deploys `docs/` to **public**
GitHub Pages on every push to `main`. Do not move confidential design documents into `docs/`.

## Files to Update

### Critical Priority

| File | Changes |
|------|---------|
| `pyproject.toml` | name, description, authors, maintainers, URLs, packages path |
| `mkdocs.yml` | site_name, site_description, site_url, site_author, repo_name, repo_url, pymdownx.magiclink settings |
| `databricks.yml` | bundle name, workspace host URLs, team tags |
| `src/lakehouse_project_template/` | **Rename directory** to `src/{package_name}/` |
| `src/{package_name}/__init__.py` | Update docstring, `__author__`, `__email__` |

### High Priority

| File | Changes |
|------|---------|
| `README.md` | Title, badge URLs, clone URL, package path in structure, remove the "Using as Template" section |
| `tests/test_import.py` | Update import statement, package references and asserted version |
| `.devcontainer/linux/devcontainer.json` | Update container name (template ships duplicate `linux/` and `windows/` configs — consider collapsing to a single `.devcontainer/devcontainer.json` and fixing the `build.dockerfile`/`build.context` paths) |
| `.devcontainer/windows/devcontainer.json` | Update container name |
| `.devcontainer/dockerfile` | Update header comment |
| `.github/workflows/ci.yml` | Workflow `name:` |
| `.github/workflows/release.yml` | Initial-release message |
| `.github/copilot-instructions.md` | Replace bootstrap instructions with project rules |
| `.github/template-settings.yml` | Delete, or rewrite as project metadata |
| `AGENTS.md` | Update description and package path |

### Documentation

| File | Changes |
|------|---------|
| `docs/index.md` | Project name, clone URL, remove "Using as Template" section |
| `docs/getting-started/installation.md` | Clone URL, package path, remove template/bootstrap sections |
| `docs/getting-started/quick-start.md` | Clone URL, package path |
| `docs/getting-started/configuration.md` | Package name, version, bundle name |
| `docs/getting-started/repository-setup.md` | Project name |
| `docs/development/ci-cd.md` | Badge and Actions URLs, remove hardcoded test/coverage counts |
| `docs/development/code-quality.md` | Package path in examples |
| `docs/development/documentation.md` | site_url, repo_name, repo_url, package path in examples |
| `docs/development/testing.md` | Project name |

## Step-by-Step Process

### 1. Rename Source Directory

```bash
mv src/lakehouse_project_template src/{package_name}
```

### 2. Update pyproject.toml

```toml
[project]
name = "{project-name}"
description = "{description}"
authors = [{ name = "{author_name}", email = "{author_email}" }]
maintainers = [{ name = "{author_name}", email = "{author_email}" }]

[project.urls]
Homepage = "https://github.com/{github_org}/{project-name}"
Documentation = "https://{github_org}.github.io/{project-name}"
Repository = "https://github.com/{github_org}/{project-name}"
Issues = "https://github.com/{github_org}/{project-name}/issues"

[tool.hatch.build.targets.wheel]
packages = ["src/{package_name}"]
```

### 3. Update mkdocs.yml

```yaml
site_name: {Project Name}
site_description: {description}
site_url: https://{github_org}.github.io/{project-name}
site_author: {author_name}
repo_name: {github_org}/{project-name}
repo_url: https://github.com/{github_org}/{project-name}

# In pymdownx.magiclink:
user: {github_org}
repo: {project-name}

# In extra.social:
link: https://github.com/{github_org}/{project-name}
```

### 4. Update databricks.yml

```yaml
bundle:
  name: {project-name}

targets:
  local:
    workspace:
      host: {databricks_host}
  dev:
    workspace:
      host: {databricks_host}
```

### 5. Update src/{package_name}/__init__.py

```python
"""{Project Name} - {description}"""

__version__ = "{version}"
__author__ = "{author_name}"
__email__ = "{author_email}"
```

### 6. Update tests/test_import.py

```python
import {package_name}

def test_package_import():
    assert {package_name} is not None

def test_package_metadata():
    assert {package_name}.__version__ == "{version}"
    assert isinstance({package_name}.__author__, str)
    assert "@" in {package_name}.__email__
```

### 7. Update README.md

- Title: `# {Project Name}`
- Badge URLs: `https://github.com/{github_org}/{project-name}/actions/...`
- Clone command: `git clone https://github.com/{github_org}/{project-name}.git`
- Structure diagram: `src/{package_name}/`

### 8. Update remaining documentation files

Apply similar URL and name replacements to all docs/*.md files.

### 9. Check the devcontainer proxy settings

The template hardcodes `http://host.docker.internal:3128` in `containerEnv`. Off a corporate
network this makes every request inside the container fail *after* a successful image build —
`postCreateCommand` hangs on `uv sync`. Change those entries to `${localEnv:HTTP_PROXY}` (and the
lowercase variants) so they resolve to empty when no proxy is configured.

## Post-Bootstrap Steps

After customization, run:

```bash
# Regenerate lock file (the old package name is baked into uv.lock)
uv sync --dev

# Verify tests pass
uv run pytest

# Verify linting passes
uv run ruff check

# Verify docs build
uv run mkdocs build
```

If the repository contains pre-existing code that should not be linted (for example a frozen
reference implementation in Databricks notebook-source format), add it to `extend-exclude`
under `[tool.ruff]` — CI runs `ruff check` and `ruff format --check` across the whole repo.

## Cleanup (Optional)

After successful bootstrap, user may want to:

1. Delete `.github/skills/bootstrap-project/` directory
2. Update `AGENTS.md` to remove template-specific references
3. Commit all changes: `git add -A && git commit -m "Bootstrap {project-name} from template"`
