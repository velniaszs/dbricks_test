# AGENTS.md

Multi-source lakehouse framework on Databricks. AAS Doors is the first source system onboarded. Python 3.12 project using uv package manager.

Bootstrapped from the Bosch lakehouse project template on 2026-08-18. **The bootstrap is done — do not re-run it.** The skill at `.github/skills/bootstrap-project/SKILL.md` is retained for reference only.

## Setup Commands

- Install dependencies: `uv sync --dev`
- Activate environment: `source .venv/bin/activate`

## Build & Test

- Run tests: `uv run pytest`
- Run tests with coverage: `uvx --with pytest-cov pytest --cov=src/ --cov-report=html:build/coverage tests/`
- Lint code: `uv run ruff check`
- Format code: `uv run ruff format`
- Pre-commit hooks: `uvx pre-commit run --all-files`

## Documentation

- Serve docs locally: `uv run mkdocs serve`
- Build docs: `uv run mkdocs build`
- Deploy to GitHub Pages: `uv run mkdocs gh-deploy --force`

## Project Structure

- `src/bedi_lakehouse/` - Main package source code (V2)
- `tests/` - Test files
- `docs/` - Documentation (MkDocs, **published publicly** to GitHub Pages)
- `design/` - Architecture and design records, deliberately **not** published
- `config/` - Source, mapping and environment declarations loaded into `meta`
- `resources/` - Databricks Asset Bundle job definitions
- `databricks_v1/` - **FROZEN** V1 PoC, reconciliation baseline. Do not edit.
- `databricks_V2/` - Notebook-era V2 design record and the AUTO CDC spike. Superseded by `design/`.
- `input/` - Local source extracts, **git-ignored, contains customer data**
- `build/coverage/` - Coverage reports output
- `site/` - Generated documentation output
- `pyproject.toml` - Project configuration
- `databricks.yml` - Asset Bundle config (root bundle is the one in use)
- `mkdocs.yml` - Documentation configuration

## Project Conventions

- **Write plain Python modules, not Databricks notebooks.** No `# COMMAND ----------` markers, no top-level side effects. Jobs invoke entry points via Asset Bundle wheel tasks.
- **`databricks_v1/` and `databricks_V2/` are excluded from Ruff** (`extend-exclude` in `pyproject.toml`). They are notebook-source format and frozen.
- **Never commit data.** `input/` is git-ignored. Do not add source extracts, CSVs or parquet anywhere in the repo.
- Architecture docs are deliberately kept out of `docs/` so they are not published to GitHub Pages.

## Code Style

- Line length: 120 characters
- Indent: 4 spaces
- Quotes: Double quotes
- Docstrings: Google style
- Linter: ruff with rules E, F, I, N, PLR

## Testing Conventions

- Test files: `test_*.py` or `*_test.py`
- Test classes: `Test*`
- Test functions: `test_*`
- Coverage target: 80% minimum

## Commit Guidelines

- Commit frequently with clear, brief messages
- Avoid buzzwords
- Run `uv run ruff check` and `uv run pytest` before committing

## Agent Behavior Guidelines

- Never implement, create, or modify files without explicit human permission. Always ask first.
- Conduct thorough research before any implementation.
- Include a research phase for technology decisions. Document findings and rationale before proceeding.
- When working with plugin ecosystems (like MkDocs), research built-in options, popular third-party alternatives, and integration patterns.
- Compare multiple approaches and document why specific technologies were chosen over alternatives.
- Provide concise and precise responses. Avoid unnecessary length.
- Do not implement changes without human approval. First, analyze the requirement and present your understanding for review.
