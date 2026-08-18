# Copilot Instructions

Lakehouse pipelines for AAS Doors requirement extracts on Databricks.

See [AGENTS.md](../AGENTS.md) for setup commands, project structure, and conventions.

## Key rules

- **Plain Python modules, not Databricks notebooks.** New code goes in `src/aas_doors_lakehouse/` and is invoked from Asset Bundle wheel tasks.
- **`databricks_v1/` is frozen.** It is the reconciliation baseline; editing it invalidates the comparison. Excluded from Ruff.
- **Never commit data.** `input/` holds customer extracts and is git-ignored.
- Ask before implementing. Research and present findings first.

## Bootstrap

This project was already bootstrapped from the Bosch lakehouse template on 2026-08-18.
The skill at `.github/skills/bootstrap-project/SKILL.md` is kept for reference only.

If the user asks to bootstrap or scaffold the project, set up or initialize a new project,
customize the template, create a project from template, or rename the package — **do not run
the skill against this repository.** Say the bootstrap is already done and confirm what they
actually want first. The skill applies to a *new* repo created from the template, or to a
deliberate package rename when this repo moves to its final remote.
