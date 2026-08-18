# CI/CD Pipeline

The AAS Doors Lakehouse project includes a comprehensive CI/CD pipeline powered by GitHub Actions that ensures code quality and automates deployment.

## Pipeline Overview

The CI/CD pipeline consists of two main jobs:

### 🧪 Test & Quality Checks

Runs on **every push and pull request** to ensure code quality:

```yaml
- Checkout repository
- Install uv with caching
- Set up Python 3.12
- Install dependencies (uv sync --dev)
- Run ruff linting
- Run ruff formatting check
- Run tests with coverage
- Store coverage artifacts
```

**Quality Gates:**
- ✅ All tests must pass
- ✅ Ruff linting must pass (no violations)
- ✅ Code formatting must be consistent
- ✅ Coverage reports generated (HTML + XML)

### 📚 Documentation Build & Deploy

Runs **only on main branch** after tests pass:

```yaml
- Checkout repository
- Install uv with caching
- Set up Python 3.12
- Install dependencies
- Download coverage artifacts
- Build documentation (mkdocs)
- Deploy to GitHub Pages
```

**Deployment:**
- 🌐 Automatic deployment to GitHub Pages
- 📊 Coverage reports integrated into docs
- 🔄 Updates on every main branch push

## Workflow Features

### ⚡ Performance Optimizations

- **uv Caching**: Dependencies cached based on `pyproject.toml`
- **Parallel Jobs**: Test and docs jobs run efficiently
- **Artifact Sharing**: Coverage reports shared between jobs
- **Conditional Deployment**: Docs only deploy from main branch

### 🔒 Security & Permissions

```yaml
permissions:
  contents: read      # Read repository code
  pages: write        # Deploy to GitHub Pages
  id-token: write     # OIDC token for Pages
```

### 🚀 Concurrency Control

```yaml
concurrency:
  group: "pages"
  cancel-in-progress: false  # Don't cancel production deployments
```

## Status Badges

The README includes live status badges:

- **CI/CD Status**: ![CI/CD](https://github.com/velniaszs/dbricks_test/actions/workflows/ci.yml/badge.svg)

## Local Testing

Test the same commands locally before pushing:

```bash
# Quality checks (same as CI)
uv run ruff check
uv run ruff format --check

# Tests with coverage (same as CI)
uvx --with pytest-cov pytest --cov=src/ --cov-report=html:build/coverage tests/

# Documentation build (same as CI)
uv run mkdocs build
```

## Workflow Triggers

### Automatic Triggers

- **Push to main**: Full pipeline (test + deploy)
- **Pull Request**: Test job only
- **Manual**: Can be triggered from GitHub Actions tab

### Branch Protection

Recommended branch protection rules:

- ✅ Require status checks to pass
- ✅ Require branches to be up to date
- ✅ Require review from code owners
- ✅ Dismiss stale reviews when new commits are pushed

## Monitoring & Debugging

### GitHub Actions

- View workflow runs: [Actions tab](https://github.com/velniaszs/dbricks_test/actions)
- Download artifacts: Coverage reports available for 30 days
- Check logs: Detailed logs for each step

### Coverage Reports

- **Local HTML Report**: `build/coverage/index.html`
- **Coverage Data**: `build/.coverage` (binary data file)
- **Integrated Docs**: Coverage included in documentation site
- **GitHub Actions**: Coverage artifacts available for 30 days

All coverage files are organized in the `build/` directory to keep the project root clean.

## Customization

### Adding New Checks

Add steps to the `test` job:

```yaml
- name: Custom check
  run: your-command-here
```

### Modifying Deployment

Update the `docs` job for different deployment targets:

```yaml
- name: Deploy to custom target
  run: your-deployment-command
```

### Environment Variables

Set secrets in repository settings for:

- Custom deployment credentials
- Third-party service tokens

## Best Practices

### Development Workflow

1. **Create feature branch** from main
2. **Make changes** with tests
3. **Run local checks** before pushing
4. **Create pull request** (triggers test job)
5. **Review and merge** (triggers full pipeline)

### Code Quality

- **Write tests** for all new features
- **Maintain coverage** above 80% (currently 100%)
- **Follow formatting** rules (ruff handles this)
- **Update docs** for significant changes

### Performance

- **Cache dependencies** when possible
- **Use artifacts** for sharing between jobs
- **Optimize test execution** time
- **Monitor workflow duration**

---

The CI/CD pipeline ensures that the project maintains high quality standards while providing fast feedback to developers. Every commit is automatically tested, and successful changes are immediately deployed to production.
