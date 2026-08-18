# Repository Setup

This guide helps you set up your GitHub repository to work with the project's CI/CD pipeline.

## GitHub Repository Configuration

### 1. Enable GitHub Pages

To enable automatic documentation deployment:

1. Go to your repository **Settings** → **Pages**
2. Under **Source**, select **GitHub Actions**
3. Save the configuration

The CI/CD pipeline will automatically deploy documentation to GitHub Pages on every push to the main branch.

### 2. Branch Protection (Recommended)

Protect your main branch to ensure code quality:

1. Go to **Settings** → **Branches**
2. Click **Add rule** for the `main` branch
3. Enable these settings:
   - ✅ **Require status checks to pass before merging**
   - ✅ **Require branches to be up to date before merging**
   - ✅ **Require a pull request before merging**
   - ✅ **Dismiss stale pull request reviews when new commits are pushed**

### 3. Required Status Checks

Add these required status checks:

- `Test & Quality Checks` - Ensures all tests pass
- `Build & Deploy Documentation` - Ensures docs build successfully

## Optional Integrations

### Repository Secrets

Add these secrets if needed:

| Secret Name | Purpose | Required |
|-------------|---------|----------|
| `CUSTOM_DEPLOY_TOKEN` | Custom deployment credentials | Optional |

## Workflow Permissions

The CI/CD workflow requires these permissions (automatically configured):

```yaml
permissions:
  contents: read      # Read repository code
  pages: write        # Deploy to GitHub Pages
  id-token: write     # OIDC token for Pages deployment
```

## Repository Settings Checklist

### General Settings

- [ ] Repository is public or has GitHub Pages enabled for private repos
- [ ] Default branch is set to `main`
- [ ] Repository description includes project purpose
- [ ] Topics/tags are added for discoverability

### Actions Settings

- [ ] **Actions permissions**: Allow all actions and reusable workflows
- [ ] **Workflow permissions**: Read and write permissions
- [ ] **Fork pull request workflows**: Require approval for first-time contributors

### Pages Settings

- [ ] **Source**: GitHub Actions
- [ ] **Custom domain**: Configure if using custom domain
- [ ] **Enforce HTTPS**: Enabled (recommended)

### Security Settings

- [ ] **Dependency graph**: Enabled
- [ ] **Dependabot alerts**: Enabled
- [ ] **Dependabot security updates**: Enabled
- [ ] **Code scanning**: Configure if desired

## Verification

After setup, verify everything works:

### 1. Test CI/CD Pipeline

Create a test commit:

```bash
# Make a small change
echo "# Test" >> test.md
git add test.md
git commit -m "test: Verify CI/CD pipeline"
git push
```

Check that:
- [ ] GitHub Actions workflow runs successfully
- [ ] All tests pass
- [ ] Documentation builds and deploys
- [ ] Status badges update in README

### 2. Test Pull Request Workflow

Create a test PR:

```bash
# Create feature branch
git checkout -b test-feature
echo "# Feature test" >> feature.md
git add feature.md
git commit -m "feat: Test feature branch"
git push -u origin test-feature
```

Create PR and verify:
- [ ] CI runs on PR
- [ ] Status checks appear
- [ ] Branch protection rules work
- [ ] Documentation preview available

### 3. Verify Documentation

Check your documentation site:

- [ ] Visit `https://yourusername.github.io/your-repo-name`
- [ ] Navigation works correctly
- [ ] Coverage reports are integrated
- [ ] All pages load properly
- [ ] Search functionality works

## Troubleshooting

### Common Issues

**GitHub Pages not deploying:**
- Check Pages settings are configured for "GitHub Actions"
- Verify workflow has `pages: write` permission
- Check workflow logs for deployment errors

**Status checks not required:**
- Ensure branch protection rules are enabled
- Add specific status check names exactly as they appear
- Wait for first successful run to see available checks

**Workflow failing:**
- Check Python version compatibility (requires 3.12)
- Verify all dependencies are in `pyproject.toml`
- Review workflow logs for specific error messages

### Getting Help

- **GitHub Actions**: Check the [Actions tab](https://github.com/your-username/your-repo/actions) for detailed logs
- **Documentation**: Review the [CI/CD Pipeline](../development/ci-cd.md) documentation
- **Issues**: Open an issue in the repository for specific problems

## Next Steps

After repository setup:

1. **Customize the project** for your needs
2. **Update documentation** with your project details
3. **Add your code** and tests
4. **Configure additional integrations** as needed
5. **Share your project** with the community

Your Lakehouse project repository is now ready for development with full CI/CD automation! 🔷
