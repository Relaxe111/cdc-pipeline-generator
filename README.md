# cdc-generator-pipeline
Reusable library for generating Redpanda Connect CDC pipelines.

## How to Rename This Repository

To rename this repository, follow these steps:

### Step 1: Rename on GitHub
1. Navigate to the repository on GitHub: https://github.com/Relaxe111/cdc-generator-pipeline
2. Click on **Settings** (you need admin/owner permissions)
3. Scroll down to the **Repository name** section
4. Enter the new repository name
5. Click **Rename**

GitHub will automatically:
- Set up redirects from the old URL to the new URL
- Update git remotes when you push/pull (but it's better to update manually)

### Step 2: Update Local Repository
After renaming on GitHub, update your local repository:

```bash
# Update the remote URL to the new repository name
git remote set-url origin https://github.com/Relaxe111/NEW-REPO-NAME

# Verify the change
git remote -v
```

### Step 3: Update References in Files
After renaming, update these files in your repository:

1. **README.md**: Update the repository name in the header and any URLs
2. **LICENSE**: The license file contains the copyright holder (Relaxe111) which doesn't need to change unless you want to
3. Any other configuration files, documentation, or code that references the old repository name

### Step 4: Notify Collaborators
Inform all collaborators about the rename so they can update their local clones:

```bash
# They should run this command:
git remote set-url origin https://github.com/Relaxe111/NEW-REPO-NAME
```

### Important Notes
- GitHub redirects will work, but it's best practice to update all references
- Update any CI/CD pipelines, deployment scripts, or external integrations
- Update any documentation, wikis, or external links
- If you have GitHub Pages, update the URL references
- Update any badges in README.md that reference the repository URL
