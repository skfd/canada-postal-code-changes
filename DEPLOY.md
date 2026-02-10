# Deployment Guide

## Publishing to GitHub

### 1. Create GitHub Repository

1. Go to https://github.com/new
2. Repository name: `postal-codes` (or your preferred name)
3. Description: "Canadian postal code change tracking and visualization"
4. Public or Private: Choose based on preference
5. **Do NOT** initialize with README, .gitignore, or license (we already have these)
6. Click "Create repository"

### 2. Push to GitHub

```bash
# Add the remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/postal-codes.git

# Push to GitHub
git push -u origin main
```

### 3. Enable GitHub Pages

1. Go to your repository on GitHub
2. Click **Settings** → **Pages** (in left sidebar)
3. Under "Source":
   - Branch: Select `main`
   - Folder: Select `/static-site`
   - Click **Save**
4. Wait 1-2 minutes for deployment

Your site will be available at:
```
https://YOUR_USERNAME.github.io/postal-codes/
```

### 4. Update Links After Deployment

After deploying, update the following file with your actual GitHub username:

**README.md** - Line with GitHub Pages demo URL:
```markdown
**[GitHub Pages Demo](https://YOUR_USERNAME.github.io/postal-codes/)**
```

Then commit and push:
```bash
git add README.md
git commit -m "Update GitHub Pages URL"
git push
```

## Updating the GitHub Pages Site

When you have new data to publish:

```bash
# 1. Download and process latest data
python -m src.cli download
python -m src.cli process
python -m src.cli diff

# 2. Regenerate static site data
python -m src.cli generate-static

# 3. Commit and push the updated data
git add static-site/data/
git commit -m "Update postal code data (YYYY-MM-DD)"
git push

# GitHub Pages will automatically redeploy within 1-2 minutes
```

## Repository Settings Checklist

After pushing to GitHub, consider:

- [ ] Add repository description and website URL in repository settings
- [ ] Add topics/tags: `postal-codes`, `canada`, `open-data`, `visualization`
- [ ] Update README.md with actual GitHub Pages URL
- [ ] Consider adding a screenshot to the README
- [ ] Set up repository social preview image (optional)

## Data Size Notes

The static site data files total ~25MB, which is well within GitHub's limits:
- GitHub Pages: 1GB site size limit
- Git repository: 100MB per file recommended limit
- Largest file: city_changed.json (~16MB)

All files are under limits and suitable for GitHub hosting.
