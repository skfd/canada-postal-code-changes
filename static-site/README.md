# Static Site for GitHub Pages

This directory contains the static site that can be deployed to GitHub Pages.

## Directory Structure

- `index.html` - Main HTML file with embedded CSS and JavaScript
- `data/` - JSON data files generated from the database

## Generating Data Files

Before committing for GitHub Pages deployment, generate the data files:

```bash
# From the project root
python -m src.cli generate-static
```

This creates the following JSON files in `data/`:
- `summary.json` - Overall statistics
- `timeline.json` - Changes by time period
- `by_province.json` - Changes by province
- `added.json` - All added postal codes
- `removed.json` - All removed postal codes
- `city_changed.json` - All city name changes

## GitHub Pages Setup

1. Generate the data files (see above)
2. Commit the `static-site/` directory including `data/` files
3. Push to GitHub
4. In GitHub repository settings:
   - Go to Settings → Pages
   - Set Source to "Deploy from a branch"
   - Select branch: `main`
   - Set folder to `/static-site`
   - Save

Your site will be available at: `https://YOUR_USERNAME.github.io/postal-codes/`

## Data Sources

This visualization uses data from:
- **Statistics Canada NAR** (Open Licence)
- **Geocoder.ca** (CC BY 2.5)
- **GeoNames** (CC BY 4.0)

See the main [README.md](../README.md) for full attribution and licensing details.
