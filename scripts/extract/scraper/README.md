# Scraper — OpenFoodFacts HTML → Schéma Produit

> ⚠️ Educational demo. Prefer the official API when possible. If you still scrape HTML pages,
> always check and respect **robots.txt** and Terms, set a custom User-Agent, and keep a polite delay.

## What this does
- Takes **barcodes or product page URLs** from a text file
- Downloads each **HTML product page** from OpenFoodFacts (world domain)
- Parses fields and maps them to *your schema*:
  - nom, nom_générique, brands_tags, marque, prix (None if unavailable), image,
    score_nutritionnel, nutriments{{protides, glucides, lipides, calories}},
    poids_unité, pays_origine, fabricant, pays_commercialisation, distributeur,
    code_barres, date_peremption, source, index
- Saves to JSONL or CSV in `out/`

## Quickstart
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate
pip install -r requirements.txt

# Run (barcodes list)
python runner.py --input sample_barcodes.txt --format jsonl --delay 1.2

# Or with explicit URLs
python runner.py --input urls.txt --format csv --delay 1.2
```

## Inputs
- `sample_barcodes.txt` (one EAN per line)
- Or a file with full URLs like `https://world.openfoodfacts.org/product/3017620422003`

## Output
- `out/products_YYYYMMDD_HHMMSS.jsonl` or `.csv`

## Legal & Ethics
- Scrape only pages you’re allowed to, at a **low rate**.
- Identify yourself with a meaningful User-Agent string.
- For Open Food Facts, **prefer the API** for stability and performance.

