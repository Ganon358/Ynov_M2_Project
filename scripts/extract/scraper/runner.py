import argparse, os, sys, re, requests
from tenacity import retry, stop_after_attempt, wait_exponential
from adapters.off_html import parse_off_product_html, looks_like_product_page
from utils import polite_sleep, ensure_outdir, write_jsonl, write_csv, now_stamp

# A clear UA + FR language helps OFF serve full product pages
DEFAULT_UA = "Zineb-ETL-Scraper/1.3 (+contact@ynov.example)"
HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr,fr-FR;q=0.9,en;q=0.8",
}

def as_world_url(s: str) -> str:
    """Barcode or URL -> world URL"""
    return s if s.startswith("http") else f"https://world.openfoodfacts.org/product/{s}"

def as_fr_url_from_barcode_or_url(s: str) -> str:
    """Barcode or world URL -> French domain product URL"""
    if s.startswith("http"):
        # try to capture barcode from url
        m = re.search(r"/product/(\d{8,14})", s)
        if not m:
            m = re.search(r"/produit/(\d{8,14})", s)
        bc = m.group(1) if m else s
        return f"https://fr.openfoodfacts.org/produit/{bc}"
    else:
        return f"https://fr.openfoodfacts.org/produit/{s}"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def fetch_html(url, timeout=30, user_agent=DEFAULT_UA):
    headers = {**HEADERS_BASE, "User-Agent": user_agent}
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    if "text/html" not in r.headers.get("content-type",""):
        raise RuntimeError(f"Unexpected content-type: {r.headers.get('content-type')}")
    return r.text, r.url  # return final url after redirects

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="File with barcodes or URLs (one per line)")
    ap.add_argument("--format", choices=["jsonl","csv"], default="jsonl")
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--user-agent", default=DEFAULT_UA)
    args = ap.parse_args()

    if not os.path.isfile(args.input):
        print(f"Input file not found: {args.input}", file=sys.stderr); sys.exit(1)

    with open(args.input, "r", encoding="utf-8") as f:
        items = [line.strip() for line in f if line.strip()]

    results = []
    for i, item in enumerate(items, start=1):
        world_url = as_world_url(item)
        try:
            html, final_world = fetch_html(world_url, user_agent=args.user_agent)

            # If the world page doesn't look like a product, try the FR variant
            if not looks_like_product_page(html):
                fr_url = as_fr_url_from_barcode_or_url(final_world)
                html, final_fr = fetch_html(fr_url, user_agent=args.user_agent)
                final_url = final_fr
            else:
                final_url = final_world

            rec = parse_off_product_html(html, final_url)
            results.append(rec)
            print(f"[{i}/{len(items)}] OK {final_url}")
        except Exception as e:
            print(f"[{i}/{len(items)}] FAIL {world_url} -> {e}", file=sys.stderr)
        polite_sleep(args.delay)

    ensure_outdir()
    ts = now_stamp()
    outpath = f"out/products_{ts}.{args.format}"
    if args.format == "jsonl":
        write_jsonl(results, outpath)
    else:
        field_order = [
            "nom","nom_générique","brands_tags","marque","prix","image",
            "score_nutritionnel","nutriments","poids_unité","pays_origine",
            "fabricant","pays_commercialisation","distributeur","code_barres",
            "date_peremption","source","index"
        ]
        write_csv(results, outpath, field_order=field_order)
    print(f"[DONE] Wrote {len(results)} records -> {outpath}")

if __name__ == "__main__":
    main()
