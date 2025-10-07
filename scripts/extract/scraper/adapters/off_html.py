from bs4 import BeautifulSoup
import json, re

# -----------------------------------------------------------
# 🔧 Helper functions
# -----------------------------------------------------------

def _text(el):
    return el.get_text(strip=True) if el else None

def _find_dd(soup, labels):
    if isinstance(labels, str):
        labels = [labels]
    for lab in labels:
        dt = soup.find("dt", string=lambda s: s and lab.lower() in s.lower())
        if dt:
            dd = dt.find_next_sibling("dd")
            if dd:
                return _text(dd)
    return None

def _tags_texts(soup, selector):
    """Return a list of anchor texts under a container (e.g. #field_*_value a)."""
    box = soup.select_one(selector)
    if not box:
        return None
    out = []
    for a in box.select("a"):
        t = _text(a)
        if t:
            out.append(t)
    return out or None

def looks_like_product_page(html: str) -> bool:
    """OFF uses og:type=food on FR pages; accept both food/product."""
    soup = BeautifulSoup(html, "html.parser")
    og = soup.select_one("meta[property='og:type']")
    if og and og.get("content","").lower() in {"product","food"}:
        return True

    canon = soup.find("link", rel="canonical")
    if canon and ("/product/" in (canon.get("href","")) or "/produit/" in (canon.get("href",""))):
        return True

    for tag in soup.find_all("script", attrs={"type":"application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            items = data if isinstance(data, list) else [data]
            if any(isinstance(x, dict) and str(x.get("@type","")).lower()=="product" for x in items):
                return True
        except Exception:
            pass
    return False

def _parse_jsonld_product(soup):
    """Prefer structured data when present."""
    for tag in soup.find_all("script", attrs={"type":"application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for d in items:
            if not isinstance(d, dict):
                continue
            if str(d.get("@type","")).lower()=="product":
                out = {}
                out["nom"] = d.get("name")
                brand = d.get("brand")
                if isinstance(brand, dict):
                    out["marque"] = brand.get("name")
                elif isinstance(brand, str):
                    out["marque"] = brand
                img = d.get("image")
                out["image"] = (img[0] if isinstance(img, list) and img else img)
                out["code_barres"] = d.get("gtin13") or d.get("gtin") or d.get("sku")
                price = None
                offers = d.get("offers")
                if isinstance(offers, dict):
                    price = offers.get("price")
                out["prix"] = float(price) if isinstance(price, (int,float,str)) and str(price).replace(".","",1).isdigit() else None
                return out
    return None

def _barcode_from(url: str):
    m = re.search(r"/(?:product|produit)/(\d{8,14})", url or "")
    return m.group(1) if m else None

def _derive_weight_from_title(title: str) -> str | None:
    if not title:
        return None
    # capture "230g", "400 g", "1,5 L", "1.5L", "750 ml"
    m = re.search(r"(\d[\d\s.,]*\s?(?:g|kg|ml|l))\b", title, flags=re.I)
    return m.group(1).replace(" ", "") if m else None

def _parse_nutrition_table(soup):
    """Robust-ish nutrition table parser for OFF variants."""
    nutriments = {"protides": None, "glucides": None, "lipides": None, "calories": None}

    table = (
        soup.find("table", {"id":"nutrition_table"})
        or soup.find("table", {"id":"nutrition_data_table"})
        or soup.find("table", class_=re.compile("nutrition", re.I))
    )
    if not table:
        return nutriments

    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True).lower() for c in tr.find_all(["th","td"])]
        row = " ".join(cells)

        def num(s):
            m = re.search(r"([0-9]+(?:[.,][0-9]+)?)", s)
            return float(m.group(1).replace(",", ".")) if m else None

        # energy / calories
        if any(k in row for k in ["énergie", "energy", "kcal", "kj"]):
            kcal = re.search(r"(\d+(?:[.,]\d+)?)\s*kcal", row)
            nutriments["calories"] = float(kcal.group(1).replace(",", ".")) if kcal else num(row)
        # proteins
        if "protéines" in row or "protein" in row:
            nutriments["protides"] = num(row)
        # carbohydrates
        if "glucides" in row or "carbohydrate" in row:
            nutriments["glucides"] = num(row)
        # fats
        if "matières grasses" in row or "fat" in row:
            nutriments["lipides"] = num(row)

    return nutriments


# -----------------------------------------------------------
# 🧠 Main parser
# -----------------------------------------------------------

def parse_off_product_html(html, url):
    soup = BeautifulSoup(html, "html.parser")

    # 1) JSON-LD first (if any)
    rec = _parse_jsonld_product(soup) or {}

    # 2) Direct selectors from your DOM inspection
    title_el = soup.select_one("h1[property='food:name'][itemprop='name']")
    nom = rec.get("nom") or _text(title_el)

    gen_el = soup.select_one("#field_generic_name_value [itemprop='description']")
    nom_generique = _text(gen_el)

    barcode_el = soup.select_one("span#barcode[itemprop='gtin13']")
    code_barres = rec.get("code_barres") or (barcode_el and barcode_el.get_text(strip=True))

    q_el = soup.select_one("#field_quantity_value")
    poids_unite = _text(q_el) or _derive_weight_from_title(nom)

    emballage_el = soup.select_one("#field_packaging_value")
    emballage = _text(emballage_el)

    brand_el = soup.select_one("#field_brands_value")
    marque = rec.get("marque") or _text(brand_el)
    brands_tags = [a.get_text(strip=True) for a in brand_el.select("a")] if brand_el else None

    cat_el = soup.select_one("#field_categories_value")
    categories = [a.get_text(strip=True) for a in cat_el.select("a")] if cat_el else None

    # 3) Nutrition table
    nutriments = _parse_nutrition_table(soup)

    # 4) Origins / manufacturing places / stores / countries of sale
    origins_list   = _tags_texts(soup, "#field_origins_value")
    manuf_places   = _tags_texts(soup, "#field_manufacturing_places_value")
    stores_list    = _tags_texts(soup, "#field_stores_value")
    countries_list = _tags_texts(soup, "#field_countries_value")

    pays_origine = ", ".join(origins_list) if origins_list else None
    fabricant = ", ".join(manuf_places) if manuf_places else None
    distributeur = ", ".join(stores_list) if stores_list else None
    pays_vente = ", ".join(countries_list) if countries_list else None

    # 5) Nutri-Score
    score_nutri = None
    h4 = soup.find("h4", string=lambda s: s and "Nutri-Score" in s)
    if h4:
        m = re.search(r"Nutri-Score\s*([A-E])", h4.get_text(), re.I)
        if m:
            score_nutri = m.group(1).upper()
    if not score_nutri and h4 and h4.has_attr("class"):
        cls = " ".join(h4["class"]).lower()
        m = re.search(r"grade_([a-e])_title", cls)
        if m:
            score_nutri = m.group(1).upper()

    # 6) Image fallback
    if not rec.get("image"):
        rec["image"] = (soup.select_one("meta[name='x:image']") or {}).get("content") \
                       or (soup.select_one("meta[property='og:image']") or {}).get("content")

    if not code_barres:
        code_barres = _barcode_from(url) \
            or _barcode_from((soup.select_one("meta[property='og:url']") or {}).get("content")) \
            or _barcode_from((soup.find("link", rel="canonical") or {}).get("href"))

    # -------------------------------------------------------
    # ✅ Final structured record
    # -------------------------------------------------------
    return {
        "nom": nom,
        "nom_générique": nom_generique,
        "brands_tags": brands_tags,
        "marque": marque,
        "prix": rec.get("prix"),
        "image": rec.get("image"),
        "score_nutritionnel": score_nutri,
        "nutriments": nutriments,
        "poids_unité": poids_unite,
        "emballage": emballage,
        "categories": categories,
        "pays_origine": pays_origine,
        "fabricant": fabricant,
        "pays_commercialisation": pays_vente,
        "distributeur": distributeur,
        "code_barres": code_barres,
        "date_peremption": None,
        "source": url,
        "index": code_barres or url,
    }
