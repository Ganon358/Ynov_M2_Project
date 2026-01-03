from bs4 import BeautifulSoup
import json
import re
from typing import Optional, List, Dict, Union
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from models.product import Product, NutrimentData
from core.sealed import sealed

class HTMLElementExtractor(metaclass=sealed(['_soup'])):
    def __init__(self, soup: BeautifulSoup):
        object.__setattr__(self, '_soup', soup)
    
    @property
    def soup(self) -> BeautifulSoup:
        return self._soup
    
    def extract_text(self, element) -> Optional[str]:
        return element.get_text(strip=True) if element else None
    
    def find_definition_data(self, labels: Union[str, List[str]]) -> Optional[str]:
        labels = [labels] if isinstance(labels, str) else labels
        for label in labels:
            dt = self.soup.find("dt", string=lambda s: s and label.lower() in s.lower())
            if dt:
                dd = dt.find_next_sibling("dd")
                if dd:
                    return self.extract_text(dd)
        return None
    
    def extract_anchor_texts(self, selector: str) -> Optional[List[str]]:
        container = self.soup.select_one(selector)
        if not container:
            return None
        
        texts = [
            self.extract_text(anchor) 
            for anchor in container.select("a")
            if self.extract_text(anchor)
        ]
        return texts or None


class ProductPageValidator(metaclass=sealed(['_soup', '_validators'])):
    def __init__(self, soup: BeautifulSoup):
        object.__setattr__(self, '_soup', soup)
        validators = {
            'og_type': self._validate_og_type,
            'canonical': self._validate_canonical,
            'jsonld': self._validate_jsonld
        }
        object.__setattr__(self, '_validators', validators)
    
    @property
    def soup(self) -> BeautifulSoup:
        return self._soup
    
    def __call__(self, **validation_kwargs) -> bool:
        if validation_kwargs:
            return any(
                validator() for name, validator in self._validators.items()
                if validation_kwargs.get(name, True)
            )
        return any(validator() for validator in self._validators.values())
    
    def is_valid_product_page(self) -> bool:
        return self()
    
    def _validate_og_type(self) -> bool:
        og_meta = self.soup.select_one("meta[property='og:type']")
        return (og_meta and 
                og_meta.get("content", "").lower() in {"product", "food"})
    
    def _validate_canonical(self) -> bool:
        canonical = self.soup.find("link", rel="canonical")
        return (canonical and 
                any(pattern in canonical.get("href", "") 
                    for pattern in ["/product/", "/produit/"]))
    
    def _validate_jsonld(self) -> bool:
        for script_tag in self.soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script_tag.string or "{}")
                items = data if isinstance(data, list) else [data]
                if any(isinstance(item, dict) and 
                      str(item.get("@type", "")).lower() == "product" 
                      for item in items):
                    return True
            except (json.JSONDecodeError, TypeError):
                continue
        return False


class JSONLDExtractor(metaclass=sealed(['_soup'])):
    def __init__(self, soup: BeautifulSoup):
        object.__setattr__(self, '_soup', soup)
    
    @property
    def soup(self) -> BeautifulSoup:
        return self._soup
    
    def extract_product_data(self) -> Optional[Dict]:
        for script_tag in self.soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script_tag.string or "{}")
                items = data if isinstance(data, list) else [data]
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    if str(item.get("@type", "")).lower() == "product":
                        return self._parse_product_item(item)
            except (json.JSONDecodeError, TypeError):
                continue
        return None
    
    def _parse_product_item(self, item: Dict) -> Dict:
        result = {"nom": item.get("name")}
        
        brand = item.get("brand")
        if isinstance(brand, dict):
            result["marque"] = brand.get("name")
        elif isinstance(brand, str):
            result["marque"] = brand
        
        image = item.get("image")
        result["image"] = (image[0] if isinstance(image, list) and image else image)
        
        result["code_barres"] = (item.get("gtin13") or 
                                item.get("gtin") or 
                                item.get("sku"))
        
        price = self._extract_price(item.get("offers"))
        result["prix"] = price
        
        return result
    
    def _extract_price(self, offers) -> Optional[float]:
        if isinstance(offers, dict):
            price = offers.get("price")
            if isinstance(price, (int, float, str)):
                try:
                    return float(price) if str(price).replace(".", "", 1).isdigit() else None
                except (ValueError, TypeError):
                    return None
        return None


class NutritionTableParser(metaclass=sealed(['_soup', '_nutrient_extractors'])):
    def __init__(self, soup: BeautifulSoup):
        object.__setattr__(self, '_soup', soup)
        extractors = {
            'calories': {
                'keywords': ["énergie", "energy", "kcal", "kj"],
                'extractor': self._extract_calories
            },
            'protides': {
                'keywords': ["protéines", "protein"],
                'extractor': self._extract_generic_nutrient
            },
            'glucides': {
                'keywords': ["glucides", "carbohydrate"],
                'extractor': self._extract_generic_nutrient
            },
            'lipides': {
                'keywords': ["matières grasses", "fat"],
                'extractor': self._extract_generic_nutrient
            }
        }
        object.__setattr__(self, '_nutrient_extractors', extractors)
    
    @property
    def soup(self) -> BeautifulSoup:
        return self._soup
    
    def __call__(self, **parsing_kwargs) -> NutrimentData:
        table = self._find_nutrition_table()
        if not table:
            return NutrimentData()
        
        nutriments = {}
        
        for row in table.find_all("tr"):
            cells = [cell.get_text(" ", strip=True).lower() for cell in row.find_all(["th", "td"])]
            row_text = " ".join(cells)
            
            for nutrient, config in self._nutrient_extractors.items():
                if parsing_kwargs.get(f'extract_{nutrient}', True):
                    if any(keyword in row_text for keyword in config['keywords']):
                        value = config['extractor'](row_text)
                        if value is not None:
                            nutriments[nutrient] = value
        
        return NutrimentData(**nutriments)
    
    def parse(self) -> NutrimentData:
        return self()
    
    def _find_nutrition_table(self):
        table_selectors = [
            ("id", "nutrition_table"),
            ("id", "nutrition_data_table"),
            ("class", re.compile("nutrition", re.I))
        ]
        
        for attr, value in table_selectors:
            table = self.soup.find("table", {attr: value})
            if table:
                return table
        return None
    
    def _extract_calories(self, row_text: str) -> Optional[float]:
        kcal_match = re.search(r"(\d+(?:[.,]\d+)?)\s*kcal", row_text)
        return (float(kcal_match.group(1).replace(",", ".")) 
                if kcal_match else self._extract_number(row_text))
    
    def _extract_generic_nutrient(self, row_text: str) -> Optional[float]:
        return self._extract_number(row_text)
    
    def _extract_number(self, text: str) -> Optional[float]:
        match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", text)
        if match:
            try:
                return float(match.group(1).replace(",", "."))
            except ValueError:
                return None
        return None

class BarcodeExtractor(metaclass=sealed()):
    @staticmethod
    def extract_from_url(url: str) -> Optional[str]:
        if not url:
            return None
        
        pattern = r"/(?:product|produit)/(\d{8,14})"
        match = re.search(pattern, url)
        return match.group(1) if match else None


class WeightExtractor(metaclass=sealed()):
    @staticmethod
    def extract_from_title(title: str) -> Optional[str]:
        if not title:
            return None
        
        pattern = r"(\d[\d\s.,]*\s?(?:g|kg|ml|l))\b"
        match = re.search(pattern, title, flags=re.I)
        return match.group(1).replace(" ", "") if match else None

class NutriScoreExtractor(metaclass=sealed(['_soup', '_extraction_strategies'])):
    def __init__(self, soup: BeautifulSoup):
        object.__setattr__(self, '_soup', soup)
        strategies = {
            'text_content': self._extract_from_text,
            'css_classes': self._extract_from_classes,
            'attributes': self._extract_from_attributes
        }
        object.__setattr__(self, '_extraction_strategies', strategies)
    
    @property
    def soup(self) -> BeautifulSoup:
        return self._soup
    
    def __call__(self, **extraction_kwargs) -> Optional[str]:
        h4_element = self.soup.find("h4", string=lambda s: s and "Nutri-Score" in s)
        if not h4_element:
            return None
        
        strategies_to_use = {
            name: strategy for name, strategy in self._extraction_strategies.items()
            if extraction_kwargs.get(f'use_{name}', True)
        }
        
        for _, strategy in strategies_to_use.items():
            result = strategy(h4_element)
            if result:
                return result
        
        return None
    
    def extract(self) -> Optional[str]:
        return self()
    
    def _extract_from_text(self, element) -> Optional[str]:
        text_match = re.search(r"Nutri-Score\s*([A-E])", element.get_text(), re.I)
        return text_match.group(1).upper() if text_match else None
    
    def _extract_from_classes(self, element) -> Optional[str]:
        if not element.has_attr("class"):
            return None
        
        class_text = " ".join(element["class"]).lower()
        class_match = re.search(r"grade_([a-e])_title", class_text)

        return class_match.group(1).upper() if class_match else None
    
    def _extract_from_attributes(self, element) -> Optional[str]:
        for attr in ['data-grade', 'data-nutri-score', 'data-score']:
            if element.has_attr(attr):
                value = element[attr]
                if isinstance(value, str) and len(value) == 1 and value.upper() in 'ABCDE':
                    return value.upper()
        return None


class OFFProductParser(metaclass=sealed(['_extraction_pipeline'])):
    def __init__(self):
        pipeline = {
            'basic_info': self._extract_basic_info,
            'geographic_info': self._extract_geographic_info,
            'nutrition_scoring': self._extract_nutrition_scoring,
            'media_pricing': self._extract_media_pricing
        }
        object.__setattr__(self, '_extraction_pipeline', pipeline)
    
    def __call__(self, html: str, url: str, **pipeline_kwargs) -> Product:
        soup = BeautifulSoup(html, "html.parser")
        
        extractors = {
            'html': HTMLElementExtractor(soup),
            'jsonld': JSONLDExtractor(soup),
            'nutrition': NutritionTableParser(soup),
            'nutri_score': NutriScoreExtractor(soup)
        }
        
        structured_data = extractors['jsonld'].extract_product_data() or {}
        
        product_data = {'source': url}
        
        for stage_name, stage_func in self._extraction_pipeline.items():
            if pipeline_kwargs.get(f'extract_{stage_name}', True):
                stage_data = stage_func(soup, extractors, structured_data, url)
                product_data.update(stage_data)
        
        product_data['index'] = product_data.get('code_barres') or url
        
        return Product(**product_data)
    
    @staticmethod
    def parse(html: str, url: str) -> Product:
        parser = OFFProductParser()
        return parser(html, url)
    
    def _extract_basic_info(self, soup, extractors, structured_data, url) -> Dict:
        result = {}
        
        title_element = soup.select_one("h1[property='food:name'][itemprop='name']")
        result['nom'] = structured_data.get("nom") or extractors['html'].extract_text(title_element)
        
        generic_element = soup.select_one("#field_generic_name_value [itemprop='description']")
        result['nom_générique'] = extractors['html'].extract_text(generic_element)
        
        barcode_element = soup.select_one("span#barcode[itemprop='gtin13']")
        result['code_barres'] = (
            structured_data.get("code_barres") or 
            (barcode_element and barcode_element.get_text(strip=True)) or
            BarcodeExtractor.extract_from_url(url)
        )
        
        quantity_element = soup.select_one("#field_quantity_value")
        result['poids_unité'] = (
            extractors['html'].extract_text(quantity_element) or
            WeightExtractor.extract_from_title(result['nom'])
        )
        
        packaging_element = soup.select_one("#field_packaging_value")
        result['emballage'] = extractors['html'].extract_text(packaging_element)
        
        brand_element = soup.select_one("#field_brands_value")
        result['marque'] = structured_data.get("marque") or extractors['html'].extract_text(brand_element)
        result['brands_tags'] = (
            [anchor.get_text(strip=True) for anchor in brand_element.select("a")]
            if brand_element else None
        )
        
        category_element = soup.select_one("#field_categories_value")
        result['categories'] = (
            [anchor.get_text(strip=True) for anchor in category_element.select("a")]
            if category_element else None
        )
        
        return result
    
    def _extract_geographic_info(self, soup, extractors, structured_data, url) -> Dict:
        geographic_fields = {
            'pays_origine': '#field_origins_value',
            'fabricant': '#field_manufacturing_places_value',
            'distributeur': '#field_stores_value',
            'pays_commercialisation': '#field_countries_value'
        }
        
        result = {}
        for field_name, selector in geographic_fields.items():
            data_list = extractors['html'].extract_anchor_texts(selector)
            result[field_name] = ", ".join(data_list) if data_list else None
        
        return result
    
    def _extract_nutrition_scoring(self, soup, extractors, structured_data, url) -> Dict:
        return {
            'nutriments': extractors['nutrition'](),
            'score_nutritionnel': extractors['nutri_score'](),
            'date_peremption': None
        }
    
    def _extract_media_pricing(self, soup, extractors, structured_data, url) -> Dict:
        image_selectors = [
            ("meta[name='x:image']", "content"),
            ("meta[property='og:image']", "content")
        ]
        
        image_url = structured_data.get("image")
        if not image_url:
            for selector, attr in image_selectors:
                element = soup.select_one(selector)
                if element and element.get(attr):
                    image_url = element.get(attr)
                    break
        
        return {
            'image': image_url,
            'prix': structured_data.get("prix")
        }


def looks_like_product_page(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    validator = ProductPageValidator(soup)

    return validator()


def parse_off_product_html(html: str, url: str) -> Dict:
    parser = OFFProductParser()
    product = parser(html, url)

    return product.to_dict()