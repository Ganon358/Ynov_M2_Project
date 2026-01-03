import argparse
import os
import sys
import re
import requests
from typing import List, Optional
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential

from adapters.off_html import parse_off_product_html, looks_like_product_page
from utils import polite_sleep, ensure_outdir, write_jsonl, write_csv, now_stamp
from core.sealed import sealed

@dataclass(frozen=True, slots=True)
class ScrapingConfig:
    input_file: str
    output_format: str = "jsonl"
    delay: float = 1.0
    user_agent: str = "Zineb-ETL-Scraper/2.0 (+contact@ynov.example)"
    timeout: int = 30
    max_retries: int = 3
    
    def __post_init__(self):
        if self.delay < 0:
            raise ValueError("Delay must be non-negative")
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if self.max_retries < 1:
            raise ValueError("Max retries must be at least 1")

class URLProcessor(metaclass=sealed(['_base_headers'])):
    def __init__(self, user_agent: str):
        headers = {
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "fr,fr-FR;q=0.9,en;q=0.8",
            "User-Agent": user_agent
        }
        object.__setattr__(self, '_base_headers', headers)
    
    @property
    def headers(self) -> dict:
        return self._base_headers
    
    def to_world_url(self, item: str) -> str:
        return item if item.startswith("http") else f"https://world.openfoodfacts.org/product/{item}"
    
    def to_french_url(self, item: str) -> str:
        if item.startswith("http"):
            barcode_match = re.search(r"/product/(\d{8,14})", item) or re.search(r"/produit/(\d{8,14})", item)
            barcode = barcode_match.group(1) if barcode_match else item
            return f"https://fr.openfoodfacts.org/produit/{barcode}"
        return f"https://fr.openfoodfacts.org/produit/{item}"
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
    def fetch_html(self, url: str, timeout: int = 30) -> tuple[str, str]:
        response = requests.get(url, headers=self.headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type:
            raise RuntimeError(f"Unexpected content-type: {content_type}")
        
        return response.text, response.url


class ScrapingSession(metaclass=sealed(['_config', '_processor', '_results'])):
    def __init__(self, config: ScrapingConfig):
        object.__setattr__(self, '_config', config)
        object.__setattr__(self, '_processor', URLProcessor(config.user_agent))
        object.__setattr__(self, '_results', [])
    
    @property
    def config(self) -> ScrapingConfig:
        return self._config
    
    @property
    def processor(self) -> URLProcessor:
        return self._processor
    
    @property
    def results(self) -> List[dict]:
        return self._results
    
    def load_input_items(self) -> List[str]:
        if not os.path.isfile(self.config.input_file):
            raise FileNotFoundError(f"Input file not found: {self.config.input_file}")
        
        with open(self.config.input_file, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    
    def process_item(self, item: str, index: int, total: int) -> Optional[dict]:
        world_url = self.processor.to_world_url(item)
        
        try:
            html, final_world_url = self.processor.fetch_html(world_url, self.config.timeout)
            
            if not looks_like_product_page(html):
                french_url = self.processor.to_french_url(final_world_url)
                html, final_url = self.processor.fetch_html(french_url, self.config.timeout)
            else:
                final_url = final_world_url
            
            product_data = parse_off_product_html(html, final_url)
            print(f"[{index}/{total}] SUCCESS {final_url}")
            return product_data
            
        except Exception as error:
            print(f"[{index}/{total}] FAILED {world_url} -> {error}", file=sys.stderr)
            return None
    
    def run(self) -> None:
        items = self.load_input_items()
        
        for index, item in enumerate(items, start=1):
            result = self.process_item(item, index, len(items))
            if result:
                self._results.append(result)
            polite_sleep(self.config.delay)
    
    def save_results(self) -> str:
        ensure_outdir()
        timestamp = now_stamp()
        output_path = f"out/products_{timestamp}.{self.config.output_format}"
        
        if self.config.output_format == "jsonl":
            write_jsonl(self.results, output_path)
        else:
            field_order = [
                "nom", "nom_générique", "brands_tags", "marque", "prix", "image",
                "score_nutritionnel", "nutriments", "poids_unité", "pays_origine",
                "fabricant", "pays_commercialisation", "distributeur", "code_barres",
                "date_peremption", "source", "index"
            ]
            write_csv(self.results, output_path, field_order=field_order)
        
        return output_path

class ArgumentParser(metaclass=sealed()):
    @staticmethod
    def parse() -> ScrapingConfig:
        parser = argparse.ArgumentParser(description="Advanced OpenFoodFacts scraper")
        
        parser.add_argument("--input", required=True, 
                          help="File with barcodes or URLs (one per line)")
        parser.add_argument("--format", choices=["jsonl", "csv"], default="jsonl",
                          help="Output format")
        parser.add_argument("--delay", type=float, default=1.0,
                          help="Delay between requests in seconds")
        parser.add_argument("--user-agent", 
                          default="Zineb-ETL-Scraper/2.0 (+contact@ynov.example)",
                          help="User agent string")
        parser.add_argument("--timeout", type=int, default=30,
                          help="Request timeout in seconds")
        parser.add_argument("--max-retries", type=int, default=3,
                          help="Maximum number of retries per request")
        
        args = parser.parse_args()
        
        return ScrapingConfig(
            input_file=args.input,
            output_format=args.format,
            delay=args.delay,
            user_agent=args.user_agent,
            timeout=args.timeout,
            max_retries=args.max_retries
        )


def main() -> None:
    try:
        config = ArgumentParser.parse()
        session = ScrapingSession(config)
        
        session.run()
        
        output_path = session.save_results()
        print(f"[COMPLETED] Processed {len(session.results)} products -> {output_path}")
        
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Scraping stopped by user")
        sys.exit(1)
    except Exception as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()