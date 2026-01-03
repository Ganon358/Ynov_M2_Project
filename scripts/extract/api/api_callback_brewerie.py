import requests
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import time
from api_callback import ApiCallBack, ApiType
from itertools import count

class BreweryApiHandler(ApiCallBack):
    __slots__ = ('_base_url', '_page_size', '_delay')
    
    def __init__(self, page_size: int = 50, delay: float = 1.0) -> None:
        config = {
            'url': 'https://api.openbrewerydb.org/v1/breweries',
            'api_key': '',
            'type_of_api': ApiType.DRINKING,
            'count_of_pages': float('inf')
        }
        super().__init__(config)
        
        self._base_url = config['url']
        self._page_size = page_size
        self._delay = delay
    
    def _fetch_page(self, page: int) -> List[Dict]:
        url = f"{self._base_url}?page={page}&per_page={self._page_size}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data:
            raise StopIteration
        
        time.sleep(self._delay)
        return data
    
    def _page_generator(self) -> List[Dict]: # type: ignore
        for page in count(1):
            try:
                data = self._fetch_page(page)
                print(f"Page {page} - {len(data)} breweries")
                yield data
            except (StopIteration, Exception):
                return
    
    def fetch_all_breweries(self) -> List[Dict]:
        return [brewery for page_data in self._page_generator() for brewery in page_data]
    
    def save_breweries(self, breweries: List[Dict]) -> str:
        output_dir = Path("data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"../../../" / output_dir / f"all_breweries_{timestamp}.json"
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump({
                "metadata": {
                    "total_count": len(breweries),
                    "timestamp": timestamp,
                    "source": "OpenBreweryDB API"
                },
                "breweries": breweries
            }, f, indent=2, ensure_ascii=False)
        
        return str(filepath)

def main() -> None:
    handler = BreweryApiHandler()
    breweries = handler.fetch_all_breweries()
    
    if breweries:
        filepath = handler.save_breweries(breweries)
        print(f"\nTotal: {len(breweries)} breweries\nSaved: {filepath}")

if __name__ == "__main__":
    main()