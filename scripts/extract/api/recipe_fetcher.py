import requests
import json
from typing import Dict, Any, Optional
from pathlib import Path
from api_callback import ApiCallBack, ApiType
import os

class RecipeApiHandler(ApiCallBack):
    __slots__ = ('_base_url', '_recipe_id')
    
    def __init__(self, api_key: str, recipe_id: int = 1957050) -> None:
        config = {
            'url': 'https://api.spoonacular.com/recipes',
            'api_key': api_key,
            'type_of_api': ApiType.RECIPE,
            'count_of_pages': 1
        }
        super().__init__(config)

        self._base_url = config['url']
        self._recipe_id = recipe_id
    
    def _fetch_page(self, page: int = 1) -> Dict[str, Any]:
        endpoint = f"{self._base_url}/{self._recipe_id}/information"
        params = {'apiKey': self.api_key}
        headers = {'accept': 'application/json'}
        
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def fetch_recipe(self, recipe_id: Optional[int] = None) -> Dict[str, Any]:
        if recipe_id:
            self._recipe_id = recipe_id
        return self._fetch_page()
    
    def save_recipe(self, recipe_data: Dict[str, Any], filename: str = None) -> str:
        if not filename:
            filename = f"{recipe_data.get('title', 'recipe').lower().replace(' ', '_')}.json"
        
        output_dir = Path("../../../data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(recipe_data, f, indent=2, ensure_ascii=False)
        
        return str(filepath)
    
    def print_recipe_info(self, recipe_data: Dict[str, Any]) -> None:
        title = recipe_data.get('title', 'Unknown')
        ready_time = recipe_data.get('readyInMinutes', 'Unknown')
        
        print(f"Title: {title}\nReady in: {ready_time} minutes\n")
        
        ingredients = recipe_data.get('extendedIngredients', [])
        if ingredients:
            print("Ingredients:")
            for ingredient in ingredients:
                print(f"- {ingredient.get('original', '')}")
        
        instructions = recipe_data.get('analyzedInstructions', [])
        if instructions:
            print("\nInstructions:")
            for instruction in instructions:
                for step in instruction.get('steps', []):
                    print(f"{step.get('number', '')}. {step.get('step', '')}")

def _load_env_local() -> dict:
    """Charge les variables d'environnement depuis le fichier .env local"""
    # Chercher le fichier .env dans le dossier scripts (3 niveaux au-dessus)
    env_path = Path(__file__).parent.parent.parent / '.env'
    env_vars = {}
    
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value.strip('"').strip("'")
    else:
        print(f"Fichier .env non trouvé à: {env_path}")
    
    return env_vars
                    
def main() -> None:
    env_vars = _load_env_local()
    API_KEY = env_vars.get('API_KEY') or os.getenv('API_KEY')
    
    if not API_KEY:
        raise ValueError("API_KEY not found in .env file or environment variables")
    
    handler = RecipeApiHandler(API_KEY)
    recipe_data = handler.fetch_recipe()
    
    if recipe_data:
        filepath = handler.save_recipe(recipe_data)
        handler.print_recipe_info(recipe_data)
        print(f"\nSaved to: {filepath}")

if __name__ == "__main__":
    main()