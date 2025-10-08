import requests
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import time

def get_all_breweries(page_size: int = 50) -> List[Dict]:
    """Récupère toutes les brasseries depuis l'API OpenBreweryDB"""
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    breweries = []
    page = 1
    
    while True:
        try:
            # Construire l'URL avec pagination
            url = f"{base_url}?page={page}&per_page={page_size}"
            
            # Faire la requête
            response = requests.get(url)
            response.raise_for_status()
            
            # Récupérer les données
            data = response.json()
            
            # Si plus de données, sortir de la boucle
            if not data:
                break
                
            breweries.extend(data)
            print(f"Page {page} récupérée - {len(data)} brasseries")
            
            # Passer à la page suivante
            page += 1
            
            # Pause polie entre les requêtes
            time.sleep(1)
            
        except Exception as e:
            print(f"Erreur lors de la requête API page {page}: {e}")
            break
    
    return breweries

def save_breweries_data(breweries: List[Dict]) -> str:
    """Sauvegarde les données des brasseries dans un fichier JSON"""
    # Créer le dossier data s'il n'existe pas
    output_dir = Path("data/breweries")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Générer un nom de fichier unique avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"all_breweries_{timestamp}.json"
    filepath = output_dir / filename
    
    # Sauvegarder les données
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

def main():
    print("Récupération de toutes les brasseries...")
    breweries = get_all_breweries()
    
    if breweries:
        # Sauvegarder les données
        filepath = save_breweries_data(breweries)
        print(f"\nRécupération terminée!")
        print(f"Nombre total de brasseries: {len(breweries)}")
        print(f"Données sauvegardées dans: {filepath}")
    else:
        print("Échec de la récupération des données")

if __name__ == "__main__":
    main()