import json
import os
import sys
from typing import List, Dict

import requests
from colorama import Fore, Style, init
from requests.exceptions import JSONDecodeError

init(autoreset=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def fetch_categories() -> List[Dict]:
    """Récupère toutes les catégories depuis OpenFoodFacts"""
    url = "https://world.openfoodfacts.org/categories.json"

    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Récupération des catégories...")
    response = requests.get(url, headers=HEADERS, timeout=10)
    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Status code: {response.status_code}")

    try:
        data = response.json()
    except JSONDecodeError:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Réponse non JSON, aperçu :")
        print(response.text[:500])
        sys.exit(1)

    categories = data.get("tags", [])

    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {len(categories)} catégories récupérées")
    return categories

def filter_categories(categories: List[Dict], min_products: int = 100) -> List[Dict]:
    """Filtre les catégories avec au moins X produits"""
    filtered = [
        {
            'id': cat['id'],
            'name': cat['name'],
            'products_count': cat['products']
        }
        for cat in categories 
        if cat.get('products', 0) >= min_products
    ]
    
    filtered.sort(key=lambda x: x['products_count'], reverse=True)
    
    print(f"{Fore.YELLOW}[FILTER]{Style.RESET_ALL} {len(filtered)} catégories avec au moins {min_products} produits")
    
    return filtered

def save_to_json(data: List[Dict], filename: str = "categories.json"):
    """Sauvegarde les données dans un fichier JSON"""
    directory = os.path.dirname(filename)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"{Fore.GREEN}[SAVED]{Style.RESET_ALL} Données sauvegardées dans {filename}")

def display_top_categories(categories: List[Dict], limit: int = 10):
    """Affiche les catégories les plus populaires"""
    print(f"\n{Fore.MAGENTA}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Top {limit} catégories{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*60}{Style.RESET_ALL}\n")
    
    for i, cat in enumerate(categories[:limit], 1):
        count = f"{cat['products_count']:,}".replace(',', ' ')
        print(f"{Fore.WHITE}{i:2d}.{Style.RESET_ALL} {Fore.CYAN}{cat['name']:<40}{Style.RESET_ALL} {Fore.GREEN}{count} produits{Style.RESET_ALL}")

def main():
    print(f"\n{Fore.MAGENTA}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}OpenFoodFacts - Récupération des catégories{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*60}{Style.RESET_ALL}\n")
    
    all_categories = fetch_categories()
    popular_categories = filter_categories(all_categories, min_products=100)
    save_to_json(popular_categories, 'data/categories.json')
    display_top_categories(popular_categories)
    
    print(f"\n{Fore.GREEN}[DONE]{Style.RESET_ALL} Processus terminé\n")

if __name__ == "__main__":
    main()