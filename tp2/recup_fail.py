import json
from pathlib import Path
from colorama import Fore, Style, init
from recup_item import OpenFoodFactsFetcher, display_header, display_stats
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

init(autoreset=True)

def get_missing_categories(min_products: int = 1):
    """Identifie les catégories non récupérées ou vides"""
    
    with open('data/categories.json', 'r', encoding='utf-8') as f:
        all_categories = json.load(f)
    
    products_dir = Path('data/products')
    existing_files = {f.stem: f for f in products_dir.glob('*.json')}
    
    missing = []
    for cat in all_categories:
        expected_filename = cat['id'].replace('en:', '').replace(':', '_')
        
        file_path = existing_files.get(expected_filename)
        if file_path is None:
            missing.append(cat)
            continue

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, list) or len(data) < min_products:
                missing.append(cat)
        except Exception:
            missing.append(cat)
    
    return missing

def main(max_workers: int = 2):
    display_header()
    
    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Identification des catégories manquantes...\n")
    missing_categories = get_missing_categories(min_products=1)
    
    if not missing_categories:
        print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Toutes les catégories ont été récupérées!\n")
        return
    
    print(f"{Fore.YELLOW}[MISSING]{Style.RESET_ALL} {len(missing_categories)} catégories à récupérer:")
    for i, cat in enumerate(missing_categories[:10], 1):
        print(f"  {i}. {cat['name']} ({cat['products_count']} produits)")
    if len(missing_categories) > 10:
        print(f"  ... et {len(missing_categories) - 10} autres\n")
    
    confirm = input(f"\n{Fore.CYAN}Lancer la récupération? (y/n):{Style.RESET_ALL} ")
    if confirm.lower() != 'y':
        print(f"{Fore.RED}[CANCELLED]{Style.RESET_ALL} Annulé par l'utilisateur\n")
        return
    
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} Démarrage avec {max_workers} workers...\n")
    
    start_time = time.time()
    fetcher = OpenFoodFactsFetcher()
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetcher.process_category, cat, i+1, len(missing_categories)): cat 
            for i, cat in enumerate(missing_categories)
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    
    duration = time.time() - start_time
    display_stats(results, duration)
    print(f"{Fore.GREEN}[DONE]{Style.RESET_ALL} Récupération des catégories manquantes terminée\n")

if __name__ == "__main__":
    import sys
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    main(max_workers=workers)