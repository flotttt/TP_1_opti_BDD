import requests
import json
import time
from typing import List, Dict, Optional
from pathlib import Path
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

init(autoreset=True)

class OpenFoodFactsFetcher:
    BASE_URL = "https://world.openfoodfacts.org"
    FIELDS = "code,product_name,brands,image_url,nutriscore_grade,nova_group,nutriments,categories_tags"
    
    def __init__(self, output_dir: str = "data/products"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.lock = Lock()
    
    def fetch_products_by_category(
        self, 
        category_id: str, 
        max_products: int = 500,
        page_size: int = 100
    ) -> List[Dict]:
        """Récupère les produits d'une catégorie"""
        
        all_products = []
        page = 1
        
        self._print(f"{Fore.CYAN}[FETCH]{Style.RESET_ALL} Catégorie: {Fore.YELLOW}{category_id}{Style.RESET_ALL}")
        
        while len(all_products) < max_products:
            params = {
                'page': str(page),
                'page_size': str(page_size),
                'fields': self.FIELDS
            }
            
            url = f"{self.BASE_URL}/category/{category_id}.json"
            
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                products = data.get('products', [])
                
                if not products:
                    break
                
                all_products.extend(products)
                self._print(f"{Fore.WHITE}  [{category_id}] Page {page:2d}{Style.RESET_ALL} | {Fore.GREEN}{len(products)} produits{Style.RESET_ALL} | Total: {Fore.CYAN}{len(all_products)}{Style.RESET_ALL}")
                
                page += 1
                time.sleep(0.3)
                
            except Exception as e:
                self._print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} [{category_id}] Page {page}: {e}")
                break
        
        return all_products[:max_products]
    
    def clean_product(self, product: Dict) -> Optional[Dict]:
        """Nettoie et normalise les données produit"""
        
        if not product.get('code') or not product.get('product_name'):
            return None
        
        nutriments = product.get('nutriments', {})
        
        return {
            'id': product['code'],
            'name': product.get('product_name', '').strip(),
            'brand': product.get('brands', '').strip(),
            'image': product.get('image_url'),
            'nutriscore': product.get('nutriscore_grade', '').upper() or None,
            'nova_group': product.get('nova_group'),
            'categories': product.get('categories_tags', []),
            'nutrition': {
                'energy_100g': nutriments.get('energy_100g'),
                'proteins_100g': nutriments.get('proteins_100g'),
                'carbohydrates_100g': nutriments.get('carbohydrates_100g'),
                'sugars_100g': nutriments.get('sugars_100g'),
                'fat_100g': nutriments.get('fat_100g'),
                'saturated_fat_100g': nutriments.get('saturated-fat_100g'),
                'fiber_100g': nutriments.get('fiber_100g'),
                'salt_100g': nutriments.get('salt_100g')
            }
        }
    
    def save_products(self, products: List[Dict], category_id: str):
        """Sauvegarde les produits dans un fichier JSON"""
        
        cleaned = [self.clean_product(p) for p in products]
        cleaned = [p for p in cleaned if p is not None]
        
        filename = category_id.replace('en:', '').replace(':', '_')
        filepath = self.output_dir / f"{filename}.json"
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=2)
        
        self._print(f"{Fore.GREEN}[SAVED]{Style.RESET_ALL} {len(cleaned)} produits -> {Fore.YELLOW}{filepath.name}{Style.RESET_ALL}")
        
        return len(cleaned)
    
    def _print(self, message: str):
        """Print thread-safe"""
        with self.lock:
            print(message)
    
    def process_category(self, cat: Dict, index: int, total: int) -> Dict:
        """Traite une catégorie (fetch + save)"""
        category_id = cat['id']
        
        self._print(f"\n{Fore.WHITE}[{index}/{total}]{Style.RESET_ALL} Démarrage: {Fore.YELLOW}{cat['name']}{Style.RESET_ALL}")
        
        try:
            products = self.fetch_products_by_category(category_id, max_products=500)
            count = self.save_products(products, category_id)
            
            return {
                'category': cat['name'],
                'count': count,
                'success': True
            }
        except Exception as e:
            self._print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} [{category_id}]: {e}")
            return {
                'category': cat['name'],
                'count': 0,
                'success': False,
                'error': str(e)
            }

def display_header():
    print(f"\n{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}OpenFoodFacts - Récupération des produits (Parallélisé){Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}\n")

def display_stats(results: List[Dict], duration: float):
    print(f"\n{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Statistiques{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}\n")
    
    total_products = sum(r['count'] for r in results)
    success = sum(1 for r in results if r['success'])
    failed = sum(1 for r in results if not r['success'])
    
    print(f"{Fore.CYAN}Catégories traitées:{Style.RESET_ALL}  {Fore.GREEN}{success}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Catégories échouées:{Style.RESET_ALL} {Fore.RED}{failed}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Produits récupérés:{Style.RESET_ALL}  {Fore.GREEN}{total_products}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Durée totale:{Style.RESET_ALL}        {Fore.YELLOW}{duration:.2f}s{Style.RESET_ALL}\n")
    
    if failed > 0:
        print(f"{Fore.RED}Catégories échouées:{Style.RESET_ALL}")
        for r in results:
            if not r['success']:
                print(f"  - {r['category']}: {r.get('error', 'Unknown error')}")

def main(max_workers: int = 5):
    display_header()
    
    start_time = time.time()
    
    with open('data/categories.json', 'r', encoding='utf-8') as f:
        categories = json.load(f)
    
    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Traitement de {Fore.YELLOW}{len(categories)}{Style.RESET_ALL} catégories avec {Fore.YELLOW}{max_workers}{Style.RESET_ALL} workers\n")
    
    fetcher = OpenFoodFactsFetcher()
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetcher.process_category, cat, i+1, len(categories)): cat 
            for i, cat in enumerate(categories)
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    
    duration = time.time() - start_time
    display_stats(results, duration)
    print(f"{Fore.GREEN}[DONE]{Style.RESET_ALL} Processus terminé\n")

if __name__ == "__main__":
    import sys
    
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    main(max_workers=workers)