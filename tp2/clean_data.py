import json
import os
import time
from pathlib import Path
from typing import Dict, Optional

from colorama import Fore, Style, init
from dotenv import load_dotenv
from pymongo import MongoClient

init(autoreset=True)
load_dotenv()

def clean_product(product: Dict) -> Optional[Dict]:
    """Nettoie et normalise un produit"""
    
    if not product.get('id') or not product.get('name'):
        return None
    
    categories = [
        cat.replace('en:', '').replace('-', ' ').title()
        for cat in product.get('categories', [])
    ]
    
    main_category = categories[0] if categories else "Uncategorized"
    
    nutrition = product.get('nutrition', {})
    cleaned_nutrition = {
        'energy_100g': nutrition.get('energy_100g', 0) or 0,
        'proteins_100g': nutrition.get('proteins_100g', 0) or 0,
        'carbohydrates_100g': nutrition.get('carbohydrates_100g', 0) or 0,
        'sugars_100g': nutrition.get('sugars_100g', 0) or 0,
        'fat_100g': nutrition.get('fat_100g', 0) or 0,
        'saturated_fat_100g': nutrition.get('saturated_fat_100g', 0) or 0,
        'fiber_100g': nutrition.get('fiber_100g', 0) or 0,
        'salt_100g': nutrition.get('salt_100g', 0) or 0,
        'energy_kcal': round(nutrition.get('energy_100g', 0) / 4.184) if nutrition.get('energy_100g') else 0,
        'energy_kj': nutrition.get('energy_100g', 0) or 0,
        'proteins': nutrition.get('proteins_100g', 0) or 0,
        'carbohydrates': nutrition.get('carbohydrates_100g', 0) or 0,
        'sugars': nutrition.get('sugars_100g', 0) or 0,
        'fat': nutrition.get('fat_100g', 0) or 0,
        'saturated_fat': nutrition.get('saturated_fat_100g', 0) or 0,
        'fiber': nutrition.get('fiber_100g', 0) or 0,
        'salt': nutrition.get('salt_100g', 0) or 0
    }
    
    health_score = calculate_health_score(cleaned_nutrition, product.get('nutriscore'))
    
    return {
        'product_id': product['id'],
        'name': product['name'].strip(),
        'brand': product.get('brand', '').strip() or "Unknown",
        'nutriscore': product.get('nutriscore'),
        'nova_group': product.get('nova_group'),
        'main_category': main_category,
        'categories': categories[:5],
        'nutrition_per_100g': cleaned_nutrition,
        'health_score': health_score
    }

def calculate_health_score(nutrition: Dict, nutriscore: str) -> int:
    """Calcule un score santé de 0 à 100"""
    score = 50
    
    if nutrition['proteins'] > 10:
        score += 15
    elif nutrition['proteins'] > 5:
        score += 10
    
    if nutrition['sugars'] > 20:
        score -= 20
    elif nutrition['sugars'] > 10:
        score -= 10
    
    if nutrition['saturated_fat'] > 10:
        score -= 15
    elif nutrition['saturated_fat'] > 5:
        score -= 8
    
    if nutrition['salt'] > 2:
        score -= 15
    elif nutrition['salt'] > 1:
        score -= 8
    
    if nutrition['fiber'] > 5:
        score += 10
    
    nutriscore_bonus = {
        'A': 20,
        'B': 10,
        'C': 0,
        'D': -10,
        'E': -20
    }
    score += nutriscore_bonus.get(nutriscore, 0)
    
    return max(0, min(100, score))

def connect_to_mongodb():
    """Connexion à MongoDB (avec ou sans authentification)"""
    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Connexion à MongoDB...")

    username = os.getenv("MONGO_ROOT_USERNAME")
    password = os.getenv("MONGO_ROOT_PASSWORD")
    port = os.getenv("MONGO_PORT", "27017")
    database = os.getenv("MONGO_DATABASE", "openfoodfacts")

    if username and password:
        uri = f"mongodb://{username}:{password}@localhost:{port}/?authSource=admin"
    else:
        uri = f"mongodb://localhost:{port}/"

    client = MongoClient(uri)
    db = client[database]
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Connecté à la base '{database}'")
    return db

def import_products(db, products_dir: str = "data/products"):
    """Import et nettoyage des produits"""
    products_path = Path(products_dir)
    json_files = list(products_path.glob("*.json"))
    
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} {len(json_files)} fichiers à traiter\n")
    
    collection = db['products']
    
    collection.drop()
    print(f"{Fore.YELLOW}[INFO]{Style.RESET_ALL} Collection 'products' réinitialisée")
    
    total_imported = 0
    total_skipped = 0
    
    start_time = time.time()
    
    for i, file in enumerate(json_files, 1):
        category_name = file.stem
        
        with open(file, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        if not products:
            continue
        
        cleaned = []
        for p in products:
            clean = clean_product(p)
            if clean:
                cleaned.append(clean)
            else:
                total_skipped += 1
        
        if cleaned:
            try:
                result = collection.insert_many(cleaned, ordered=False)
                count = len(result.inserted_ids)
                total_imported += count
                print(f"{Fore.WHITE}[{i}/{len(json_files)}]{Style.RESET_ALL} {Fore.GREEN}{category_name:<30}{Style.RESET_ALL} {count:>4} produits")
            except Exception as e:
                print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} {category_name}: {str(e)[:50]}")
    
    duration = time.time() - start_time
    
    print(f"\n{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {total_imported} produits importés en {duration:.2f}s")
    print(f"{Fore.YELLOW}[INFO]{Style.RESET_ALL} {total_skipped} produits ignorés (données manquantes)")
    
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} Création des index...")
    try:
        collection.create_index("product_id")
    except Exception as e:
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} Index product_id: {str(e)[:80]}")

    collection.create_index("name")
    collection.create_index("brand")
    collection.create_index("nutriscore")
    collection.create_index("main_category")
    collection.create_index("health_score")
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Index créés")
    
    print(f"\n{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Statistiques MongoDB{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}\n")
    
    db_stats = db.command("dbStats")
    db_size_mb = db_stats['dataSize'] / (1024 * 1024)
    
    print(f"{Fore.CYAN}Total produits:{Style.RESET_ALL}        {collection.count_documents({})}")
    print(f"{Fore.CYAN}Taille base:{Style.RESET_ALL}           {db_size_mb:.2f} MB")
    print(f"{Fore.CYAN}Temps d'import:{Style.RESET_ALL}       {duration:.2f}s")
    print(f"{Fore.CYAN}Nutriscore A:{Style.RESET_ALL}          {collection.count_documents({'nutriscore': 'A'})}")
    print(f"{Fore.CYAN}Nutriscore E:{Style.RESET_ALL}          {collection.count_documents({'nutriscore': 'E'})}")
    print(f"{Fore.CYAN}Nova group 1 (min):{Style.RESET_ALL}    {collection.count_documents({'nova_group': 1})}")
    print(f"{Fore.CYAN}Nova group 4 (max):{Style.RESET_ALL}    {collection.count_documents({'nova_group': 4})}")
    
    pipeline = [
        {"$group": {"_id": "$main_category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_categories = list(collection.aggregate(pipeline))
    
    print(f"\n{Fore.CYAN}Top 5 catégories:{Style.RESET_ALL}")
    for cat in top_categories:
        print(f"  - {cat['_id']}: {cat['count']} produits")

def main():
    print(f"\n{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Nettoyage et Import vers MongoDB{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}\n")
    
    db = connect_to_mongodb()
    import_products(db)
    
    print(f"\n{Fore.GREEN}[DONE]{Style.RESET_ALL} Import terminé\n")

if __name__ == "__main__":
    main()