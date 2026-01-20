import json
import os
import time
from pathlib import Path
from typing import Dict, Optional, List

import psycopg2
from colorama import Fore, Style, init
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from clean_data import clean_product

init(autoreset=True)
load_dotenv()

def connect_to_postgres():
    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Connexion à PostgreSQL...")

    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    database = os.getenv("POSTGRES_DB", "mydb")
    port = os.getenv("POSTGRES_PORT", "5432")

    conn = psycopg2.connect(
        host="localhost",
        database=database,
        user=user,
        password=password,
        port=port,
    )
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Connecté à PostgreSQL")
    return conn

def create_postgres_schema(conn):
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} Création du schéma PostgreSQL...")
    
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS nutrition_facts CASCADE")
        cur.execute("DROP TABLE IF EXISTS product_categories CASCADE")
        cur.execute("DROP TABLE IF EXISTS products CASCADE")
        cur.execute("DROP TABLE IF EXISTS categories CASCADE")
        cur.execute("DROP TABLE IF EXISTS brands CASCADE")
        
        cur.execute("""
            CREATE TABLE brands (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE products (
                product_id VARCHAR(50) PRIMARY KEY,
                name TEXT NOT NULL,
                brand_id INTEGER REFERENCES brands(id),
                main_category_id INTEGER REFERENCES categories(id),
                nutriscore VARCHAR(20),
                nova_group INTEGER,
                health_score INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        cur.execute("""
            CREATE TABLE nutrition_facts (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE CASCADE,
                energy_kcal NUMERIC(10,3),
                energy_kj NUMERIC(10,3),
                proteins NUMERIC(10,3),
                carbohydrates NUMERIC(10,3),
                sugars NUMERIC(10,3),
                fat NUMERIC(10,3),
                saturated_fat NUMERIC(10,3),
                fiber NUMERIC(10,3),
                salt NUMERIC(10,3)
            )
        """)
        
        cur.execute("""
            CREATE TABLE product_categories (
                product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE CASCADE,
                category_id INTEGER REFERENCES categories(id),
                PRIMARY KEY (product_id, category_id)
            )
        """)
        
        conn.commit()
    
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Schéma créé")

def create_postgres_indexes(conn):
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} Création des index PostgreSQL...")
    
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX idx_products_brand ON products(brand_id)")
        cur.execute("CREATE INDEX idx_products_category ON products(main_category_id)")
        cur.execute("CREATE INDEX idx_products_nutriscore ON products(nutriscore)")
        cur.execute("CREATE INDEX idx_products_nova ON products(nova_group)")
        cur.execute("CREATE INDEX idx_products_health_score ON products(health_score)")
        cur.execute("CREATE INDEX idx_products_created_at ON products(created_at)")
        cur.execute("CREATE INDEX idx_products_name ON products USING gin(to_tsvector('english', name))")
        
        cur.execute("CREATE INDEX idx_nutrition_product ON nutrition_facts(product_id)")
        cur.execute("CREATE INDEX idx_nutrition_energy ON nutrition_facts(energy_kcal)")
        cur.execute("CREATE INDEX idx_nutrition_proteins ON nutrition_facts(proteins)")
        cur.execute("CREATE INDEX idx_nutrition_sugars ON nutrition_facts(sugars)")
        cur.execute("CREATE INDEX idx_nutrition_fat ON nutrition_facts(fat)")
        cur.execute("CREATE INDEX idx_nutrition_salt ON nutrition_facts(salt)")
        
        cur.execute("CREATE INDEX idx_product_categories_product ON product_categories(product_id)")
        cur.execute("CREATE INDEX idx_product_categories_category ON product_categories(category_id)")
        
        conn.commit()
    
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Index créés")

def import_to_postgres(products: List[Dict], conn):
    def _clamp_numeric(value: Optional[float], max_abs: float = 1_000_000.0) -> float:
        """Sécurise les valeurs numériques pour éviter les overflows NUMERIC."""
        if value is None:
            return 0.0
        try:
            v = float(value)
        except (TypeError, ValueError):
            return 0.0
        if v > max_abs:
            return max_abs
        if v < -max_abs:
            return -max_abs
        return v
    with conn.cursor() as cur:
        brands_map = {}
        categories_map = {}
        
        brands = {p['brand'] for p in products}
        for brand in brands:
            cur.execute(
                "INSERT INTO brands (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                (brand,)
            )
            brands_map[brand] = cur.fetchone()[0]
        
        all_categories = set()
        for p in products:
            all_categories.add(p['main_category'])
            all_categories.update(p['categories'])
        
        for cat in all_categories:
            cur.execute(
                "INSERT INTO categories (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                (cat,)
            )
            categories_map[cat] = cur.fetchone()[0]
        
        products_data = [
            (p['product_id'], p['name'], brands_map[p['brand']], 
             categories_map[p['main_category']], p['nutriscore'], 
             p['nova_group'], p['health_score'])
            for p in products
        ]
        
        execute_batch(cur, """
            INSERT INTO products (product_id, name, brand_id, main_category_id, nutriscore, nova_group, health_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        """, products_data, page_size=1000)
        
        nutrition_data = []
        for p in products:
            nutrition = p.get("nutrition_per_100g", {})
            nutrition_data.append(
                (
                    p["product_id"],
                    _clamp_numeric(nutrition.get("energy_kcal")),
                    _clamp_numeric(nutrition.get("energy_kj")),
                    _clamp_numeric(nutrition.get("proteins")),
                    _clamp_numeric(nutrition.get("carbohydrates")),
                    _clamp_numeric(nutrition.get("sugars")),
                    _clamp_numeric(nutrition.get("fat")),
                    _clamp_numeric(nutrition.get("saturated_fat")),
                    _clamp_numeric(nutrition.get("fiber")),
                    _clamp_numeric(nutrition.get("salt")),
                )
            )
        
        execute_batch(cur, """
            INSERT INTO nutrition_facts (product_id, energy_kcal, energy_kj, proteins, 
                                        carbohydrates, sugars, fat, saturated_fat, fiber, salt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, nutrition_data, page_size=1000)
        
        product_categories_data = []
        for p in products:
            for cat in p['categories']:
                if cat in categories_map:
                    product_categories_data.append((p['product_id'], categories_map[cat]))
        
        if product_categories_data:
            execute_batch(cur, """
                INSERT INTO product_categories (product_id, category_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, product_categories_data, page_size=1000)
        
        conn.commit()

def load_products_from_json(products_dir: str = "data/products"):
    products_path = Path(products_dir)
    json_files = list(products_path.glob("*.json"))
    
    all_products = []
    
    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            products = json.load(f)
            for p in products:
                cleaned = clean_product(p)
                if cleaned:
                    all_products.append(cleaned)
    
    return all_products

def get_database_stats(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                pg_size_pretty(pg_database_size(current_database())) as db_size
        """)
        db_size = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM products")
        products_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM brands")
        brands_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM categories")
        categories_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT nutriscore) FROM products WHERE nutriscore IS NOT NULL")
        nutriscore_count = cur.fetchone()[0]
    
    return {
        'db_size': db_size,
        'products': products_count,
        'brands': brands_count,
        'categories': categories_count,
        'nutriscore_grades': nutriscore_count
    }

def main():
    print(f"\n{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Import vers PostgreSQL{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}\n")
    
    conn = connect_to_postgres()
    
    create_postgres_schema(conn)
    
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} Chargement des produits depuis JSON...")
    products = load_products_from_json()
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {len(products)} produits chargés")
    
    print(f"\n{Fore.CYAN}[INFO]{Style.RESET_ALL} Import des données...")
    start_time = time.time()
    
    batch_size = 5000
    for i in range(0, len(products), batch_size):
        batch = products[i:i+batch_size]
        import_to_postgres(batch, conn)
        print(f"{Fore.WHITE}[PROGRESS]{Style.RESET_ALL} {min(i+batch_size, len(products))}/{len(products)} produits importés")
    
    duration = time.time() - start_time
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Import terminé en {duration:.2f}s")
    
    create_postgres_indexes(conn)
    
    print(f"\n{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Statistiques PostgreSQL{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*70}{Style.RESET_ALL}\n")
    
    stats = get_database_stats(conn)
    print(f"{Fore.CYAN}Taille base:{Style.RESET_ALL}        {stats['db_size']}")
    print(f"{Fore.CYAN}Produits:{Style.RESET_ALL}           {stats['products']}")
    print(f"{Fore.CYAN}Marques:{Style.RESET_ALL}            {stats['brands']}")
    print(f"{Fore.CYAN}Catégories:{Style.RESET_ALL}         {stats['categories']}")
    print(f"{Fore.CYAN}Nutriscore grades:{Style.RESET_ALL}  {stats['nutriscore_grades']}")
    print(f"{Fore.CYAN}Temps d'import:{Style.RESET_ALL}     {duration:.2f}s\n")
    
    conn.close()
    print(f"{Fore.GREEN}[DONE]{Style.RESET_ALL} Processus terminé\n")

if __name__ == "__main__":
    main()