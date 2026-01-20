import os
import time
from typing import Dict, List

import psycopg2
from colorama import Fore, Style, init
from dotenv import load_dotenv

init(autoreset=True)
load_dotenv()


def connect_to_postgres():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    database = os.getenv("POSTGRES_DB", "mydb")
    port = os.getenv("POSTGRES_PORT", "5432")

    return psycopg2.connect(
        host="localhost",
        database=database,
        user=user,
        password=password,
        port=port,
    )

class PostgresOptimizer:
    def __init__(self, conn):
        self.conn = conn
    
    def create_partitioned_table(self):
        print(f"\n{Fore.CYAN}[1/3]{Style.RESET_ALL} Création de la table partitionnée par date...")
        
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products_partitioned (
                    product_id VARCHAR(50),
                    name TEXT NOT NULL,
                    brand_id INTEGER,
                    main_category_id INTEGER,
                    nutriscore VARCHAR(20),
                    nova_group INTEGER,
                    health_score INTEGER,
                    created_at TIMESTAMP DEFAULT NOW()
                ) PARTITION BY RANGE (created_at)
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products_part_2024 
                PARTITION OF products_partitioned
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products_part_2025 
                PARTITION OF products_partitioned
                FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products_part_2026 
                PARTITION OF products_partitioned
                FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')
            """)
            
            cur.execute("""
                INSERT INTO products_partitioned 
                SELECT * FROM products
                ON CONFLICT DO NOTHING
            """)
            
            cur.execute("CREATE INDEX IF NOT EXISTS idx_part_nutriscore ON products_partitioned(nutriscore)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_part_health_score ON products_partitioned(health_score)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_part_created_at ON products_partitioned(created_at)")
            
            self.conn.commit()
        
        print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Table partitionnée créée")
    
    def create_materialized_views(self):
        print(f"\n{Fore.CYAN}[2/3]{Style.RESET_ALL} Création des vues matérialisées...")
        
        with self.conn.cursor() as cur:
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_products_stats CASCADE")
            cur.execute("""
                CREATE MATERIALIZED VIEW mv_products_stats AS
                SELECT 
                    c.name as category,
                    b.name as brand,
                    p.nutriscore,
                    COUNT(*) as product_count,
                    AVG(nf.proteins) as avg_proteins,
                    AVG(nf.sugars) as avg_sugars,
                    AVG(nf.fat) as avg_fat,
                    AVG(nf.salt) as avg_salt,
                    AVG(p.health_score) as avg_health_score
                FROM products p
                JOIN brands b ON p.brand_id = b.id
                JOIN categories c ON p.main_category_id = c.id
                JOIN nutrition_facts nf ON p.product_id = nf.product_id
                GROUP BY c.name, b.name, p.nutriscore
            """)
            
            cur.execute("CREATE INDEX idx_mv_category ON mv_products_stats(category)")
            cur.execute("CREATE INDEX idx_mv_brand ON mv_products_stats(brand)")
            cur.execute("CREATE INDEX idx_mv_nutriscore ON mv_products_stats(nutriscore)")
            
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_top_healthy_products CASCADE")
            cur.execute("""
                CREATE MATERIALIZED VIEW mv_top_healthy_products AS
                SELECT 
                    p.product_id,
                    p.name,
                    b.name as brand,
                    c.name as category,
                    p.nutriscore,
                    p.health_score,
                    nf.proteins,
                    nf.sugars,
                    nf.fat
                FROM products p
                JOIN brands b ON p.brand_id = b.id
                JOIN categories c ON p.main_category_id = c.id
                JOIN nutrition_facts nf ON p.product_id = nf.product_id
                WHERE p.health_score > 70
                ORDER BY p.health_score DESC
            """)
            
            cur.execute("CREATE INDEX idx_mv_top_health_score ON mv_top_healthy_products(health_score)")
            
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_category_nutrition CASCADE")
            cur.execute("""
                CREATE MATERIALIZED VIEW mv_category_nutrition AS
                SELECT 
                    c.name as category,
                    c.id as category_id,
                    AVG(nf.energy_kcal) as avg_energy_kcal,
                    AVG(nf.proteins) as avg_proteins,
                    AVG(nf.carbohydrates) as avg_carbohydrates,
                    AVG(nf.sugars) as avg_sugars,
                    AVG(nf.fat) as avg_fat,
                    AVG(nf.saturated_fat) as avg_saturated_fat,
                    AVG(nf.fiber) as avg_fiber,
                    AVG(nf.salt) as avg_salt,
                    COUNT(*) as product_count,
                    AVG(p.health_score) as avg_health_score
                FROM products p
                JOIN categories c ON p.main_category_id = c.id
                JOIN nutrition_facts nf ON p.product_id = nf.product_id
                GROUP BY c.name, c.id
                HAVING COUNT(*) > 50
                ORDER BY product_count DESC
            """)
            
            cur.execute("CREATE INDEX idx_mv_cat_nutrition ON mv_category_nutrition(category_id)")
            
            self.conn.commit()
        
        print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Vues matérialisées créées")
    
    def create_additional_indexes(self):
        print(f"\n{Fore.CYAN}[3/3]{Style.RESET_ALL} Création d'index composés supplémentaires...")
        
        with self.conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_nutrition_sugars_fat ON nutrition_facts(sugars, fat)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_nutrition_proteins_desc ON nutrition_facts(proteins DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_products_nutriscore_health ON products(nutriscore, health_score DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_products_nova_nutriscore ON products(nova_group, nutriscore)")
            
            self.conn.commit()
        
        print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Index composés créés")
    
    def benchmark_query(self, name: str, query: str, params=None):
        with self.conn.cursor() as cur:
            start = time.time()
            cur.execute(query, params)
            results = cur.fetchall()
            duration = time.time() - start
            
            return {
                'name': name,
                'duration_ms': duration * 1000,
                'rows': len(results)
            }
    
    def compare_before_after(self):
        print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}Comparaison avant/après optimisations{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}\n")
        
        queries = [
            ("Stats par catégorie (sans MV)", """
                SELECT c.name as category,
                       AVG(nf.proteins) as avg_proteins,
                       AVG(nf.sugars) as avg_sugars,
                       AVG(nf.fat) as avg_fat,
                       COUNT(*) as product_count
                FROM products p
                JOIN categories c ON p.main_category_id = c.id
                JOIN nutrition_facts nf ON p.product_id = nf.product_id
                GROUP BY c.name
                HAVING COUNT(*) > 100
                ORDER BY avg_proteins DESC
                LIMIT 20
            """),
            ("Stats par catégorie (avec MV)", """
                SELECT category, avg_proteins, avg_sugars, avg_fat, 
                       SUM(product_count) as product_count
                FROM mv_products_stats
                GROUP BY category, avg_proteins, avg_sugars, avg_fat
                HAVING SUM(product_count) > 100
                ORDER BY avg_proteins DESC
                LIMIT 20
            """),
            ("Top produits sains (sans MV)", """
                SELECT p.name, b.name as brand, p.health_score
                FROM products p
                JOIN brands b ON p.brand_id = b.id
                WHERE p.health_score > 70
                ORDER BY p.health_score DESC
                LIMIT 100
            """),
            ("Top produits sains (avec MV)", """
                SELECT name, brand, health_score
                FROM mv_top_healthy_products
                LIMIT 100
            """),
            ("Filtre sucre/gras (sans index composé)", """
                SELECT p.name, nf.sugars, nf.fat
                FROM products p
                JOIN nutrition_facts nf ON p.product_id = nf.product_id
                WHERE nf.sugars < 5 AND nf.fat < 3
                LIMIT 200
            """),
            ("Nutrition par catégorie (avec MV)", """
                SELECT category, avg_proteins, avg_sugars, avg_fat, product_count
                FROM mv_category_nutrition
                ORDER BY avg_health_score DESC
                LIMIT 20
            """),
            ("Recherche partitionnée par date", """
                SELECT product_id, name, nutriscore, created_at
                FROM products_partitioned
                WHERE created_at >= '2026-01-01'
                AND nutriscore = 'A'
                LIMIT 100
            """)
        ]
        
        results = []
        for name, query in queries:
            print(f"{Fore.CYAN}[TEST]{Style.RESET_ALL} {name}")
            result = self.benchmark_query(name, query)
            results.append(result)
            print(f"{Fore.GREEN}[RESULT]{Style.RESET_ALL} {result['duration_ms']:.2f}ms ({result['rows']} lignes)\n")
        
        print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}Résumé des performances{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}\n")
        
        for r in results:
            print(f"{Fore.CYAN}{r['name']:<55}{Style.RESET_ALL} {r['duration_ms']:>8.2f}ms")
        
        sans_mv_1 = results[0]['duration_ms']
        avec_mv_1 = results[1]['duration_ms']
        gain_1 = ((sans_mv_1 - avec_mv_1) / sans_mv_1) * 100
        
        sans_mv_2 = results[2]['duration_ms']
        avec_mv_2 = results[3]['duration_ms']
        gain_2 = ((sans_mv_2 - avec_mv_2) / sans_mv_2) * 100
        
        print(f"\n{Fore.YELLOW}Gains de performance:{Style.RESET_ALL}")
        print(f"  Stats catégorie: {Fore.GREEN}{gain_1:+.1f}%{Style.RESET_ALL} ({sans_mv_1:.2f}ms → {avec_mv_1:.2f}ms)")
        print(f"  Top produits:    {Fore.GREEN}{gain_2:+.1f}%{Style.RESET_ALL} ({sans_mv_2:.2f}ms → {avec_mv_2:.2f}ms)")
    
    def analyze_explain_plans(self):
        print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}Analyse des plans d'exécution (optimisé){Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}\n")
        
        queries = [
            ("Vue matérialisée - Stats catégories", """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT category, avg_proteins, avg_sugars
                FROM mv_products_stats
                WHERE avg_health_score > 60
                ORDER BY avg_proteins DESC
                LIMIT 10
            """),
            ("Index composé - Filtre nutrition", """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT p.name, nf.sugars, nf.fat
                FROM products p
                JOIN nutrition_facts nf ON p.product_id = nf.product_id
                WHERE nf.sugars < 5 AND nf.fat < 3
                LIMIT 100
            """),
            ("Table partitionnée - Filtre temporel", """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT product_id, name, created_at
                FROM products_partitioned
                WHERE created_at >= '2026-01-01'
                AND nutriscore = 'A'
            """)
        ]
        
        with self.conn.cursor() as cur:
            for name, query in queries:
                print(f"{Fore.CYAN}{name}:{Style.RESET_ALL}\n")
                cur.execute(query)
                for row in cur.fetchall():
                    print(f"  {row[0]}")
                print()

def main():
    print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}PostgreSQL - Phase 3 : Optimisations avancées{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}\n")
    
    conn = connect_to_postgres()
    optimizer = PostgresOptimizer(conn)
    
    optimizer.create_partitioned_table()
    optimizer.create_materialized_views()
    optimizer.create_additional_indexes()
    
    optimizer.compare_before_after()
    optimizer.analyze_explain_plans()
    
    print(f"\n{Fore.GREEN}[DONE]{Style.RESET_ALL} Optimisations terminées\n")
    
    conn.close()

if __name__ == "__main__":
    main()