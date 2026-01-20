import os
import time
from typing import Dict, List

import psycopg2
from colorama import Fore, Style, init
from dotenv import load_dotenv

init(autoreset=True)
load_dotenv()


def connect_to_postgres():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")

    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )


class PostgresBenchmark:
    def __init__(self, conn):
        self.conn = conn

    def execute_query(self, query: str, params=None):
        with self.conn.cursor() as cur:
            start = time.time()
            cur.execute(query, params)
            results = cur.fetchall()
            duration = time.time() - start
            return results, duration

    def analyze_query(self, name: str, query: str, params=None):
        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Requête: {name}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        print(f"{Fore.YELLOW}SQL:{Style.RESET_ALL}")
        print(query)
        print()

        results, duration = self.execute_query(query, params)
        print(
            f"{Fore.GREEN}Résultats:{Style.RESET_ALL} {len(results)} lignes en {duration * 1000:.2f}ms\n"
        )

        explain_query = f"EXPLAIN {query}"
        explain_results, _ = self.execute_query(explain_query, params)
        print(f"{Fore.MAGENTA}EXPLAIN:{Style.RESET_ALL}")
        for row in explain_results:
            print(f"  {row[0]}")
        print()

        analyze_query = f"EXPLAIN ANALYZE {query}"
        analyze_results, _ = self.execute_query(analyze_query, params)
        print(f"{Fore.MAGENTA}EXPLAIN ANALYZE:{Style.RESET_ALL}")
        for row in analyze_results:
            print(f"  {row[0]}")
        print()

        buffers_query = f"EXPLAIN (ANALYZE, BUFFERS) {query}"
        buffers_results, _ = self.execute_query(buffers_query, params)
        print(f"{Fore.MAGENTA}EXPLAIN (ANALYZE, BUFFERS):{Style.RESET_ALL}")
        for row in buffers_results:
            print(f"  {row[0]}")

        return {"name": name, "duration_ms": duration * 1000, "rows": len(results)}


def run_benchmarks():
    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Benchmark PostgreSQL - Phase 2{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    conn = connect_to_postgres()
    benchmark = PostgresBenchmark(conn)

    results = []

    print(f"{Fore.CYAN}[1/8]{Style.RESET_ALL} Filtre simple sur nutriscore")
    results.append(
        benchmark.analyze_query(
            "Produits avec Nutriscore A",
            """
        SELECT product_id, name, nutriscore, health_score
        FROM products
        WHERE nutriscore = 'A'
        """,
        )
    )

    print(f"{Fore.CYAN}[2/8]{Style.RESET_ALL} Filtre + tri + limit")
    results.append(
        benchmark.analyze_query(
            "Top 100 produits sains",
            """
        SELECT p.name, b.name as brand, p.health_score
        FROM products p
        JOIN brands b ON p.brand_id = b.id
        WHERE p.health_score > 70
        ORDER BY p.health_score DESC
        LIMIT 100
        """,
        )
    )

    print(f"{Fore.CYAN}[3/8]{Style.RESET_ALL} GROUP BY simple")
    results.append(
        benchmark.analyze_query(
            "Nombre de produits par nutriscore",
            """
        SELECT nutriscore, COUNT(*) as count
        FROM products
        WHERE nutriscore IS NOT NULL
        GROUP BY nutriscore
        ORDER BY nutriscore
        """,
        )
    )

    print(f"{Fore.CYAN}[4/8]{Style.RESET_ALL} GROUP BY avec jointure")
    results.append(
        benchmark.analyze_query(
            "Nombre de produits par marque",
            """
        SELECT b.name, COUNT(*) as product_count
        FROM products p
        JOIN brands b ON p.brand_id = b.id
        GROUP BY b.name
        HAVING COUNT(*) > 10
        ORDER BY product_count DESC
        LIMIT 50
        """,
        )
    )

    print(f"{Fore.CYAN}[5/8]{Style.RESET_ALL} Jointure multiple")
    results.append(
        benchmark.analyze_query(
            "Produits avec nutrition et catégorie",
            """
        SELECT p.name, b.name as brand, c.name as category,
               nf.proteins, nf.sugars, nf.fat
        FROM products p
        JOIN brands b ON p.brand_id = b.id
        JOIN categories c ON p.main_category_id = c.id
        JOIN nutrition_facts nf ON p.product_id = nf.product_id
        WHERE nf.proteins > 15
        LIMIT 500
        """,
        )
    )

    print(f"{Fore.CYAN}[6/8]{Style.RESET_ALL} Agrégation complexe")
    results.append(
        benchmark.analyze_query(
            "Stats nutritionnelles par catégorie",
            """
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
        """,
        )
    )

    print(f"{Fore.CYAN}[7/8]{Style.RESET_ALL} Filtre sur nutrition")
    results.append(
        benchmark.analyze_query(
            "Produits faibles en sucre et gras",
            """
        SELECT p.name, b.name as brand, nf.sugars, nf.fat
        FROM products p
        JOIN brands b ON p.brand_id = b.id
        JOIN nutrition_facts nf ON p.product_id = nf.product_id
        WHERE nf.sugars < 5 AND nf.fat < 3
        ORDER BY nf.sugars ASC
        LIMIT 200
        """,
        )
    )

    print(f"{Fore.CYAN}[8/8]{Style.RESET_ALL} Recherche texte + filtres")
    results.append(
        benchmark.analyze_query(
            "Recherche pizza nutriscore A/B",
            """
        SELECT p.name, b.name as brand, p.nutriscore
        FROM products p
        JOIN brands b ON p.brand_id = b.id
        WHERE p.name ILIKE '%pizza%'
        AND p.nutriscore IN ('A', 'B')
        ORDER BY p.health_score DESC
        LIMIT 50
        """,
        )
    )

    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Résumé des performances{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    for r in results:
        print(
            f"{Fore.CYAN}{r['name']:<50}{Style.RESET_ALL} {r['duration_ms']:>8.2f}ms ({r['rows']} lignes)"
        )

    avg_time = sum(r["duration_ms"] for r in results) / len(results)
    print(f"\n{Fore.YELLOW}Temps moyen:{Style.RESET_ALL} {avg_time:.2f}ms")

    conn.close()


if __name__ == "__main__":
    run_benchmarks()
