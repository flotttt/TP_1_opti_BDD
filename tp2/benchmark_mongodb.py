import json
import os
import time
from typing import Dict, List

from colorama import Fore, Style, init
from dotenv import load_dotenv
from pymongo import MongoClient

init(autoreset=True)
load_dotenv()


def connect_to_mongodb():
    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Connexion à MongoDB...")

    username = os.getenv("MONGO_ROOT_USERNAME")
    password = os.getenv("MONGO_ROOT_PASSWORD")
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    database = os.getenv("MONGO_DATABASE")

    if username and password:
        uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
    else:
        uri = f"mongodb://{host}:{port}/"

    client = MongoClient(uri)
    db = client[database]
    print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} Connecté à MongoDB")
    return db


class MongoDBBenchmark:
    def __init__(self, db):
        self.db = db
        self.collection = db["products"]

    def benchmark_query(
        self,
        name: str,
        query: Dict,
        projection: Dict = None,
        sort: List = None,
        limit: int = None,
    ):
        cursor = self.collection.find(query, projection)

        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)

        start = time.time()
        results = list(cursor)
        duration = time.time() - start

        return {"name": name, "duration_ms": duration * 1000, "rows": len(results)}

    def explain_query(
        self,
        name: str,
        query: Dict,
        projection: Dict = None,
        sort: List = None,
        limit: int = None,
    ):
        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Requête: {name}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        print(f"{Fore.YELLOW}Query:{Style.RESET_ALL}")
        print(json.dumps(query, indent=2))
        if projection:
            print(f"\n{Fore.YELLOW}Projection:{Style.RESET_ALL}")
            print(json.dumps(projection, indent=2))
        print()

        cursor = self.collection.find(query, projection)
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)

        explain_result = cursor.explain()

        exec_stats = explain_result.get("executionStats", {})

        print(f"{Fore.MAGENTA}Execution Stats:{Style.RESET_ALL}")
        print(f"  Execution Time: {exec_stats.get('executionTimeMillis', 0)}ms")
        print(f"  Total Docs Examined: {exec_stats.get('totalDocsExamined', 0)}")
        print(f"  Total Keys Examined: {exec_stats.get('totalKeysExamined', 0)}")
        print(f"  Returned: {exec_stats.get('nReturned', 0)}")

        winning_plan = explain_result.get("queryPlanner", {}).get("winningPlan", {})
        stage = winning_plan.get("stage", "Unknown")

        print(f"\n{Fore.MAGENTA}Winning Plan:{Style.RESET_ALL}")
        print(f"  Stage: {stage}")

        if stage == "COLLSCAN":
            print(
                f"  {Fore.RED}⚠ Collection Scan (pas d'index utilisé){Style.RESET_ALL}"
            )
        elif stage == "IXSCAN" or "IXSCAN" in str(winning_plan):
            print(f"  {Fore.GREEN}✓ Index Scan{Style.RESET_ALL}")
            if "indexName" in winning_plan:
                print(f"  Index: {winning_plan['indexName']}")

        return exec_stats


def run_phase4_before_optimization():
    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}MongoDB - Phase 4 : AVANT optimisation{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    db = connect_to_mongodb()
    benchmark = MongoDBBenchmark(db)

    results_before = []

    print(f"{Fore.CYAN}[1/8]{Style.RESET_ALL} Filtre simple sur nutriscore")
    stats = benchmark.explain_query("Produits Nutriscore A", {"nutriscore": "A"})
    results_before.append(
        {
            "name": "Nutriscore A",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[2/8]{Style.RESET_ALL} Filtre avec tri")
    stats = benchmark.explain_query(
        "Top produits sains",
        {"health_score": {"$gt": 70}},
        projection={"name": 1, "brand": 1, "health_score": 1},
        sort=[("health_score", -1)],
        limit=100,
    )
    results_before.append(
        {
            "name": "Top produits sains",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[3/8]{Style.RESET_ALL} Agrégation simple")
    pipeline = [
        {"$match": {"nutriscore": {"$ne": None}}},
        {"$group": {"_id": "$nutriscore", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    start = time.time()
    results = list(db.products.aggregate(pipeline))
    duration = time.time() - start

    print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Agrégation: Compte par nutriscore{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")
    print(f"{Fore.YELLOW}Pipeline:{Style.RESET_ALL}")
    print(json.dumps(pipeline, indent=2))
    print(f"\n{Fore.MAGENTA}Résultat:{Style.RESET_ALL} {duration * 1000:.2f}ms\n")

    results_before.append(
        {
            "name": "Agrégation nutriscore",
            "time": duration * 1000,
            "docs_examined": "N/A",
            "keys_examined": "N/A",
        }
    )

    print(f"{Fore.CYAN}[4/8]{Style.RESET_ALL} Filtre sur nutrition")
    stats = benchmark.explain_query(
        "Produits faibles sucre et gras",
        {"nutrition_per_100g.sugars": {"$lt": 5}, "nutrition_per_100g.fat": {"$lt": 3}},
        projection={
            "name": 1,
            "brand": 1,
            "nutrition_per_100g.sugars": 1,
            "nutrition_per_100g.fat": 1,
        },
        limit=200,
    )
    results_before.append(
        {
            "name": "Filtre nutrition",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[5/8]{Style.RESET_ALL} Recherche texte")
    stats = benchmark.explain_query(
        "Recherche pizza",
        {
            "name": {"$regex": "pizza", "$options": "i"},
            "nutriscore": {"$in": ["A", "B"]},
        },
        projection={"name": 1, "brand": 1, "nutriscore": 1},
        sort=[("health_score", -1)],
        limit=50,
    )
    results_before.append(
        {
            "name": "Recherche pizza",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[6/8]{Style.RESET_ALL} Filtre catégorie")
    stats = benchmark.explain_query(
        "Produits d'une catégorie", {"main_category": "Beverages"}, limit=100
    )
    results_before.append(
        {
            "name": "Filtre catégorie",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[7/8]{Style.RESET_ALL} Filtre nova group")
    stats = benchmark.explain_query(
        "Produits ultra-transformés",
        {"nova_group": 4},
        projection={"name": 1, "brand": 1, "nova_group": 1},
        limit=200,
    )
    results_before.append(
        {
            "name": "Nova group 4",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[8/8]{Style.RESET_ALL} Agrégation complexe")
    pipeline = [
        {"$match": {"nutrition_per_100g.proteins": {"$exists": True}}},
        {
            "$group": {
                "_id": "$main_category",
                "avg_proteins": {"$avg": "$nutrition_per_100g.proteins"},
                "avg_sugars": {"$avg": "$nutrition_per_100g.sugars"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 100}}},
        {"$sort": {"avg_proteins": -1}},
        {"$limit": 20},
    ]
    start = time.time()
    results = list(db.products.aggregate(pipeline))
    duration = time.time() - start

    print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Agrégation: Stats par catégorie{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")
    print(f"{Fore.MAGENTA}Résultat:{Style.RESET_ALL} {duration * 1000:.2f}ms\n")

    results_before.append(
        {
            "name": "Agrégation catégorie",
            "time": duration * 1000,
            "docs_examined": "N/A",
            "keys_examined": "N/A",
        }
    )

    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Résumé AVANT optimisation{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    for r in results_before:
        print(
            f"{Fore.CYAN}{r['name']:<30}{Style.RESET_ALL} {r['time']:>8.2f}ms | Docs: {r['docs_examined']:>6} | Keys: {r['keys_examined']:>6}"
        )

    return results_before


def create_mongodb_indexes():
    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Création des index MongoDB{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    db = connect_to_mongodb()
    collection = db["products"]

    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Suppression des anciens index...")
    collection.drop_indexes()

    print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} Création des nouveaux index...\n")

    indexes = [
        # product_id n'est PAS unique dans les données, on évite donc l'unicité
        ("product_id", [("product_id", 1)], False),
        ("nutriscore", [("nutriscore", 1)], False),
        ("health_score", [("health_score", -1)], False),
        ("nova_group", [("nova_group", 1)], False),
        ("main_category", [("main_category", 1)], False),
        ("brand", [("brand", 1)], False),
        ("nutriscore_health", [("nutriscore", 1), ("health_score", -1)], False),
        ("nutrition_sugars", [("nutrition_per_100g.sugars", 1)], False),
        ("nutrition_fat", [("nutrition_per_100g.fat", 1)], False),
        ("nutrition_proteins", [("nutrition_per_100g.proteins", -1)], False),
        (
            "nutrition_sugars_fat",
            [("nutrition_per_100g.sugars", 1), ("nutrition_per_100g.fat", 1)],
            False,
        ),
        ("category_health", [("main_category", 1), ("health_score", -1)], False),
    ]

    for name, keys, unique in indexes:
        collection.create_index(keys, name=name, unique=unique)
        print(f"{Fore.GREEN}[CREATED]{Style.RESET_ALL} Index: {name}")

    collection.create_index([("name", "text")])
    print(f"{Fore.GREEN}[CREATED]{Style.RESET_ALL} Index: text_search (name)")

    print(f"\n{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {len(indexes) + 1} index créés")


def run_phase4_after_optimization():
    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}MongoDB - Phase 4 : APRÈS optimisation{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    db = connect_to_mongodb()
    benchmark = MongoDBBenchmark(db)

    results_after = []

    print(f"{Fore.CYAN}[1/8]{Style.RESET_ALL} Filtre simple sur nutriscore")
    stats = benchmark.explain_query("Produits Nutriscore A", {"nutriscore": "A"})
    results_after.append(
        {
            "name": "Nutriscore A",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[2/8]{Style.RESET_ALL} Filtre avec tri")
    stats = benchmark.explain_query(
        "Top produits sains",
        {"health_score": {"$gt": 70}},
        projection={"name": 1, "brand": 1, "health_score": 1},
        sort=[("health_score", -1)],
        limit=100,
    )
    results_after.append(
        {
            "name": "Top produits sains",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[3/8]{Style.RESET_ALL} Agrégation simple")
    pipeline = [
        {"$match": {"nutriscore": {"$ne": None}}},
        {"$group": {"_id": "$nutriscore", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    start = time.time()
    results = list(db.products.aggregate(pipeline))
    duration = time.time() - start

    print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Agrégation: Compte par nutriscore{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")
    print(f"{Fore.MAGENTA}Résultat:{Style.RESET_ALL} {duration * 1000:.2f}ms\n")

    results_after.append(
        {
            "name": "Agrégation nutriscore",
            "time": duration * 1000,
            "docs_examined": "N/A",
            "keys_examined": "N/A",
        }
    )

    print(f"{Fore.CYAN}[4/8]{Style.RESET_ALL} Filtre sur nutrition")
    stats = benchmark.explain_query(
        "Produits faibles sucre et gras",
        {"nutrition_per_100g.sugars": {"$lt": 5}, "nutrition_per_100g.fat": {"$lt": 3}},
        projection={
            "name": 1,
            "brand": 1,
            "nutrition_per_100g.sugars": 1,
            "nutrition_per_100g.fat": 1,
        },
        limit=200,
    )
    results_after.append(
        {
            "name": "Filtre nutrition",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[5/8]{Style.RESET_ALL} Recherche texte")
    stats = benchmark.explain_query(
        "Recherche pizza",
        {"$text": {"$search": "pizza"}, "nutriscore": {"$in": ["A", "B"]}},
        projection={
            "name": 1,
            "brand": 1,
            "nutriscore": 1,
            "score": {"$meta": "textScore"},
        },
        sort=[("score", {"$meta": "textScore"})],
        limit=50,
    )
    results_after.append(
        {
            "name": "Recherche pizza",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[6/8]{Style.RESET_ALL} Filtre catégorie")
    stats = benchmark.explain_query(
        "Produits d'une catégorie", {"main_category": "Beverages"}, limit=100
    )
    results_after.append(
        {
            "name": "Filtre catégorie",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[7/8]{Style.RESET_ALL} Filtre nova group")
    stats = benchmark.explain_query(
        "Produits ultra-transformés",
        {"nova_group": 4},
        projection={"name": 1, "brand": 1, "nova_group": 1},
        limit=200,
    )
    results_after.append(
        {
            "name": "Nova group 4",
            "time": stats.get("executionTimeMillis", 0),
            "docs_examined": stats.get("totalDocsExamined", 0),
            "keys_examined": stats.get("totalKeysExamined", 0),
        }
    )

    print(f"{Fore.CYAN}[8/8]{Style.RESET_ALL} Agrégation complexe")
    pipeline = [
        {"$match": {"nutrition_per_100g.proteins": {"$exists": True}}},
        {
            "$group": {
                "_id": "$main_category",
                "avg_proteins": {"$avg": "$nutrition_per_100g.proteins"},
                "avg_sugars": {"$avg": "$nutrition_per_100g.sugars"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 100}}},
        {"$sort": {"avg_proteins": -1}},
        {"$limit": 20},
    ]
    start = time.time()
    results = list(db.products.aggregate(pipeline))
    duration = time.time() - start

    print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Agrégation: Stats par catégorie{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")
    print(f"{Fore.MAGENTA}Résultat:{Style.RESET_ALL} {duration * 1000:.2f}ms\n")

    results_after.append(
        {
            "name": "Agrégation catégorie",
            "time": duration * 1000,
            "docs_examined": "N/A",
            "keys_examined": "N/A",
        }
    )

    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Résumé APRÈS optimisation{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    for r in results_after:
        print(
            f"{Fore.CYAN}{r['name']:<30}{Style.RESET_ALL} {r['time']:>8.2f}ms | Docs: {r['docs_examined']:>6} | Keys: {r['keys_examined']:>6}"
        )

    return results_after


def compare_results(before, after):
    print(f"\n{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Comparaison AVANT / APRÈS{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'=' * 80}{Style.RESET_ALL}\n")

    for b, a in zip(before, after):
        before_time = b["time"]
        after_time = a["time"]

        if isinstance(before_time, (int, float)) and isinstance(
            after_time, (int, float)
        ):
            if before_time > 0:
                gain = ((before_time - after_time) / before_time) * 100
                color = Fore.GREEN if gain > 0 else Fore.RED
                print(
                    f"{Fore.CYAN}{b['name']:<30}{Style.RESET_ALL} {before_time:>7.2f}ms → {after_time:>7.2f}ms {color}({gain:+.1f}%){Style.RESET_ALL}"
                )
            else:
                print(
                    f"{Fore.CYAN}{b['name']:<30}{Style.RESET_ALL} {before_time:>7.2f}ms → {after_time:>7.2f}ms"
                )


def main():
    results_before = run_phase4_before_optimization()

    create_mongodb_indexes()

    results_after = run_phase4_after_optimization()

    compare_results(results_before, results_after)

    print(f"\n{Fore.GREEN}[DONE]{Style.RESET_ALL} Phase 4 MongoDB terminée\n")


if __name__ == "__main__":
    main()
