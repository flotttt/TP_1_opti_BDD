#!/usr/bin/env python3

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).parent


def init_performance_views():
    print("🔧 Initialisation des vues de performance...")

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "perfdb"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )
        cursor = conn.cursor()

        # Charger et exécuter performance_views.sql
        views_file = SCRIPT_DIR / "performance_views.sql"
        with open(views_file, "r") as f:
            cursor.execute(f.read())

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ Vues de performance créées avec succès")

    except Exception as e:
        print(f"❌ Erreur lors de la création des vues: {e}")
        return False

    return True


def main():
    print("🚀 Configuration du Dashboard Grafana")
    print("=" * 60)

    if not init_performance_views():
        print("\n❌ Échec de l'initialisation")
        return

    print("\n" + "=" * 60)
    print("✅ Configuration terminée!")
    print("=" * 60)
    print("\n📊 Pour importer le dashboard dans Grafana:")
    print("   1. Ouvre Grafana (http://localhost:3000)")
    print("   2. Va dans Dashboards → Import")
    print("   3. Clique sur 'Upload JSON file'")
    print("   4. Sélectionne: flight_dashboard.json")
    print("   5. Sélectionne la datasource PostgreSQL")
    print("   6. Clique sur 'Import'")
    print("\n🎯 Le dashboard contient:")
    print("   - Métriques de vols en temps réel")
    print("   - Carte GPS des avions")
    print("   - Statistiques par pays")
    print("   - Top avions actifs")
    print("   - Performance PostgreSQL")
    print("   - Taille des tables et index")
    print("   - Cache hit ratio")
    print("")


if __name__ == "__main__":
    main()
