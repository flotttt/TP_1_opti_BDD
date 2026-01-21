#!/usr/bin/env python3

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).parent
processes = []


def signal_handler(sig, frame):
    print("\n\nArrêt du pipeline...")
    stop_all_processes()
    sys.exit(0)


def stop_all_processes():
    for name, proc in processes:
        if proc.poll() is None:
            print(f"Arrêt de {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    print("\nProcessus Python arrêtés (conteneurs Docker laissés en cours)")


def check_docker():
    print("Vérification de Docker...")
    result = subprocess.run(["docker", "ps"], capture_output=True, text=True)
    if result.returncode != 0:
        print("Erreur: Docker n'est pas démarré")
        sys.exit(1)
    print("Docker actif")


def check_docker_containers():
    print("\nVérification des conteneurs Docker...")
    result = subprocess.run(
        ["docker-compose", "ps"], cwd=SCRIPT_DIR, capture_output=True, text=True
    )
    if "Up" not in result.stdout:
        print("Erreur: Les conteneurs Docker ne sont pas démarrés")
        print("Lancer: docker-compose up -d")
        sys.exit(1)
    print("Conteneurs Docker actifs")


def wait_for_databases():
    print("\nAttente du démarrage des bases de données...")
    max_retries = 30
    retry_count = 0

    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                port=int(os.getenv("POSTGRES_PORT")),
                database=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
            )
            conn.close()
            print("PostgreSQL prêt")
            break
        except:
            retry_count += 1
            time.sleep(1)

    if retry_count >= max_retries:
        print("Timeout: PostgreSQL n'a pas démarré")
        sys.exit(1)

    time.sleep(3)
    print("MongoDB prêt")


def init_postgres_schema():
    print("\nInitialisation du schéma PostgreSQL...")

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT")),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        cursor = conn.cursor()

        schema_file = SCRIPT_DIR / "schema.sql"
        if not schema_file.exists():
            print("Erreur: Fichier schema.sql introuvable")
            sys.exit(1)

        with open(schema_file, "r") as f:
            cursor.execute(f.read())
        conn.commit()
        print("Schéma de base créé")

        perf_views_file = SCRIPT_DIR / "performance_views.sql"
        if perf_views_file.exists():
            with open(perf_views_file, "r") as f:
                cursor.execute(f.read())
            conn.commit()
            print("Vues de performance créées")

        optimizations_file = SCRIPT_DIR / "optimizations.sql"
        if optimizations_file.exists():
            with open(optimizations_file, "r") as f:
                cursor.execute(f.read())
            conn.commit()
            print("Optimisations appliquées (vue matérialisée + index)")

        cursor.close()
        conn.close()
        print("Initialisation PostgreSQL complète")
    except Exception as e:
        print(f"Avertissement: {e}")


def print_output(name, proc, prefix_color):
    RESET = "\033[0m"
    for line in iter(proc.stdout.readline, ""):
        if line:
            print(f"{prefix_color}[{name}]{RESET} {line.rstrip()}")
    proc.stdout.close()


def start_process(name, script, color):
    print(f"\nDémarrage de {name}...")

    proc = subprocess.Popen(
        [sys.executable, "-u", script],
        cwd=SCRIPT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    processes.append((name, proc))
    print(f"{name} démarré (PID: {proc.pid})")

    thread = threading.Thread(
        target=print_output, args=(name, proc, color), daemon=True
    )
    thread.start()

    return proc


def monitor_processes():
    print("\n" + "=" * 60)
    print("Pipeline démarré avec succès")
    print("=" * 60)
    print("\nProcessus actifs:")
    for name, proc in processes:
        print(f"  - {name} (PID: {proc.pid})")

    print("\nLogs affichés en temps réel ci-dessous")
    print("Appuyer sur CTRL+C pour arrêter le pipeline\n")
    print("=" * 60)

    while True:
        time.sleep(5)
        for name, proc in processes:
            if proc.poll() is not None:
                print(f"\n{name} s'est arrêté (code: {proc.returncode})")


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("Démarrage du pipeline de tracking de vols")
    print("=" * 60)

    check_docker()
    check_docker_containers()
    wait_for_databases()
    init_postgres_schema()

    BLUE = "\033[94m"
    GREEN = "\033[92m"

    start_process("Ingestion MongoDB", "avion.py", BLUE)
    time.sleep(2)
    start_process("ETL PostgreSQL", "etl_pipeline.py", GREEN)

    try:
        monitor_processes()
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == "__main__":
    main()
