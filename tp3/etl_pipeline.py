import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Lock

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from pymongo import MongoClient

load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_URI = f"mongodb://{os.getenv('MONGO_ROOT_USERNAME')}:{os.getenv('MONGO_ROOT_PASSWORD')}@{MONGO_HOST}:{MONGO_PORT}/"

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

ETL_INTERVAL = int(os.getenv("ETL_INTERVAL"))
BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE"))
NUM_WORKERS = int(os.getenv("ETL_WORKERS", "4"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DATABASE]
mongo_collection = mongo_db[MONGO_COLLECTION]

aircraft_cache = {}
country_cache = {}
cache_lock = Lock()


def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)


def get_or_create_aircraft_batch(cursor, icao24_list):
    if not icao24_list:
        return {}

    with cache_lock:
        cached = {
            icao: aircraft_cache[icao] for icao in icao24_list if icao in aircraft_cache
        }
        missing = [icao for icao in icao24_list if icao not in aircraft_cache]

    result = cached.copy()

    if missing:
        cursor.execute(
            "SELECT aircraft_id, icao24 FROM dim_aircraft WHERE icao24 = ANY(%s)",
            (missing,),
        )
        existing = {row[1]: row[0] for row in cursor.fetchall()}
        result.update(existing)

        new_icao24 = list(set(missing) - set(existing.keys()))

        if new_icao24:
            execute_values(
                cursor,
                "INSERT INTO dim_aircraft (icao24) VALUES %s ON CONFLICT (icao24) DO NOTHING RETURNING aircraft_id, icao24",
                [(icao,) for icao in new_icao24],
            )
            for row in cursor.fetchall():
                result[row[1]] = row[0]

        with cache_lock:
            aircraft_cache.update(result)

    if icao24_list:
        cursor.execute(
            "UPDATE dim_aircraft SET last_seen = NOW() WHERE icao24 = ANY(%s)",
            (icao24_list,),
        )

    return result


def get_or_create_country_batch(cursor, country_list):
    if not country_list:
        return {}

    country_list = [c for c in country_list if c]

    with cache_lock:
        cached = {
            country: country_cache[country]
            for country in country_list
            if country in country_cache
        }
        missing = [country for country in country_list if country not in country_cache]

    result = cached.copy()

    if missing:
        cursor.execute(
            "SELECT country_id, country_name FROM dim_country WHERE country_name = ANY(%s)",
            (missing,),
        )
        existing = {row[1]: row[0] for row in cursor.fetchall()}
        result.update(existing)

        new_countries = list(set(missing) - set(existing.keys()))

        if new_countries:
            execute_values(
                cursor,
                "INSERT INTO dim_country (country_name) VALUES %s ON CONFLICT (country_name) DO NOTHING RETURNING country_id, country_name",
                [(country,) for country in new_countries],
            )
            for row in cursor.fetchall():
                result[row[1]] = row[0]

        with cache_lock:
            country_cache.update(result)

    return result


def process_chunk(chunk):
    if not chunk:
        return 0

    conn = get_pg_connection()
    try:
        with conn.cursor() as cursor:
            icao24_list = [doc.get("icao24") for doc in chunk if doc.get("icao24")]
            country_list = [
                doc.get("origin_country") for doc in chunk if doc.get("origin_country")
            ]

            aircraft_map = get_or_create_aircraft_batch(cursor, icao24_list)
            country_map = get_or_create_country_batch(cursor, country_list)

            values = []
            for doc in chunk:
                icao24 = doc.get("icao24")
                if not icao24 or icao24 not in aircraft_map:
                    continue

                values.append(
                    (
                        aircraft_map[icao24],
                        country_map.get(doc.get("origin_country")),
                        doc.get("callsign"),
                        doc.get("longitude"),
                        doc.get("latitude"),
                        doc.get("geo_altitude"),
                        doc.get("velocity"),
                        doc.get("true_track"),
                        doc.get("on_ground"),
                        doc.get("api_timestamp"),
                        doc.get("ingestion_time"),
                    )
                )

            if values:
                execute_values(
                    cursor,
                    """
                    INSERT INTO fact_flight_positions (
                        aircraft_id, country_id, callsign, longitude, latitude,
                        geo_altitude, velocity, true_track, on_ground,
                        api_timestamp, ingestion_time
                    ) VALUES %s
                    ON CONFLICT (aircraft_id, api_timestamp)
                    DO UPDATE SET
                        country_id = EXCLUDED.country_id,
                        callsign = EXCLUDED.callsign,
                        longitude = EXCLUDED.longitude,
                        latitude = EXCLUDED.latitude,
                        geo_altitude = EXCLUDED.geo_altitude,
                        velocity = EXCLUDED.velocity,
                        true_track = EXCLUDED.true_track,
                        on_ground = EXCLUDED.on_ground,
                        ingestion_time = EXCLUDED.ingestion_time,
                        processed_time = NOW()
                    """,
                    values,
                )

        conn.commit()
        return len(values)
    except Exception as e:
        conn.rollback()
        print(f"Erreur chunk: {e}", flush=True)
        return 0
    finally:
        conn.close()


def run_etl():
    print("Démarrage du pipeline ETL MongoDB -> PostgreSQL (multi-thread)")
    print(
        f"Intervalle: {ETL_INTERVAL}s | Batch size: {BATCH_SIZE} | Workers: {NUM_WORKERS}\n"
    )

    last_processed_time = datetime.now() - timedelta(hours=1)

    conn = get_pg_connection()

    try:
        with conn.cursor() as cursor:
            with open("schema.sql", "r") as f:
                cursor.execute(f.read())
            conn.commit()
            print("Schéma PostgreSQL initialisé\n")
    except Exception as e:
        print(f"Schéma déjà existant ou erreur: {e}\n")
        conn.rollback()
    finally:
        conn.close()

    cycle_count = 0

    while True:
        try:
            query = {"ingestion_time": {"$gt": last_processed_time}}

            documents = list(
                mongo_collection.find(query).sort("ingestion_time", 1).limit(BATCH_SIZE)
            )

            if documents:
                chunk_size = len(documents) // NUM_WORKERS
                if chunk_size == 0:
                    chunk_size = len(documents)

                chunks = [
                    documents[i : i + chunk_size]
                    for i in range(0, len(documents), chunk_size)
                ]

                total_processed = 0
                with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                    futures = [
                        executor.submit(process_chunk, chunk) for chunk in chunks
                    ]
                    for future in as_completed(futures):
                        total_processed += future.result()

                last_processed_time = max(doc["ingestion_time"] for doc in documents)

                now = datetime.now().strftime("%H:%M:%S")
                print(
                    f"[{now}] Cycle #{cycle_count} | {total_processed} positions traitées ({NUM_WORKERS} threads)",
                    flush=True,
                )

                if cycle_count % 60 == 0 and cycle_count > 0:
                    conn = get_pg_connection()
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT refresh_latest_positions()")
                            cursor.execute("SELECT aggregate_hourly_stats()")
                            stats_count = cursor.fetchone()[0]
                            cursor.execute("SELECT cleanup_old_positions(48)")
                            deleted = cursor.fetchone()[0]
                        conn.commit()
                        print(
                            f"   Vue matérialisée rafraîchie | {stats_count} stats horaires | {deleted} anciennes positions supprimées",
                            flush=True,
                        )
                    finally:
                        conn.close()

            else:
                now = datetime.now().strftime("%H:%M:%S")
                print(
                    f"[{now}] Cycle #{cycle_count} | Aucune nouvelle donnée", flush=True
                )

        except Exception as e:
            print(f"Erreur ETL: {e}", flush=True)

        cycle_count += 1
        time.sleep(ETL_INTERVAL)


if __name__ == "__main__":
    run_etl()
