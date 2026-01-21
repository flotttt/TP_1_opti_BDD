import os
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne

load_dotenv()

URL_OPENSKY = "https://opensky-network.org/api/states/all"
PARAMS = {}

RETENTION_HOURS = int(os.getenv("RETENTION_HOURS"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL"))

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_USER = os.getenv("MONGO_ROOT_USERNAME")
MONGO_PASS = os.getenv("MONGO_ROOT_PASSWORD")
MONGO_DB = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

collection.create_index(
    [("ingestion_time", ASCENDING)], expireAfterSeconds=RETENTION_HOURS * 3600
)
collection.create_index(
    [("icao24", ASCENDING), ("api_timestamp", ASCENDING)], unique=True
)

print(
    f"Connexion MongoDB active (rétention: {RETENTION_HOURS}h, intervalle: {SCRAPE_INTERVAL}s)"
)


def cleanup_old_data():
    cutoff = datetime.now() - timedelta(hours=RETENTION_HOURS)
    result = collection.delete_many({"ingestion_time": {"$lt": cutoff}})
    if result.deleted_count > 0:
        print(f"{result.deleted_count} documents supprimés (> {RETENTION_HOURS}h)")


def run_ingestion():
    print("Démarrage de l'ingestion continue (Zone: Monde entier)", flush=True)
    cycle_count = 0

    while True:
        try:
            response = requests.get(URL_OPENSKY, params=PARAMS, timeout=10)

            if response.status_code == 200:
                data = response.json()
                states = data.get("states", [])
                timestamp = data["time"]

                operations = []
                if states:
                    for state in states:
                        if state[5] and state[6]:
                            plane_obj = {
                                "icao24": state[0],
                                "callsign": state[1].strip() if state[1] else "N/A",
                                "origin_country": state[2],
                                "longitude": state[5],
                                "latitude": state[6],
                                "geo_altitude": state[13],
                                "velocity": state[9],
                                "true_track": state[10],
                                "on_ground": state[8],
                                "ingestion_time": datetime.now(),
                                "api_timestamp": timestamp,
                            }

                            operations.append(
                                UpdateOne(
                                    {"icao24": state[0], "api_timestamp": timestamp},
                                    {"$set": plane_obj},
                                    upsert=True,
                                )
                            )

                if operations:
                    result = collection.bulk_write(operations, ordered=False)
                    inserted = result.upserted_count
                    updated = result.modified_count

                    now = datetime.now().strftime("%H:%M:%S")
                    print(
                        f"[{now}] Cycle #{cycle_count} | {inserted} nouveaux | {updated} mis à jour",
                        flush=True,
                    )
                else:
                    now = datetime.now().strftime("%H:%M:%S")
                    print(f"[{now}] Cycle #{cycle_count} | Aucune donnée", flush=True)

                if cycle_count % 10 == 0:
                    cleanup_old_data()

            else:
                print(f"Erreur API: {response.status_code}", flush=True)

        except Exception as e:
            print(f"Erreur: {e}", flush=True)

        cycle_count += 1
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    run_ingestion()
