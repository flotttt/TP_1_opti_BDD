from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient
import os


def get_mongo_uri() -> str:
    uri = os.getenv("MONGO_URI")
    if uri:
        return uri
    username = os.getenv("MONGO_ROOT_USERNAME", "root")
    password = os.getenv("MONGO_ROOT_PASSWORD", "example")
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    database = os.getenv("MONGO_DATABASE", "test")
    return f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"


def main() -> None:
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(env_path)
    uri = get_mongo_uri()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    info = client.server_info()
    db_names = client.list_database_names()
    print("Connexion MongoDB OK")
    print(f"Version: {info.get('version')}")
    print(f"Bases: {db_names}")


if __name__ == "__main__":
    main()

