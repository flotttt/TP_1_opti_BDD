import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pymongo import MongoClient


def get_mongo_uri() -> str:
    uri = os.getenv("MONGO_URI")
    if uri:
        return uri
    username = os.getenv("MONGO_ROOT_USERNAME")
    password = os.getenv("MONGO_ROOT_PASSWORD")
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    database = os.getenv("MONGO_DATABASE")

    # Encoder le username et password pour l'URI
    username_encoded = quote_plus(username) if username else ""
    password_encoded = quote_plus(password) if password else ""

    return f"mongodb://{username_encoded}:{password_encoded}@{host}:{port}/{database}?authSource=admin"


def main() -> None:
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        print(f"Fichier .env non trouvé: {env_path}")
        return

    uri = get_mongo_uri()
    print(f"URI: {uri}")  # Debug
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    info = client.server_info()
    db_names = client.list_database_names()
    print("Connexion MongoDB OK")
    print(f"Version: {info.get('version')}")
    print(f"Bases: {db_names}")


if __name__ == "__main__":
    main()
