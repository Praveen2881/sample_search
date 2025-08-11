import yaml
import os

# Locate config.yaml (same directory as config.py or via ENV var)
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yaml")

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

# Database
DATABASE_URL = config["database"]["url"]

# Azure
AZURE_STORAGE_CONNECTION_STRING = config["azure"]["storage_connection_string"]
AZURE_SERVICE_BUS_CONNECTION_STRING = config["azure"]["service_bus_connection_string"]
AZURE_EVENTGRID_ENDPOINT = config["azure"]["event_grid_endpoint"]

# MosaicDB
MOSAICDB_URI = config['mosaic']["mosaicdb_uri"]
MOSAIC_API_KEY = config['mosaic']["mosaic_api_key"]
MOSAIC_MODEL_ENDPOINT = config['mosaic']["mosaic_model_endpoint"]

# Embedding
EMBEDDING_PROVIDER = config["embedding"]["provider"]
EMBEDDING_API_KEY = config["embedding"]["api_key"]
