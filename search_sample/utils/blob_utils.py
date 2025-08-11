"""
Blob Storage utilities — handles upload, download, and JSON storage.
Uses Azure Blob SDK in production, with config loaded from config.yaml.
"""

import json
from tempfile import NamedTemporaryFile
from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING

# Create blob service client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def download_blob_to_bytes(blob_path: str, container_name: str = "documents") -> bytes:
    """
    Download a blob and return it as bytes (in-memory, no temp file).
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    return blob_client.download_blob().readall()

def upload_file(file_obj, container_name: str, blob_name: str):
    """
    Upload a file-like object to Azure Blob Storage.
    :param file_obj: File-like object (opened in binary mode)
    :param container_name: Azure Blob container
    :param blob_name: Path/name of blob in container
    """
    container_client = blob_service_client.get_container_client(container_name)
    container_client.upload_blob(name=blob_name, data=file_obj, overwrite=True)
    print(f"[Blob] Uploaded {blob_name} to container {container_name}")

def upload_json(data: dict, container_name: str, blob_name: str):
    """
    Upload JSON data as a blob.
    """
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    container_client = blob_service_client.get_container_client(container_name)
    container_client.upload_blob(name=blob_name, data=json_bytes, overwrite=True)
    print(f"[Blob] Uploaded JSON {blob_name} to container {container_name}")

# def download_file(container_name: str, blob_name: str) -> str:
#     """
#     Download blob to a temp file and return the local path.
#     """
#     container_client = blob_service_client.get_container_client(container_name)
#     blob_client = container_client.get_blob_client(blob_name)

#     temp_path = NamedTemporaryFile(delete=False)
#     temp_path.close()

#     with open(temp_path.name, "wb") as file:
#         download_stream = blob_client.download_blob()
#         file.write(download_stream.readall())

#     print(f"[Blob] Downloaded {blob_name} from container {container_name} -> {temp_path.name}")
#     return temp_path.name
