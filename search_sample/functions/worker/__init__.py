# worker/__init__.py
import logging
import json
import azure.functions as func
from azure.storage.blob import BlobClient
from config import (
    AZURE_STORAGE_ACCOUNT_URL,
    AZURE_STORAGE_CONTAINER_NAME
)
from functions.diverter_function import route_document
from functions.chunk_embed_processor import process_chunking_and_embedding
from utils.blob_utils import download_blob_to_bytes

def main(msg: func.ServiceBusMessage):
    logging.info('[Worker] Triggered by Service Bus message.')

    try:
        message_body = json.loads(msg.get_body().decode('utf-8'))
        blob_path = message_body['blob_path']
        extension = message_body['extension']
        metadata = message_body.get('metadata', {})

        logging.info(f"[Worker] Processing {blob_path} ({extension})")

        # Download blob into memory
        file_bytes = download_blob_to_bytes(blob_path)

        # Route to correct processor — returns processed JSON
        processed_json = route_document(file_bytes, extension)

        # Upload processed JSON to Blob
        json_blob_name = f"processed/{metadata.get('document_id', 'unknown')}.json"
        blob_client = BlobClient(
            account_url=AZURE_STORAGE_ACCOUNT_URL,
            container_name=AZURE_STORAGE_CONTAINER_NAME,
            blob_name=json_blob_name
        )
        blob_client.upload_blob(
            json.dumps(processed_json, ensure_ascii=False),
            overwrite=True
        )
        logging.info(f"[Worker] Uploaded processed JSON to {json_blob_name}")

        # Chunk + embed → store in MosaicDB
        process_chunking_and_embedding(json_blob_name, metadata)

        logging.info(f"[Worker] Completed processing for {blob_path}")

    except Exception as e:
        logging.error(f"[Worker] Error processing document: {e}", exc_info=True)
        raise
