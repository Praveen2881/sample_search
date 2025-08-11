import logging
import json
import azure.functions as func
from azure.storage.blob import BlobClient
from config import (
    SERVICE_BUS_CONNECTION_STRING,
    SERVICE_BUS_QUEUE_NAME,
    AZURE_STORAGE_ACCOUNT_URL,
    AZURE_STORAGE_CONTAINER_NAME,
    MOSAICDB_URI
)
from db.crud import update_job_status
from functions.diverter_function import route_document
from functions.chunk_embed_processor import process_chunking_and_embedding

def main(msg: func.ServiceBusMessage):
    logging.info('Worker triggered by Service Bus message.')

    message_body = json.loads(msg.get_body().decode('utf-8'))
    blob_url = message_body['blob_url']
    metadata = message_body['metadata']

    try:
        # Update job to "PROCESSING"
        update_job_status(metadata.get("job_id"), status="PROCESSING")

        # Route to correct processor
        processed_json = route_document(blob_url, metadata)

        # Upload processed JSON to Blob
        json_blob_name = f"processed/{metadata.get('document_id')}.json"
        blob_client = BlobClient(
            account_url=AZURE_STORAGE_ACCOUNT_URL,
            container_name=AZURE_STORAGE_CONTAINER_NAME,
            blob_name=json_blob_name
        )
        blob_client.upload_blob(json.dumps(processed_json), overwrite=True)
        logging.info(f"Uploaded processed JSON to {json_blob_name}")

        # Trigger chunking + embedding
        process_chunking_and_embedding(json_blob_name, metadata)

        # Update job to "COMPLETED"
        update_job_status(metadata.get("job_id"), status="COMPLETED")

    except Exception as e:
        logging.error(f"Error processing document: {e}")
        update_job_status(metadata.get("job_id"), status="FAILED", error_message=str(e))
