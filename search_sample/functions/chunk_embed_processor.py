import logging
import json
from azure.storage.blob import BlobClient
from config import (
    AZURE_STORAGE_ACCOUNT_URL,
    AZURE_STORAGE_CONTAINER_NAME
)
from utils.embedding_utils import chunk_text, generate_embeddings, store_embeddings
from db.crud import update_job_status

def process_chunking_and_embedding(json_blob_name: str, metadata: dict):
    """
    Download processed JSON from Blob, chunk text, generate embeddings,
    store in MosaicDB, and update job status — all in memory.
    """
    job_id = metadata.get("job_id", "unknown")
    document_id = metadata.get("document_id", job_id)

    try:
        logging.info(f"[ChunkEmbed] Starting for blob '{json_blob_name}' (Job ID: {job_id})")

        # Download processed JSON (already contains combined text)
        blob_client = BlobClient(
            account_url=AZURE_STORAGE_ACCOUNT_URL,
            container_name=AZURE_STORAGE_CONTAINER_NAME,
            blob_name=json_blob_name
        )
        json_str = blob_client.download_blob().readall().decode("utf-8")
        data = json.loads(json_str)

        if "text" not in data:
            raise ValueError("Processed JSON missing 'text' key.")

        # Chunk text
        chunks = chunk_text(data["text"])

        # Generate embeddings
        embeddings = generate_embeddings(chunks)

        # Store in MosaicDB
        store_embeddings(document_id=document_id, chunks=chunks, vectors=embeddings, metadata=metadata)

        update_job_status(job_id, status="COMPLETED")
        logging.info(f"[ChunkEmbed] Job {job_id} completed successfully.")

    except Exception as e:
        logging.error(f"[ChunkEmbed] Error in chunk/embed for job {job_id}: {e}", exc_info=True)
        update_job_status(job_id, status="FAILED", error_message=str(e))
