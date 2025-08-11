# functions/diverter_function.py
"""
Document Diverter Function
--------------------------
Simulates an Azure Function triggered when a new document is uploaded to Blob Storage.
Routes documents based on extension to the correct processing queue.
Updates job status in Postgres.
"""

import os
import mimetypes
from pathlib import Path

from utils import servicebus_utils, logging_utils
from db import crud
from db.session import SessionLocal

logger = logging_utils.get_logger(__name__)

# Supported extensions mapping to queue names
EXTENSION_QUEUE_MAP = {
    ".pdf": "pdf-processing-queue",
    ".docx": "docx-processing-queue",
    ".pptx": "pptx-processing-queue",
    ".xlsx": "excel-processing-queue",
    # Future: audio/video
}

def route_document(blob_path: str, job_id: int):
    """
    Routes a document to the appropriate Service Bus queue based on its extension.

    Args:
        blob_path (str): Path to the document in blob storage.
        job_id (int): Job status record ID in Postgres.
    """
    logger.info(f"Routing document {blob_path} for job {job_id}")

    ext = Path(blob_path).suffix.lower()
    queue_name = EXTENSION_QUEUE_MAP.get(ext)

    db = SessionLocal()

    try:
        if not queue_name:
            logger.error(f"No processor for extension: {ext}")
            crud.update_job_status(
                db=db,
                job_id=job_id,
                status="FAILED",
                stage="ROUTING",
                error_message=f"Unsupported file type: {ext}",
            )
            return

        # Update status: ROUTING -> QUEUED
        crud.update_job_status(
            db=db,
            job_id=job_id,
            status="QUEUED",
            stage="ROUTING",
        )

        # Send message to Service Bus queue
        message = {
            "job_id": job_id,
            "blob_path": blob_path,
            "extension": ext,
        }
        servicebus_utils.send_message(queue_name, message)

        logger.info(f"Document {blob_path} queued to {queue_name}")

    except Exception as e:
        logger.exception("Error routing document")
        crud.update_job_status(
            db=db,
            job_id=job_id,
            status="FAILED",
            stage="ROUTING",
            error_message=str(e),
        )

    finally:
        db.close()

if __name__ == "__main__":
    # Local test
    route_document("docs/sample.pdf", job_id=1)
