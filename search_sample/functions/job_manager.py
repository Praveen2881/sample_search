# functions/job_manager.py
"""
Job Manager (Event Grid -> Service Bus)
- Parses Event Grid blob-created events
- Creates document record in Postgres
- Creates initial job_status entries
- Enqueues a message to a Service Bus queue (per extension)
"""

import json
import os
from urllib.parse import urlparse, unquote
from pathlib import Path
from typing import Dict, Optional

from db.session import SessionLocal
from db import crud
from utils.logging_utils import get_logger
from utils import servicebus_utils
from config import AZURE_SERVICE_BUS_CONNECTION_STRING

logger = get_logger(__name__)

# map extension -> queue name (adjust names to match your system)
EXT_TO_QUEUE = {
    ".pdf": "pdf-processing-queue",
    ".docx": "docx-processing-queue",
    ".pptx": "pptx-processing-queue",
    ".xlsx": "excel-processing-queue",
    # add more as needed
}

# Helper functions ----------------------------------------------------------

def parse_eventgrid_event(body: Dict) -> Optional[Dict]:
    """
    Parse an Event Grid event payload (single event or array) and extract blob info.
    Supports the common 'Microsoft.Storage.BlobCreated' event shape.

    Returns dict with:
      - container
      - blob_path (path within container)
      - url (full blob url)
      - file_name
    or None if parsing fails / unsupported event.
    """
    # Event Grid can deliver a list of events or a single event depending on wiring.
    event = None
    if isinstance(body, list) and len(body) > 0:
        event = body[0]
    elif isinstance(body, dict):
        # Some Event Grid endpoints post {"data": {...}} directly
        if "value" in body and isinstance(body["value"], list) and len(body["value"]) > 0:
            event = body["value"][0]
        else:
            event = body
    else:
        return None

    # Many Event Grid blob events include data.url (full blob url)
    data = event.get("data") or {}
    blob_url = data.get("url") or data.get("api") or None

    # Some integrations put the blob path in 'subject' ("/blobServices/default/containers/{container}/blobs/{blob}")
    subject = event.get("subject")

    if blob_url:
        # e.g., https://<account>.blob.core.windows.net/container/path/to/blob.pdf
        parsed = urlparse(blob_url)
        path = parsed.path.lstrip("/")  # container/path/to/blob.pdf
        parts = path.split("/", 1)
        if len(parts) == 2:
            container, blob_path = parts[0], parts[1]
        else:
            # fallback: if no container separation, treat entire path as blob
            container, blob_path = "default", parts[0]
        file_name = Path(unquote(blob_path)).name
        return {"container": container, "blob_path": blob_path, "url": blob_url, "file_name": file_name}

    if subject:
        # parse subject to find container and blob name
        # subject example: "/blobServices/default/containers/mycontainer/blobs/path%2Fto%2Ffile.pdf"
        try:
            parts = subject.split("/containers/", 1)[1]
            container, blob_part = parts.split("/blobs/", 1)
            blob_path = unquote(blob_part)
            file_name = Path(blob_path).name
            return {"container": container, "blob_path": blob_path, "url": None, "file_name": file_name}
        except Exception:
            return None

    return None


def extension_to_queue(ext: str) -> Optional[str]:
    return EXT_TO_QUEUE.get(ext.lower())


# Core logic ---------------------------------------------------------------

def create_doc_and_enqueue(container: str, blob_path: str, team_id: Optional[str] = None, uploaded_by: Optional[str] = None, size_bytes: Optional[int] = None, metadata: Optional[dict] = None) -> Dict:
    """
    Create Document DB record and enqueue a message to service bus for processing.
    Returns a dict containing document_id, queued_to (queue name), and job_status info.
    """
    db = SessionLocal()
    try:
        full_blob_path = f"{container}/{blob_path}"
        # Create document record
        doc = crud.create_document(
            db=db,
            blob_path=full_blob_path,
            file_name=Path(blob_path).name,
            team_id=team_id,
            uploaded_by=uploaded_by,
            size_bytes=size_bytes,
            metadata=metadata or {},
            checksum=None,
        )
        logger.info(f"Document created: id={doc.document_id}, blob={full_blob_path}")

        # Create initial job status (ingest)
        crud.create_job_status(
            db=db,
            document_id=doc.document_id,
            stage="ingest",
            status=crud.JobStatusEnum.pending,
            message="Ingest created"
        )

        # Determine queue by file extension
        ext = Path(blob_path).suffix.lower()
        queue = extension_to_queue(ext)
        if not queue:
            # Mark routing error
            err_msg = f"No queue configured for extension '{ext}'"
            logger.error(err_msg)
            crud.create_job_status(
                db=db,
                document_id=doc.document_id,
                stage="routing",
                status=crud.JobStatusEnum.error,
                message=err_msg
            )
            return {"document_id": str(doc.document_id), "error": err_msg}

        # Enqueue message to service bus
        message = {
            "document_id": str(doc.document_id),
            "container": container,
            "blob_path": blob_path,
            "file_name": doc.file_name,
            "team_id": team_id,
        }

        # best-effort: create routing job status -> queued
        crud.create_job_status(
            db=db,
            document_id=doc.document_id,
            stage="routing",
            status=crud.JobStatusEnum.processing,
            message=f"Routing to {queue}"
        )

        # send message (servicebus_utils handles stub or real azure)
        servicebus_utils.send_message(queue, message)
        logger.info(f"Enqueued document {doc.document_id} to queue {queue}")

        crud.create_job_status(
            db=db,
            document_id=doc.document_id,
            stage="routing",
            status=crud.JobStatusEnum.completed,
            message=f"Queued to {queue}"
        )

        return {"document_id": str(doc.document_id), "queued_to": queue}
    except Exception as e:
        logger.exception("Failed to create document and enqueue")
        # attempt to write a job_status error if possible
        try:
            crud.create_job_status(db=db, document_id=None, stage="ingest", status=crud.JobStatusEnum.error, message=str(e))
        except Exception:
            logger.exception("Failed to write fallback job_status")
        raise
    finally:
        db.close()


# HTTP handler (usable as Azure Function HTTP trigger) ---------------------

def http_eventgrid_handler(request_body: dict) -> dict:
    """
    Accepts the incoming Event Grid request body (dict).
    This function parses the event, creates DB record, and enqueues to service bus.
    Returns a dictionary that can be used as a JSON HTTP response.
    """

    parsed = parse_eventgrid_event(request_body)
    if not parsed:
        logger.error("Could not parse Event Grid event body")
        return {"status": "error", "message": "Could not parse Event Grid event body"}

    container = parsed["container"]
    blob_path = parsed["blob_path"]
    file_name = parsed["file_name"]

    # optional: you might parse additional metadata from event or blob metadata
    # For now, no team info; you can enrich message later
    result = create_doc_and_enqueue(container=container, blob_path=blob_path, team_id=None, uploaded_by=None)
    return {"status": "ok", "result": result}



