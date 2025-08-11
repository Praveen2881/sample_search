# main.py
"""
FastAPI entrypoint for document uploads and search.
Uploads document to Azure Blob Storage, creates a job record in Postgres.
Event Grid will trigger the Diverter Function for further routing.
"""

import os
import uuid
from fastapi import FastAPI, UploadFile, HTTPException, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import crud
from db.session import SessionLocal
from utils import blob_utils, logging_utils

logger = logging_utils.get_logger(__name__)

app = FastAPI(title="Enterprise Search API")

# Dependency for getting DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/upload")
async def upload_document(file: UploadFile, db: Session = Depends(get_db)):
    """
    Uploads a document to blob storage and creates a job record.
    """
    try:
        file_ext = os.path.splitext(file.filename)[1].lower()
        job_id = crud.create_job_status(
            db=db,
            filename=file.filename,
            status="PENDING",
            stage="UPLOAD",
        )

        # Generate unique blob path
        unique_name = f"{uuid.uuid4()}{file_ext}"
        blob_path = f"raw/{unique_name}"

        # Upload to blob storage
        content = await file.read()
        blob_utils.upload_blob(blob_path, content)

        logger.info(f"Uploaded file {file.filename} to {blob_path}, job {job_id}")

        # Update job with blob path
        crud.update_job_status(db, job_id, blob_path=blob_path)

        return JSONResponse(
            {
                "message": "File uploaded successfully",
                "job_id": job_id,
                "blob_path": blob_path,
            }
        )

    except Exception as e:
        logger.exception("Error uploading file")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job/{job_id}")
def get_job_status(job_id: int, db: Session = Depends(get_db)):
    """
    Returns the current status of a document processing job.
    """
    job = crud.get_job_status(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job.id,
        "filename": job.filename,
        "status": job.status,
        "stage": job.stage,
        "error_message": job.error_message,
        "blob_path": job.blob_path,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


@app.get("/search")
def search_documents(query: str, filters: dict = None):
    """
    Placeholder search API — in real usage, will query MosaicDB
    with vector search + ACL filtering.
    """
    # TODO: Implement hybrid + vector search
    return {"query": query, "filters": filters, "results": []}
