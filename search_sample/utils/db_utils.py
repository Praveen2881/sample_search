# utils/db_utils.py
"""
Database utilities for fetching document metadata.
"""

from sqlalchemy.orm import Session
from db.session import get_db
from db import models

def get_document_metadata(document_id: int, db: Session = None) -> dict:
    """
    Fetch metadata for a document from the database.
    """
    # If no session is passed, create one
    close_session = False
    if db is None:
        db = next(get_db())
        close_session = True

    try:
        doc = db.query(models.Document).filter(models.Document.id == document_id).first()
        if not doc:
            return {}

        # Convert to dict (modify to fit your schema)
        return {
            "id": doc.id,
            "title": doc.title,
            "uploaded_by": doc.uploaded_by,
            "uploaded_at": doc.uploaded_at.isoformat() if doc.uploaded_at else None,
            "status": doc.status,
            "tags": doc.tags or []
        }

    finally:
        if close_session:
            db.close()
