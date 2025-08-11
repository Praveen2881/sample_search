# db/models/job_status.py
"""
JobStatus model tracks per-document, per-stage progress and error messages.

Stages are free text (e.g., 'ingest', 'extraction', 'chunking', 'embedding', 'indexing').
Status is an Enum to make queries simple.
"""

import uuid
import enum
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Enum, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from ..base import Base
from sqlalchemy.orm import relationship

class JobStatusEnum(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    indexing_completed = "indexing_completed"
    error = "error"

class JobStatus(Base):
    __tablename__ = "job_status"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(PG_UUID(as_uuid=True), ForeignKey("documents.document_id", ondelete="CASCADE"), nullable=False, index=True)
    stage = Column(String(length=128), nullable=False, index=True)
    status = Column(Enum(JobStatusEnum, name="job_status_enum"), nullable=False, default=JobStatusEnum.pending)
    # Capture a short or structured message; consider storing trace_id/stack in production
    message = Column(Text, nullable=True)
    last_updated = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # relationship back to Document
    document = relationship("Document", back_populates="job_statuses", lazy="joined")

    def __repr__(self) -> str:
        return f"<JobStatus(id={self.id}, doc={self.document_id}, stage={self.stage}, status={self.status})>"
