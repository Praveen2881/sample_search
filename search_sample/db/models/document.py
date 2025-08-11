# db/models/document.py
"""
Document model - one row per blob-uploaded document.
Holds pointer to Azure Blob (container/path) and ingestion metadata.
"""

import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, JSON, BigInteger, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from ..base import Base
from sqlalchemy.orm import relationship

class Document(Base):
    __tablename__ = "documents"

    document_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # store as "<container>/<path/to/blob>" or full url if you prefer
    blob_path = Column(Text, nullable=False, index=True)
    file_name = Column(String(length=1024), nullable=False)
    team_id = Column(PG_UUID(as_uuid=True), ForeignKey("teams.team_id", ondelete="SET NULL"), nullable=True, index=True)
    checksum = Column(String(length=128), nullable=True, index=True)  # sha256 hex
    uploaded_by = Column(String(length=255), nullable=True)
    uploaded_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    size_bytes = Column(BigInteger, nullable=True)
    # metadata stores arbitrary JSON such as content type, language, processed blob pointer, page counts, etc.
    metadata = Column(JSON, nullable=True, default={})

    job_statuses = relationship("JobStatus", back_populates="document", lazy="selectin", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Document(id={self.document_id}, file_name={self.file_name})>"
