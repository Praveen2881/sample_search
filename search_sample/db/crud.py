# db/crud.py
"""
CRUD operations for Teams, Documents, JobStatuses, and Permissions.

All functions here are synchronous SQLAlchemy ORM by default.
If you're using async SQLAlchemy, adapt session calls accordingly.
"""

import uuid
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import select, update
from datetime import datetime

from .models import Team, Document, JobStatus, JobStatusEnum, Permission


# # ------------------ TEAM ------------------ #

# def create_team(db: Session, name: str, azure_ad_group_id: Optional[str] = None) -> Team:
#     team = Team(name=name, azure_ad_group_id=azure_ad_group_id)
#     db.add(team)
#     db.commit()
#     db.refresh(team)
#     return team

# def get_team_by_id(db: Session, team_id: uuid.UUID) -> Optional[Team]:
#     return db.get(Team, team_id)

# def get_team_by_name(db: Session, name: str) -> Optional[Team]:
#     stmt = select(Team).where(Team.name == name)
#     return db.scalar(stmt)


# ------------------ DOCUMENT ------------------ #

def create_document(
    db: Session,
    blob_path: str,
    file_name: str,
    team_id: Optional[uuid.UUID],
    uploaded_by: Optional[str],
    size_bytes: Optional[int],
    metadata: Optional[dict] = None,
    checksum: Optional[str] = None
) -> Document:
    doc = Document(
        blob_path=blob_path,
        file_name=file_name,
        team_id=team_id,
        uploaded_by=uploaded_by,
        size_bytes=size_bytes,
        metadata=metadata or {},
        checksum=checksum
    )
    db.add(doc)
    db.commit()
    db.refresh(doc)
    return doc

def get_document_by_id(db: Session, document_id: uuid.UUID) -> Optional[Document]:
    return db.get(Document, document_id)

def list_documents_by_team(db: Session, team_id: uuid.UUID) -> List[Document]:
    stmt = select(Document).where(Document.team_id == team_id)
    return list(db.scalars(stmt))


# ------------------ JOB STATUS ------------------ #

def create_job_status(
    db: Session,
    document_id: uuid.UUID,
    stage: str,
    status: JobStatusEnum = JobStatusEnum.pending,
    message: Optional[str] = None
) -> JobStatus:
    job_status = JobStatus(
        document_id=document_id,
        stage=stage,
        status=status,
        message=message
    )
    db.add(job_status)
    db.commit()
    db.refresh(job_status)
    return job_status

def update_job_status(
    db: Session,
    document_id: uuid.UUID,
    stage: str,
    status: JobStatusEnum,
    message: Optional[str] = None
) -> Optional[JobStatus]:
    stmt = (
        update(JobStatus)
        .where(JobStatus.document_id == document_id, JobStatus.stage == stage)
        .values(status=status, message=message, last_updated=datetime.utcnow())
        .returning(JobStatus)
    )
    result = db.execute(stmt)
    db.commit()
    row = result.fetchone()
    return row[0] if row else None

def get_job_statuses_for_document(db: Session, document_id: uuid.UUID) -> List[JobStatus]:
    stmt = select(JobStatus).where(JobStatus.document_id == document_id)
    return list(db.scalars(stmt))

def mark_stage_error(db: Session, document_id: uuid.UUID, stage: str, error_message: str) -> Optional[JobStatus]:
    return update_job_status(db, document_id, stage, JobStatusEnum.error, error_message)

def mark_stage_completed(db: Session, document_id: uuid.UUID, stage: str, message: Optional[str] = None) -> Optional[JobStatus]:
    return update_job_status(db, document_id, stage, JobStatusEnum.completed, message)


# ------------------ PERMISSION ------------------ #

# def add_permission(
#     db: Session,
#     team_id: uuid.UUID,
#     principal_type: str,
#     principal_id: str
# ) -> Permission:
#     perm = Permission(
#         team_id=team_id,
#         principal_type=principal_type,
#         principal_id=principal_id
#     )
#     db.add(perm)
#     db.commit()
#     db.refresh(perm)
#     return perm

# def list_permissions_for_team(db: Session, team_id: uuid.UUID) -> List[Permission]:
#     stmt = select(Permission).where(Permission.team_id == team_id)
#     return list(db.scalars(stmt))

# def check_user_has_permission(db: Session, user_id: str, team_id: uuid.UUID) -> bool:
#     stmt = select(Permission).where(
#         Permission.team_id == team_id,
#         Permission.principal_type == "user",
#         Permission.principal_id == user_id
#     )
#     return db.scalar(stmt) is not None
