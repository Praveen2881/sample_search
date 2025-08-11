# db/session.py
"""
Database session and engine configuration.
Uses synchronous SQLAlchemy ORM by default.
"""

import os
from config import DATABASE_URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Example: postgresql+psycopg2://user:password@host:port/dbname
# DATABASE_URL = os.getenv(
#     "DATABASE_URL",
#     "postgresql+psycopg2://postgres:postgres@localhost:5432/enterprise_search"
# )

# Engine creation
engine = create_engine(
    DATABASE_URL,
    echo=False,            # Set to True for SQL debug logging
    pool_pre_ping=True,    # Check connections before using
    pool_size=10,
    max_overflow=20
)

# SessionLocal is the session factory to be used in app
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Dependency for FastAPI or any app
def get_db():
    """
    Yields a new database session.
    Ensures session is closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
