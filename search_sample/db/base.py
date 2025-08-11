"""
Declarative base for SQLAlchemy models.
All model classes should import and inherit from `Base` here.
"""

from sqlalchemy.orm import declarative_base

# This Base will be inherited by all models
Base = declarative_base()