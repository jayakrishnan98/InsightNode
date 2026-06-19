"""
Database connection setup for the InsightNode API.

Uses SQLAlchemy with a sync PostgreSQL driver. One engine per process; each
request or worker batch opens a short-lived Session.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://insightnode:insightnode@localhost:5432/insightnode",
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base — all ORM models inherit from this."""
    pass


def get_db():
    """
    FastAPI dependency that yields a database session per HTTP request.

    Logic:
        - Create a SessionLocal() at the start of the request.
        - yield it to the route handler.
        - Close the session in finally (whether the request succeeded or failed).

    Reason:
        Sessions must not leak connections. The yield/finally pattern ensures
        every GET /metrics request gets an isolated session that is always closed.
        The ingest worker uses SessionLocal() directly instead of this dependency.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()