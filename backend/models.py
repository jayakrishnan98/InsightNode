from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, func
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class MetricRecord(Base):
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    machine_id: Mapped[str] = mapped_column(String(255), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )