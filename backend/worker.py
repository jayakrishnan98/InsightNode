"""
Background worker — drains the ingest queue and batch-writes to PostgreSQL.

Runs in a separate thread (started from backend.main lifespan). Batching reduces
commit overhead; failed batches are re-queued with a retry cap to handle transient DB errors.
"""

import logging
import threading
from queue import Empty, Full, Queue

from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import MetricRecord

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_TIMEOUT_SECONDS = 1.0
MAX_BATCH_RETRIES = 3


def _payload_to_rows(payload: dict) -> list[MetricRecord]:
  """
  Expand one agent payload into one MetricRecord per metric in the payload.

  Logic:
      - Read machine_id and timestamp once from the payload envelope.
      - For each entry in payload["metrics"], create a MetricRecord row.

  Reason:
      Agents send batched payloads (3 metrics per POST); the database stores one
      row per metric for simpler filtering and indexing. Internal fields like
      _retry_count are ignored because they are not part of the metric schema.
  """
  return [
    MetricRecord(
      machine_id=payload["machine_id"],
      metric_name=metric["name"],
      value=metric["value"],
      unit=metric["unit"],
      timestamp=payload["timestamp"],
    )
    for metric in payload["metrics"]
  ]


def _flush_batch(db: Session, batch: list[dict]) -> None:
  """
  Insert all rows from a batch of payloads in a single database transaction.

  Logic:
      - Flatten each payload to ORM rows via _payload_to_rows.
      - db.add_all(rows) then db.commit() once for the whole batch.

  Reason:
      One commit per batch amortizes round-trip cost to PostgreSQL. Separate
      commits per payload would not scale under high ingest rates.
  """
  rows = []
  for payload in batch:
    rows.extend(_payload_to_rows(payload))

  db.add_all(rows)
  db.commit()
  logger.info("Persisted %s metric rows from %s payloads", len(rows), len(batch))


def run_ingest_worker(ingest_queue: Queue, stop_event: threading.Event) -> None:
  """
  Long-running consumer loop: dequeue payloads, batch, persist, handle failures.

  Logic:
      1. Wait up to BATCH_TIMEOUT_SECONDS for the first queue item.
      2. Drain up to BATCH_SIZE items total (non-blocking for extras).
      3. _flush_batch() — write all rows in one transaction.
      4. On DB error: rollback, re-queue each payload if _retry_count < MAX_BATCH_RETRIES.
      5. On shutdown (stop_event): exit only when the queue times out empty.
      6. Always call task_done() per item and close the DB session.

  Reason:
      Decouples HTTP accept from DB write speed. Batching + timeout balances
      latency (flush partial batches after 1s) vs throughput (up to 50 payloads).
      Re-queue handles transient DB outages; max retries prevents infinite loops.
      Tradeoff: in-memory queue is lost on process crash — Phase 2 adds Kafka/Redis.
  """
  while True:
    batch: list[dict] = []

    # Collect up to BATCH_SIZE items, or wait BATCH_TIMEOUT_SECONDS
    try:
      first = ingest_queue.get(timeout=BATCH_TIMEOUT_SECONDS)
      batch.append(first)
    except Empty:
      if stop_event.is_set():
        break
      continue

    while len(batch) < BATCH_SIZE:
      try:
        batch.append(ingest_queue.get_nowait())
      except Empty:
        break

    db = SessionLocal()
    try:
      _flush_batch(db, batch)
    except Exception:
      logger.exception("Failed to persist batch of %s payloads", len(batch))

      db.rollback()

      for payload in batch:
        retries = payload.get("_retry_count", 0)
        if retries < MAX_BATCH_RETRIES:
          payload["_retry_count"] = retries + 1
          try:
            ingest_queue.put(payload, block=False)
            logger.warning(
              "Re-queued payload machine=%s retry=%s",
              payload.get("machine_id"),
              payload["_retry_count"],
            )
          except Full:
            logger.error(
              "Queue full — dropping payload machine=%s after DB failure",
              payload.get("machine_id"),
            )
        else:
          logger.error(
            "Dropping payload machine=%s after %s failed attempts",
            payload.get("machine_id"),
            MAX_BATCH_RETRIES,
          )
    finally:
      db.close()
      for _ in batch:
        ingest_queue.task_done()