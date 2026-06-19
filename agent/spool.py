"""
On-disk buffer for metric payloads the agent could not send to the API.

Uses NDJSON (one JSON object per line) so new failures can be appended cheaply
without rewriting the entire file. Datadog-style agents use similar local spooling
when their intake endpoint is unreachable.
"""

import json
from pathlib import Path

SPOOL_DIR = Path(__file__).resolve().parent / "data"
SPOOL_FILE = SPOOL_DIR / "spool.ndjson"


def _ensure_spool_file() -> None:
    """
    Create the spool directory and empty file if they do not exist yet.

    Logic:
        - mkdir(parents=True) creates agent/data/ if missing.
        - touch() creates an empty spool.ndjson if missing.

    Reason:
        Every public spool function can assume the file exists, avoiding
        duplicated setup logic and FileNotFoundError on first failure.
    """
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    if not SPOOL_FILE.exists():
        SPOOL_FILE.touch()


def append(payload: dict) -> None:
    """
    Persist one failed payload as a new line in the spool file.

    Logic:
        - Ensure spool file exists.
        - Serialize payload to JSON, write one line + newline (append mode).

    Reason:
        Append is O(1) and crash-friendly: each line is an independent record.
        Simpler than maintaining a JSON array that must be read and rewritten
        on every failure.
    """
    _ensure_spool_file()
    with SPOOL_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


def read_all() -> list[dict]:
    """
    Load every buffered payload from disk into memory.

    Logic:
        - Open spool.ndjson, read line by line.
        - Skip blank lines; json.loads() each non-empty line into a dict.
        - Return list in file order (oldest first).

    Reason:
        Replay needs the full backlog in FIFO order. Line-by-line reading keeps
        memory proportional to spool size and handles partial files gracefully.
    """
    _ensure_spool_file()
    payloads: list[dict] = []
    with SPOOL_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                payloads.append(json.loads(line))
    return payloads


def rewrite(payloads: list[dict]) -> None:
    """
    Replace the spool file with only payloads that have not been sent yet.

    Logic:
        - Open spool.ndjson in write mode (truncates existing content).
        - Write each remaining payload as one NDJSON line.

    Reason:
        After replay, successfully sent payloads must be removed from disk.
        Full rewrite is simple for small spools; production systems would use
        offset tracking or segmented files at larger scale.
    """
    _ensure_spool_file()
    with SPOOL_FILE.open("w", encoding="utf-8") as f:
        for payload in payloads:
            f.write(json.dumps(payload) + "\n")


def size() -> int:
    """
    Return how many payloads are currently buffered on disk.

    Logic:
        - Delegate to read_all() and return len().

    Reason:
        Convenience for logging after append (e.g. "total=3 buffered"). Not used
        on the hot path for correctness — replay always reads the actual file.
    """
    return len(read_all())