"""src/payer_simulation/enrollment_parser.py — X12 834 benefit enrollment parser.

SIMULATION ONLY — Parses mock 834 EDI files for local testing.
Not affiliated with any real payer or enrollment system.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import structlog
from dotenv import load_dotenv

load_dotenv()
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), "..", "simulation.config"),
    override=False,
)

log = structlog.get_logger()

# Plan code substrings → canonical plan type labels
_PLAN_TYPE_MAP = {
    "PPO": "PPO",
    "HMO": "HMO",
    "EPO": "EPO",
    "POS": "POS",
}


def _parse_plan_type(plan_code: str) -> str:
    """Infer plan type label from HD segment plan code string."""
    upper = plan_code.upper()
    for key, label in _PLAN_TYPE_MAP.items():
        if key in upper:
            return label
    return plan_code or "UNKNOWN"


def _parse_date(date_str: str) -> date:
    """Parse EDI date string CCYYMMDD → date object."""
    try:
        return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except (ValueError, IndexError):
        log.warning("834_date_parse_failed", value=date_str)
        return date.today()


def _finalize(current: dict, payer_id: str) -> dict:
    """Fill defaults for any missing fields and return a completed member record."""
    return {
        "coverage_start": current.get("coverage_start", date.today()),
        "coverage_end": current.get("coverage_end", date(9999, 12, 31)),
        "plan_type": current.get("plan_type", "UNKNOWN"),
        "payer_id": payer_id,
        "group_number": current.get("group_number", ""),
    }


def parse_834(file_path: str | None = None) -> dict[str, dict]:
    """Parse an X12 834 benefit enrollment file into a member store dict.

    Reads the 834 EDI file and extracts one entry per member (INS loop).
    Payer ID is sourced from the N1*IN segment in the file, so the 834 file
    is the single source of truth for membership and payer identity.

    Args:
        file_path: Path to the 834 EDI file. Defaults to the
                   ENROLLMENT_834_PATH env var, then the built-in mock path.

    Returns:
        dict[str, dict]: Keyed by member_id; each value has:
            coverage_start (date), coverage_end (date),
            plan_type (str), payer_id (str), group_number (str).

    Raises:
        FileNotFoundError: if the resolved file path does not exist.
    """
    if file_path is None:
        file_path = os.getenv("ENROLLMENT_834_PATH")

    content = Path(file_path).read_text()

    # Normalize: strip newlines, split on segment terminator ~
    segments = [
        s.strip() for s in content.replace("\n", "").replace("\r", "").split("~") if s.strip()
    ]

    # Extract payer ID from the N1*IN loop (payer sender)
    payer_id = ""
    for seg in segments:
        parts = seg.split("*")
        if parts[0] == "N1" and len(parts) > 4 and parts[1] == "IN":
            payer_id = parts[4].strip()
            break

    member_store: dict[str, dict] = {}
    current: dict = {}

    for seg in segments:
        parts = seg.split("*")
        tag = parts[0]

        if tag == "INS":
            # New member loop — save previous member if complete
            if current.get("member_id"):
                member_store[current["member_id"]] = _finalize(current, payer_id)
            current = {}

        elif tag == "REF" and len(parts) > 2:
            qualifier = parts[1]
            value = parts[2].strip()
            if qualifier == "0F":  # subscriber identifier (member ID)
                current["member_id"] = value
            elif qualifier == "1L":  # group/plan number
                current["group_number"] = value

        elif tag == "DTP" and len(parts) > 3:
            qualifier = parts[1]
            value = parts[3].strip()
            if qualifier == "348":  # benefit begin date (coverage start)
                current["coverage_start"] = _parse_date(value)
            elif qualifier == "349":  # benefit termination date (coverage end)
                current["coverage_end"] = _parse_date(value)

        elif tag == "HD" and len(parts) > 4:
            current["plan_type"] = _parse_plan_type(parts[4].strip())

    # Flush the last member record
    if current.get("member_id"):
        member_store[current["member_id"]] = _finalize(current, payer_id)

    log.info("834_parsed", member_count=len(member_store), source=file_path)
    return member_store
