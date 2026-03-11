"""src/payer_simulation/eligibility_checker.py — Member eligibility checker.

Loads the member store from the 834 enrollment EDI file via parse_834().
SIMULATION ONLY — driven by mock enrollment data for local testing.
"""

from __future__ import annotations

import os
from datetime import date

import structlog
from dotenv import load_dotenv

from src.payer_simulation.enrollment_parser import parse_834

load_dotenv()
_load_dotenv_sim = __import__("dotenv").load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), "..", "simulation.config"),
    override=False,
)

log = structlog.get_logger()

_MEMBER_STORE = None


def _get_member_store() -> dict[str, dict]:
    global _MEMBER_STORE
    if _MEMBER_STORE is None:
        path = os.getenv(
            "ENROLLMENT_834_PATH",
            "sample_data/834/mock_enrollment_001.edi",
        )
        _MEMBER_STORE = parse_834(path)
    return _MEMBER_STORE


def check_eligibility(member_id: str, date_of_service: date) -> dict:
    """Check member eligibility for a DOS against the 834-derived member store.

    Args:
        member_id:       Member identifier (e.g. MCKMEMBR0001).
        date_of_service: Date to check coverage against.

    Returns:
        dict with keys: eligible (bool), reason (str), plan_type (str),
                        payer_id (str), group_number (str).
    """
    if member_id not in _get_member_store():
        log.warning("member_not_found", member_id=member_id)
        return {
            "eligible": False,
            "reason": "member_not_found",
            "plan_type": "",
            "payer_id": "",
            "group_number": "",
        }
    m = _get_member_store()[member_id]
    start = m["coverage_start"]
    end = m["coverage_end"]
    if not (start <= date_of_service <= end):
        log.warning("coverage_not_active", member_id=member_id, dos=str(date_of_service))
        return {
            "eligible": False,
            "reason": "coverage_not_active_on_dos",
            "plan_type": m.get("plan_type", ""),
            "payer_id": m.get("payer_id", ""),
            "group_number": m.get("group_number", ""),
        }
    log.info("member_eligible", member_id=member_id)
    return {
        "eligible": True,
        "reason": "active_coverage",
        "plan_type": m.get("plan_type", ""),
        "payer_id": m.get("payer_id", ""),
        "group_number": m.get("group_number", ""),
    }
