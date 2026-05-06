"""payer_simulation/payer_engine.py — Mock payer adjudication engine."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import structlog

from src.payer_simulation.eligibility_checker import check_eligibility

log = structlog.get_logger()
_FEE_SCHEDULE_PATH = Path("src/payer_simulation/fee_schedule.json")
_fee_schedule: dict[str, float] | None = None

# Procedures that always require prior authorization regardless of billed amount
_AUTH_REQUIRED_CPTS: frozenset[str] = frozenset({"93458", "27447"})
# Claims billed above this threshold also require prior auth
_AUTH_REQUIRED_AMOUNT: float = 1000.0

# Non-covered benefit exclusions (cosmetic / elective / plan exclusion)
_NON_COVERED_CPTS: frozenset[str] = frozenset(
    {
        "15820",
        "15821",
        "15822",
        "15823",  # blepharoplasty
        "17340",  # cryotherapy cosmetic
        "21120",
        "21121",  # genioplasty cosmetic
        "11950",
        "11951",
        "11952",  # soft-tissue injection cosmetic
    }
)

# Procedures classified as investigational / experimental by the plan
_EXPERIMENTAL_CPTS: frozenset[str] = frozenset(
    {
        "0191T",
        "0195T",
        "0301T",
    }
)

# Claims filed more than this many days after DOS are rejected for timely filing
_TIMELY_FILING_DAYS: int = 180

# Denial reason code → CAS adjustment code
_DENIAL_CAS: dict[str, str] = {
    "prior_auth_required": "CO-197",
    "service_not_covered": "CO-49",
    "timely_filing_exceeded": "CO-29",
    "experimental_procedure": "CO-50",
    "coverage_not_active_on_dos": "OA-23",
    "member_not_found": "OA-23",
}


def _get_fee_schedule() -> dict[str, float]:
    global _fee_schedule
    if _fee_schedule is None:
        _fee_schedule = json.loads(_FEE_SCHEDULE_PATH.read_text())
    return _fee_schedule


def _denied(
    claim_id: str,
    reason_code: str,
    billed: float,
    member_id: str,
    dos: str,
    procedures: list,
) -> dict:
    cas_code = _DENIAL_CAS.get(reason_code, "CO-97")
    log.warning("claim_denied", claim_id=claim_id, reason=reason_code, billed=billed)
    return {
        "claim_id": claim_id,
        "status": "denied",
        "denial_reason": reason_code,
        "paid_amount": 0.0,
        "billed_amount": billed,
        "cas_code": cas_code,
        "cas_amount": billed,
        "member_id": member_id,
        "date_of_service": dos,
        "procedures": procedures,
    }


def adjudicate_claim(fhir_claim: dict) -> dict:
    """Adjudicate a FHIR R4 Claim. Returns Contract F dict (10 keys)."""
    claim_id = fhir_claim.get("id", "UNKNOWN")
    billed = float(fhir_claim.get("total", {}).get("value", 0.0))
    patient_ref = fhir_claim.get("patient", {}).get("reference", "")
    member_id = patient_ref.split("/")[-1] if "/" in patient_ref else patient_ref
    items = fhir_claim.get("item", [])
    dos_str = items[0].get("servicedDate", "") if items else ""
    try:
        dos = date.fromisoformat(dos_str) if dos_str else date.today()
    except ValueError:
        dos = date.today()

    fs = _get_fee_schedule()
    procedures = []
    for item in items:
        for coding in item.get("productOrService", {}).get("coding", []):
            cpt = coding.get("code", "")
            if cpt:
                procedures.append({"cpt": cpt, "allowed": fs.get(cpt, fs.get("DEFAULT", 150.0))})
    cpts = {p["cpt"] for p in procedures}

    # ── 1. Member eligibility ─────────────────────────────────────────────────
    elig = check_eligibility(member_id, dos)
    if not elig["eligible"]:
        return _denied(claim_id, elig["reason"], billed, member_id, str(dos), procedures)

    # ── 2. Timely filing ──────────────────────────────────────────────────────
    if (date.today() - dos).days > _TIMELY_FILING_DAYS:
        return _denied(claim_id, "timely_filing_exceeded", billed, member_id, str(dos), procedures)

    # ── 3. Non-covered benefit exclusion ─────────────────────────────────────
    if cpts & _NON_COVERED_CPTS:
        return _denied(claim_id, "service_not_covered", billed, member_id, str(dos), procedures)

    # ── 4. Experimental / investigational ────────────────────────────────────
    if cpts & _EXPERIMENTAL_CPTS:
        return _denied(claim_id, "experimental_procedure", billed, member_id, str(dos), procedures)

    # ── 5. Prior authorization ────────────────────────────────────────────────
    prior_auth = fhir_claim.get("_prior_auth") or ""
    needs_auth = bool(cpts & _AUTH_REQUIRED_CPTS) or billed > _AUTH_REQUIRED_AMOUNT
    if needs_auth and not prior_auth:
        return _denied(claim_id, "prior_auth_required", billed, member_id, str(dos), procedures)

    # ── 6. Pay at fee schedule ────────────────────────────────────────────────
    paid = round(min(sum(p["allowed"] for p in procedures), billed), 2)
    cas = round(billed - paid, 2)
    log.info("claim_paid", claim_id=claim_id, paid=paid, billed=billed)
    return {
        "claim_id": claim_id,
        "status": "paid",
        "denial_reason": "",
        "paid_amount": paid,
        "billed_amount": billed,
        "cas_code": "CO-45",
        "cas_amount": cas,
        "member_id": member_id,
        "date_of_service": str(dos),
        "procedures": procedures,
    }
