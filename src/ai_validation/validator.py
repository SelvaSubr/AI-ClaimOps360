"""ai_validation/validator.py
NPI Luhn algorithm + FHIR Claim completeness checks.
Called by denial_risk_scorer.py before scoring.
"""

from __future__ import annotations

import structlog

log = structlog.get_logger()

REQUIRED_FHIR_FIELDS: list[str] = [
    "resourceType",
    "id",
    "status",
    "patient",
    "provider",
    "diagnosis",
    "item",
    "total",
]


def _luhn_check(npi: str) -> bool:
    """Validate 10-digit NPI using Luhn algorithm with 80840 prefix."""
    if not npi or not npi.isdigit() or len(npi) != 10:
        return False
    full = "80840" + npi
    total = 0
    for i, ch in enumerate(reversed(full)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def validate_npi(npi: str) -> bool:
    valid = _luhn_check(npi)
    if not valid:
        log.warning("invalid_npi", npi=npi)
    return valid


def validate_fhir_claim(fhir_claim: dict) -> tuple[bool, list[str]]:
    """Check FHIR Claim for required fields. Returns (is_valid, errors)."""
    errors: list[str] = []
    for field in REQUIRED_FHIR_FIELDS:
        if field not in fhir_claim or not fhir_claim[field]:
            errors.append(f"missing_required_field:{field}")
    if fhir_claim.get("resourceType") != "Claim":
        errors.append("wrong_resource_type")
    if not fhir_claim.get("diagnosis"):
        errors.append("empty_diagnosis_array")
    if not fhir_claim.get("item"):
        errors.append("empty_item_array")
    total = fhir_claim.get("total", {})
    if not isinstance(total.get("value"), (int, float)) or total.get("value", 0) <= 0:
        errors.append("invalid_total_value")
    npi = fhir_claim.get("provider", {}).get("identifier", {}).get("value", "")
    if npi and not validate_npi(npi):
        errors.append(f"invalid_billing_npi:{npi}")
    is_valid = len(errors) == 0
    log.info("fhir_validation", claim_id=fhir_claim.get("id"), valid=is_valid, errors=errors)
    return is_valid, errors
