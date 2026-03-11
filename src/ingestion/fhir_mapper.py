"""FHIR R4 Claim resource builder: converts parse_837() output into FHIR R4 Claim dicts."""

from __future__ import annotations

import os

import structlog
from dotenv import load_dotenv

load_dotenv()
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), "..", "simulation.config"),
    override=False,
)

log = structlog.get_logger()

# System URIs — FHIR R4 standard identifiers
NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
ICD10_SYSTEM = "http://hl7.org/fhir/sid/icd-10-cm"
CPT_SYSTEM = "http://www.ama-assn.org/go/cpt"
CLAIM_TYPE_SYS = "http://terminology.hl7.org/CodeSystem/claim-type"


def _dos_to_iso(dos: str) -> str:
    """Convert CCYYMMDD (EDI date) to ISO YYYY-MM-DD for FHIR created field."""
    if len(dos) == 8:
        return f"{dos[:4]}-{dos[4:6]}-{dos[6:8]}"
    return dos  # Already ISO or empty string


def unwrap_fhir(data: dict) -> list[dict]:
    """
    Extract Claim resources from a FHIR R4 Bundle or a bare Claim.

    Args:
        data: Parsed FHIR JSON dict — either a Bundle or a single Claim.

    Returns:
        list[dict]: Zero or more FHIR Claim resource dicts.
                    Bundle  → all entries whose resource.resourceType == 'Claim'
                    Claim   → [data]
                    Other   → []
    """
    if data.get("resourceType") == "Bundle":
        return [
            e["resource"]
            for e in data.get("entry", [])
            if e.get("resource", {}).get("resourceType") == "Claim"
        ]
    elif data.get("resourceType") == "Claim":
        return [data]
    return []


def map_to_fhir_claim(parsed_claim: dict) -> dict:
    """
    Build a FHIR R4 Claim resource dict from a parse_837() output dict.

    Args:
        parsed_claim: Output of parse_837(). Must match Contract A.

    Returns:
        dict: FHIR R4 Claim resource. Matches Contract B.
              Serializable to JSON with json.dumps().
    """
    dos_iso = _dos_to_iso(parsed_claim.get("date_of_service", ""))

    # Build diagnosis array — FHIR sequence starts at 1
    diagnoses = [
        {
            "sequence": i + 1,
            "diagnosisCodeableConcept": {"coding": [{"system": ICD10_SYSTEM, "code": code}]},
        }
        for i, code in enumerate(parsed_claim.get("diagnosis_codes", []))
    ]

    # Build item array — each procedure code becomes one line item
    billed = parsed_claim.get("billed_amount", 0.0)
    items = [
        {
            "sequence": i + 1,
            "productOrService": {"coding": [{"system": CPT_SYSTEM, "code": code}]},
            "servicedDate": dos_iso,
            "unitPrice": {"value": billed, "currency": "USD"},
            "net": {"value": billed, "currency": "USD"},
        }
        for i, code in enumerate(parsed_claim.get("procedure_codes", []))
    ]

    fhir_claim = {
        "resourceType": "Claim",
        "id": parsed_claim["transaction_id"],
        "status": "active",
        "type": {"coding": [{"system": CLAIM_TYPE_SYS, "code": "professional"}]},
        "use": "claim",
        "patient": {"reference": f"Patient/{parsed_claim['patient_member_id']}"},
        "created": dos_iso,
        "insurer": {"identifier": {"value": os.getenv("PAYER_ID")}},
        "provider": {
            "identifier": {
                "system": NPI_SYSTEM,
                "value": parsed_claim["billing_npi"],
            }
        },
        "priority": {"coding": [{"code": "normal"}]},
        "diagnosis": diagnoses,
        "item": items,
        "total": {"value": billed, "currency": "USD"},
        # Private fields — carry non-FHIR metadata for downstream modules
        "_denial_risk_score": None,
        "_prior_auth": parsed_claim.get("prior_auth_number"),
        "_rendering_npi": parsed_claim.get("rendering_npi", ""),
    }
    log.info(
        "fhir_mapped",
        claim_id=fhir_claim["id"],
        diagnoses=len(diagnoses),
        items=len(items),
        billed=billed,
    )
    return fhir_claim


def validate_fhir_claim(claim: dict) -> list[str]:
    """
    Check for missing required FHIR R4 Claim fields.
    Used in validator.py after mapping — not in the mapping itself.

    Returns:
        list[str]: List of missing field names. Empty list = valid.
    """
    required = [
        "resourceType",
        "id",
        "status",
        "type",
        "use",
        "patient",
        "created",
        "insurer",
        "provider",
        "priority",
        "diagnosis",
        "item",
        "total",
    ]
    return [f for f in required if not claim.get(f)]
