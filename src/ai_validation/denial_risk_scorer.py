"""ai_validation/denial_risk_scorer.py — FHIR validation + SHAP denial risk scoring.

CMS: ai_decision_only/reconsideration_right fixed BEFORE inference; human_review_required
is a hard rule (score >= 70) applied AFTER scoring. Never raises — safe fallback on error.
"""

from __future__ import annotations

import structlog

from src.ai_validation.explainability import generate_explanation
from src.ai_validation.validator import validate_fhir_claim

log = structlog.get_logger()


def calculate_denial_risk(fhir_claim: dict) -> dict:
    """Score a FHIR Claim for denial risk. Returns Contract E dict (10 keys).

    CMS mandatory fields returned on every call:
        denial_risk_score      — int(model_probability * 100), range 0-100
        human_review_required  — True if score >= 70 (HARD RULE, not model)
        provider_explanation   — readable English sentence
        ai_decision_only       — ALWAYS False (set before inference)
        reconsideration_right  — ALWAYS True  (set before inference)
        primary_driver         — top SHAP feature label
        decision_basis         — {feature: pct_contribution}, sums to 100

    Never raises — validation and model failures return safe fallback dicts.

    Args:
        fhir_claim: FHIR R4 Claim resource dict (see Contract B).

    Returns:
        dict: Contract E denial risk output with 10 keys.
    """
    # ── CMS compliance flags — set BEFORE inference runs ─────────────────────
    ai_decision_only = False  # ALWAYS False  — CMS §431.10
    reconsideration_right = True  # ALWAYS True   — CMS §431.220

    claim_id = fhir_claim.get("id", "UNKNOWN")
    is_valid, errors = validate_fhir_claim(fhir_claim)
    if not is_valid:
        log.warning("claim_validation_failed", claim_id=claim_id, errors=errors)
        return _validation_failure(claim_id, errors, ai_decision_only, reconsideration_right)

    try:
        result = generate_explanation(fhir_claim)
        # HARD RULE: human_review_required is derived from score, not model
        result["human_review_required"] = result["denial_risk_score"] >= 70
        result["ai_decision_only"] = ai_decision_only
        result["reconsideration_right"] = reconsideration_right
        return result
    except Exception as e:
        log.error("scoring_failed", claim_id=claim_id, error=str(e))
        return _scoring_failure(claim_id, str(e), ai_decision_only, reconsideration_right)


def reset_duplicate_store() -> None:
    """No-op reset hook for test isolation.

    Provided for test compatibility.  The V1 model does not maintain an
    in-process duplicate store; this function exists so integration tests can
    call it between test cases without needing version-specific logic.
    """


def _validation_failure(
    claim_id: str,
    errors: list[str],
    ai_decision_only: bool,
    reconsideration_right: bool,
) -> dict:
    """Safe fallback dict for claims that fail FHIR validation."""
    return {
        "denial_risk_score": 99,
        "decision_basis": {"validation_failed": 100.0},
        "primary_driver": "failed FHIR validation",
        "primary_driver_weight": "100%",
        "human_review_required": True,
        "ai_decision_only": ai_decision_only,
        "reconsideration_right": reconsideration_right,
        "provider_explanation": f"Claim {claim_id} failed validation: {errors[0]}.",
        "model_version": "none",
        "feature_vector": [],
    }


def _scoring_failure(
    claim_id: str,
    error: str,
    ai_decision_only: bool,
    reconsideration_right: bool,
) -> dict:
    """Safe fallback dict when model scoring raises an exception."""
    return {
        "denial_risk_score": 70,
        "decision_basis": {"model_error": 100.0},
        "primary_driver": "model scoring error",
        "primary_driver_weight": "100%",
        "human_review_required": True,
        "ai_decision_only": ai_decision_only,
        "reconsideration_right": reconsideration_right,
        "provider_explanation": (
            f"Automated scoring unavailable for {claim_id}. Routed to human review."
        ),
        "model_version": "error",
        "feature_vector": [],
    }
