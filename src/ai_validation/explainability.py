"""ai_validation/explainability.py
SHAP-based explainability + CMS Non-Discrimination in AI rule fields.
Supports V1 (sklearn RF) and V2 (LightGBM) via DENIAL_MODEL_VERSION env var.
"""

from __future__ import annotations

import os
from pathlib import Path

import joblib
import numpy as np
import shap
import structlog

from src.ai_validation.train_denial_model import FEATURE_NAMES

log = structlog.get_logger()
V1_MODEL_PATH = Path("src/ai_validation/models/denial_model_v1.pkl")
V2_MODEL_PATH = Path("src/ai_validation/models/denial_model_v2.json")

_DRIVER_LABELS: dict[str, str] = {
    "billed_amount": "elevated billed amount",
    "prior_auth": "missing prior authorization",
    "diagnosis_code_count": "high diagnosis code count",
    "procedure_complexity": "procedure complexity level",
    "provider_specialty_code": "provider specialty category",
    "days_since_service": "days elapsed since service date",
}


def _load_model(version: str) -> object:
    """Load the trained model artifact for the given version string.

    Args:
        version: 'v1' (sklearn RF pkl) or 'v2' (LightGBM json).

    Returns:
        Fitted model object.

    Raises:
        FileNotFoundError: If the model artifact is not found on disk.
        ValueError: If version is not 'v1' or 'v2'.
    """
    if version == "v1":
        if not V1_MODEL_PATH.exists():
            raise FileNotFoundError(f"V1 model not found: {V1_MODEL_PATH}. Run: make train")
        return joblib.load(V1_MODEL_PATH)
    elif version == "v2":
        import lightgbm as lgb

        if not V2_MODEL_PATH.exists():
            raise FileNotFoundError(f"V2 model not found: {V2_MODEL_PATH}. Run: make train-v2")
        return lgb.Booster(model_file=str(V2_MODEL_PATH))
    raise ValueError(f"Unknown version: {version}. Expected v1 or v2.")


def _get_shap_values(model: object, features: np.ndarray, version: str) -> np.ndarray:
    """Compute per-feature SHAP values for a single claim.

    For V1, scales features through the pipeline's StandardScaler before
    passing to TreeExplainer so values are consistent with model training.

    Args:
        model: Fitted sklearn Pipeline (v1) or LightGBM Booster (v2).
        features: 1-D feature array of length len(FEATURE_NAMES).
        version: 'v1' or 'v2'.

    Returns:
        np.ndarray: 1-D array of SHAP values (one per feature, class-1 for v1).
    """
    X = features.reshape(1, -1)
    if version == "v1":
        scaler = model.named_steps["scaler"]
        clf = model.named_steps["clf"]
        X_scaled = scaler.transform(X)
        sv = shap.TreeExplainer(clf).shap_values(X_scaled)
        return sv[1][0]  # sv[1] = class-1 (denied), [0] = single sample
    else:
        sv = shap.TreeExplainer(model).shap_values(X)
        # SHAP ≥ 0.42: LightGBM binary returns list[ndarray]; take class-1 (denied)
        if isinstance(sv, list):
            arr = sv[1] if len(sv) > 1 else sv[0]
        else:
            arr = sv
        return np.asarray(arr).flatten()


def _normalize_shap(abs_vals: np.ndarray) -> dict[str, float]:
    """Normalize absolute SHAP values to percentage contributions summing to 100.

    Each value is rounded to one decimal place.  The largest bucket absorbs
    any rounding residual so the dict always sums to exactly 100.0.

    Args:
        abs_vals: Array of absolute SHAP values, one per feature.

    Returns:
        dict mapping feature name → percentage contribution.
    """
    total = abs_vals.sum()
    if total == 0:
        even = round(100.0 / len(FEATURE_NAMES), 1)
        pcts = [even] * len(FEATURE_NAMES)
    else:
        pcts = [round(float(v / total * 100), 1) for v in abs_vals]

    residual = round(100.0 - sum(pcts), 1)
    if residual != 0.0:
        max_idx = pcts.index(max(pcts))
        pcts[max_idx] = round(pcts[max_idx] + residual, 1)

    return {FEATURE_NAMES[i]: pcts[i] for i in range(len(FEATURE_NAMES))}


def generate_explanation(fhir_claim: dict, version: str | None = None) -> dict:
    """Generate SHAP denial risk explanation satisfying 2024 CMS requirements.

    Extracts six model features from the FHIR Claim dict, runs the trained
    model to obtain a probability-based denial risk score, computes SHAP
    attribution for each feature, and returns the full Contract E dict.

    Args:
        fhir_claim: FHIR R4 Claim resource dict (see Contract B).
        version: Model version string ('v1' or 'v2').  Defaults to
                 DENIAL_MODEL_VERSION env var, falling back to 'v1'.

    Returns:
        dict: CMS-aligned output with 10 keys (see Contract E).

    Raises:
        FileNotFoundError: if model pkl file not found at ai_validation/models/
    """
    if version is None:
        version = os.getenv("DENIAL_MODEL_VERSION", "v2")
    model = _load_model(version)

    # ── Feature extraction from FHIR Claim ───────────────────────────────────
    billed_amount = float(fhir_claim.get("total", {}).get("value", 0.0))
    prior_auth = 1.0 if fhir_claim.get("_prior_auth") else 0.0
    diagnosis_code_count = float(len(fhir_claim.get("diagnosis", [])))
    procedure_complexity = float(len(fhir_claim.get("item", [])))
    provider_specialty_code = 11.0  # default: general practice
    days_since_service = 30.0  # default: typical submission lag

    features = np.array(
        [
            billed_amount,
            prior_auth,
            diagnosis_code_count,
            procedure_complexity,
            provider_specialty_code,
            days_since_service,
        ],
        dtype=float,
    )

    # ── Model score: int(probability * 100) ──────────────────────────────────
    # lgb.Booster.predict() returns probabilities directly (no predict_proba)
    if version == "v2":
        prob = float(model.predict(features.reshape(1, -1))[0])
    else:
        prob = float(model.predict_proba(features.reshape(1, -1))[0][1])
    denial_risk_score = int(prob * 100)

    # ── SHAP attribution ──────────────────────────────────────────────────────
    shap_vals = _get_shap_values(model, features, version)
    abs_vals = np.abs(shap_vals)
    decision_basis = _normalize_shap(abs_vals)

    primary_driver_key = max(decision_basis, key=decision_basis.get)
    primary_driver_weight = decision_basis[primary_driver_key]
    driver_label = _DRIVER_LABELS.get(primary_driver_key, primary_driver_key)

    # ── CMS-compliant human-readable explanation ──────────────────────────────
    human_review = denial_risk_score >= 70
    if human_review:
        explanation = (
            f"This claim was flagged for human review (risk score {denial_risk_score}/100). "
            f"The primary contributing factor was {driver_label} "
            f"({primary_driver_weight:.0f}% of risk attribution). "
            "A qualified reviewer will evaluate before any adverse determination."
        )
    else:
        explanation = (
            f"Claim scored {denial_risk_score}/100 — below human review threshold. "
            f"Primary risk factor: {driver_label} ({primary_driver_weight:.0f}% attribution)."
        )

    result = {
        "denial_risk_score": denial_risk_score,
        "decision_basis": decision_basis,
        "primary_driver": driver_label,
        "primary_driver_weight": f"{primary_driver_weight:.0f}%",
        "human_review_required": human_review,
        "ai_decision_only": False,  # ALWAYS False — CMS §431.10
        "reconsideration_right": True,  # ALWAYS True  — CMS §431.220
        "provider_explanation": explanation,
        "model_version": version,
        "feature_vector": features.tolist(),
    }

    log.info(
        "explanation_generated",
        claim_id=fhir_claim.get("id"),
        score=denial_risk_score,
        review=human_review,
        version=version,
    )
    return result
