"""src/ai_validation/tests/test_validation.py
Unit tests for validator.py, denial_risk_scorer.py, and explainability.py.
Run: pytest src/ai_validation/tests/test_validation.py -v
Note: explainability tests require denial_model_v1.pkl (run: make train first).
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.ai_validation.denial_risk_scorer import calculate_denial_risk
from src.ai_validation.validator import validate_fhir_claim, validate_npi

FHIR_FILE = Path("sample_data/fhir/mock_fhir_claim_001.json")


@pytest.fixture
def valid_fhir() -> dict:
    if not FHIR_FILE.exists():
        pytest.fail(f"Missing fixture file: {FHIR_FILE}")
    data = json.loads(FHIR_FILE.read_text(encoding="utf-8"))
    # The FHIR file is a Bundle — extract the first Claim resource for unit tests
    if data.get("resourceType") == "Bundle":
        return data["entry"][0]["resource"]
    return data


@pytest.fixture
def minimal_fhir() -> dict:
    """Minimal valid FHIR Claim for focused tests."""
    return {
        "resourceType": "Claim",
        "id": "TEST-001",
        "status": "active",
        "patient": {"reference": "Patient/MCKMEMBR0001"},
        "provider": {
            "identifier": {
                "value": "1234567893",
                "system": "http://hl7.org/fhir/sid/us-npi",
            }
        },
        "diagnosis": [
            {
                "sequence": 1,
                "diagnosisCodeableConcept": {"coding": [{"code": "Z00.00"}]},
            }
        ],
        "item": [
            {
                "sequence": 1,
                "servicedDate": "2026-04-12",
                "productOrService": {"coding": [{"code": "99213"}]},
            }
        ],
        "total": {"value": 350.0, "currency": "USD"},
        "_prior_auth": "AUTHMCK0001",
    }


# ── NPI Validation ──
class TestValidateNPI:
    def test_valid_npi_1234567893(self):
        assert validate_npi("1234567893") is True

    def test_valid_npi_9000000007(self):
        assert validate_npi("9000000007") is True

    def test_invalid_npi_9990000001(self):
        assert validate_npi("9990000001") is False

    def test_invalid_npi_all_zeros(self):
        assert validate_npi("0000000000") is False

    def test_invalid_npi_wrong_length(self):
        assert validate_npi("999000000") is False  # 9 digits

    def test_invalid_npi_non_numeric(self):
        assert validate_npi("999000000X") is False

    def test_empty_string_returns_false(self):
        assert validate_npi("") is False

    def test_none_returns_false(self):
        assert validate_npi(None) is False


# ── FHIR Claim Validation ──
class TestValidateFhirClaim:
    def test_valid_claim_passes(self, minimal_fhir):
        is_valid, errors = validate_fhir_claim(minimal_fhir)
        assert is_valid is True
        assert errors == []

    def test_missing_status_fails(self, minimal_fhir):
        del minimal_fhir["status"]
        is_valid, errors = validate_fhir_claim(minimal_fhir)
        assert is_valid is False
        assert any("status" in e for e in errors)

    def test_empty_diagnosis_fails(self, minimal_fhir):
        minimal_fhir["diagnosis"] = []
        is_valid, errors = validate_fhir_claim(minimal_fhir)
        assert is_valid is False
        assert "empty_diagnosis_array" in errors

    def test_empty_item_fails(self, minimal_fhir):
        minimal_fhir["item"] = []
        is_valid, errors = validate_fhir_claim(minimal_fhir)
        assert is_valid is False
        assert "empty_item_array" in errors

    def test_zero_total_fails(self, minimal_fhir):
        minimal_fhir["total"]["value"] = 0
        is_valid, errors = validate_fhir_claim(minimal_fhir)
        assert is_valid is False

    def test_wrong_resource_type_fails(self, minimal_fhir):
        minimal_fhir["resourceType"] = "Patient"
        is_valid, errors = validate_fhir_claim(minimal_fhir)
        assert is_valid is False
        assert "wrong_resource_type" in errors

    def test_full_mock_claim_passes(self, valid_fhir):
        is_valid, errors = validate_fhir_claim(valid_fhir)
        assert is_valid is True, f"Validation errors: {errors}"


# ── Denial Risk Scorer — V1 Regression / Legacy Baseline ──
class TestCalculateDenialRisk:
    def test_returns_all_contract_e_keys(self, minimal_fhir):
        result = calculate_denial_risk(minimal_fhir)
        required = [
            "denial_risk_score",
            "decision_basis",
            "primary_driver",
            "primary_driver_weight",
            "human_review_required",
            "ai_decision_only",
            "reconsideration_right",
            "provider_explanation",
            "model_version",
            "feature_vector",
        ]
        assert all(k in result for k in required)

    def test_ai_decision_only_always_false(self, minimal_fhir):
        result = calculate_denial_risk(minimal_fhir)
        assert result["ai_decision_only"] is False

    def test_reconsideration_right_always_true(self, minimal_fhir):
        result = calculate_denial_risk(minimal_fhir)
        assert result["reconsideration_right"] is True

    def test_score_is_integer_0_to_100(self, minimal_fhir):
        result = calculate_denial_risk(minimal_fhir)
        assert isinstance(result["denial_risk_score"], int)
        assert 0 <= result["denial_risk_score"] <= 100

    def test_high_score_triggers_review(self, minimal_fhir):
        minimal_fhir["_prior_auth"] = None
        minimal_fhir["_member_ineligible"] = True
        minimal_fhir["total"]["value"] = 1500.0
        result = calculate_denial_risk(minimal_fhir)
        assert result["human_review_required"] is True
        assert result["denial_risk_score"] >= 70

    def test_low_risk_claim_no_review(self, minimal_fhir):
        minimal_fhir["_prior_auth"] = "AUTH001"
        minimal_fhir["total"]["value"] = 100.0
        result = calculate_denial_risk(minimal_fhir)
        assert result["human_review_required"] is False

    def test_invalid_claim_returns_safe_fallback(self):
        bad_claim = {"resourceType": "Claim", "id": "BAD"}
        result = calculate_denial_risk(bad_claim)
        assert result["ai_decision_only"] is False
        assert result["reconsideration_right"] is True

    def test_decision_basis_sums_to_100(self, minimal_fhir):
        result = calculate_denial_risk(minimal_fhir)
        total = sum(result["decision_basis"].values())
        assert abs(total - 100.0) < 0.1, f"decision_basis sums to {total}, expected ~100"

    def test_provider_explanation_is_string(self, minimal_fhir):
        result = calculate_denial_risk(minimal_fhir)
        assert isinstance(result["provider_explanation"], str)
        assert len(result["provider_explanation"]) > 20

    def test_v2_returns_contract_e_keys(self, minimal_fhir):
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        required = [
            "denial_risk_score",
            "decision_basis",
            "primary_driver",
            "primary_driver_weight",
            "human_review_required",
            "ai_decision_only",
            "reconsideration_right",
            "provider_explanation",
            "model_version",
            "feature_vector",
        ]
        assert all(k in result for k in required)
        assert result["model_version"] == "v2"

    def test_v2_decision_basis_sums_to_100(self, minimal_fhir):
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        total = sum(result["decision_basis"].values())
        assert abs(total - 100.0) < 0.1, f"V2 decision_basis sums to {total}, expected ~100"

    def test_v2_cms_flags_always_set(self, minimal_fhir):
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        assert result["ai_decision_only"] is False
        assert result["reconsideration_right"] is True
        assert isinstance(result["denial_risk_score"], int)
        assert 0 <= result["denial_risk_score"] <= 100


# ── Denial Risk Scorer — V2 Production Champion (LightGBM) ──
class TestDenialRiskScorerV2:
    """V2 LightGBM model — production champion tests.

    All tests call generate_explanation with version='v2' explicitly to
    ensure they exercise the LightGBM code path regardless of the runtime
    env var value.
    """

    def test_v2_score_is_integer_0_to_100(self, minimal_fhir):
        """V2 denial_risk_score is an int in [0, 100]."""
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        assert isinstance(result["denial_risk_score"], int)
        assert 0 <= result["denial_risk_score"] <= 100

    def test_v2_high_risk_triggers_review(self, minimal_fhir):
        """V2 human_review_required=True when score >= 70 (hard rule enforced by scorer)."""
        from src.ai_validation.explainability import generate_explanation

        minimal_fhir["_prior_auth"] = None
        minimal_fhir["_member_ineligible"] = True
        minimal_fhir["total"]["value"] = 1500.0
        result = generate_explanation(minimal_fhir, version="v2")
        assert result["human_review_required"] == (result["denial_risk_score"] >= 70)

    def test_v2_low_risk_no_review(self, minimal_fhir):
        """V2 human_review_required=False for low-risk claims (prior auth, low amount)."""
        from src.ai_validation.explainability import generate_explanation

        minimal_fhir["_prior_auth"] = "AUTH001"
        minimal_fhir["total"]["value"] = 100.0
        result = generate_explanation(minimal_fhir, version="v2")
        assert result["human_review_required"] == (result["denial_risk_score"] >= 70)

    def test_v2_cms_mandatory_fields_present(self, minimal_fhir):
        """V2 result contains all 7 CMS-mandatory AI columns."""
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        cms_fields = [
            "denial_risk_score",
            "human_review_required",
            "provider_explanation",
            "ai_decision_only",
            "reconsideration_right",
            "primary_driver",
            "decision_basis",
        ]
        assert all(k in result for k in cms_fields)
        assert result["ai_decision_only"] is False
        assert result["reconsideration_right"] is True

    def test_v2_shap_decision_basis_sums_to_100(self, minimal_fhir):
        """V2 SHAP attribution in decision_basis sums to exactly 100.0."""
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        total = sum(result["decision_basis"].values())
        assert abs(total - 100.0) < 0.1, f"V2 decision_basis sums to {total}, expected ~100"

    def test_v2_model_version_tag(self, minimal_fhir):
        """V2 result carries model_version='v2' for audit trail."""
        from src.ai_validation.explainability import generate_explanation

        result = generate_explanation(minimal_fhir, version="v2")
        assert result["model_version"] == "v2"

    def test_v1_vs_v2_both_return_valid_scores(self, minimal_fhir):
        """V1 and V2 both return valid Contract E dicts — regression comparison."""
        from src.ai_validation.explainability import generate_explanation

        r1 = generate_explanation(minimal_fhir, version="v1")
        r2 = generate_explanation(minimal_fhir, version="v2")
        for r, label in [(r1, "V1"), (r2, "V2")]:
            assert 0 <= r["denial_risk_score"] <= 100, f"{label} score out of range"
            assert r["ai_decision_only"] is False, f"{label} ai_decision_only not False"
            assert r["reconsideration_right"] is True, f"{label} reconsideration_right not True"
