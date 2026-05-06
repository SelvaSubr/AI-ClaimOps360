"""
Tests for payer_simulation module.
Run: pytest src/payer_simulation/tests/test_payer_simulation.py -v
"""

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from src.payer_simulation.eligibility_checker import check_eligibility
from src.payer_simulation.payer_engine import adjudicate_claim
from src.payer_simulation.remittance_generator import generate_835

FHIR_FILE = Path("sample_data/fhir/mock_fhir_claim_001.json")


@pytest.fixture
def mock_fhir() -> dict:
    data = json.loads(FHIR_FILE.read_text())
    # The FHIR file is a Bundle — extract the first Claim resource for unit tests
    if data.get("resourceType") == "Bundle":
        return data["entry"][0]["resource"]
    return data


class TestEligibilityChecker:
    def test_known_member_is_eligible(self):
        result = check_eligibility("MCKMEMBR0001", date(2026, 4, 12))
        assert result["eligible"] is True

    def test_unknown_member_is_ineligible(self):
        result = check_eligibility("UNKNOWN_MEMBER_XYZ", date(2026, 4, 12))
        assert result["eligible"] is False
        assert result["reason"] == "member_not_found"

    def test_member_outside_coverage_dates_is_ineligible(self):
        result = check_eligibility("MCKMEMBR0001", date(2019, 12, 31))
        assert result["eligible"] is False
        assert result["reason"] == "coverage_not_active_on_dos"

    def test_eligible_result_includes_plan_type(self):
        result = check_eligibility("MCKMEMBR0001", date(2026, 4, 12))
        assert "plan_type" in result


class TestPayerEngine:
    def test_eligible_claim_is_paid(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "paid"

    def test_paid_amount_matches_fee_schedule(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        assert result["paid_amount"] == 280.0

    def test_cas_code_is_co45_for_paid(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        assert result["cas_code"] == "CO-45"

    def test_cas_amount_is_billed_minus_paid(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        assert result["cas_amount"] == round(result["billed_amount"] - result["paid_amount"], 2)

    def test_ineligible_member_is_denied(self, mock_fhir):
        mock_fhir["patient"]["reference"] = "Patient/UNKNOWN_MEMBER"
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "denied"
        assert result["paid_amount"] == 0.0
        assert result["cas_code"] == "OA-23"

    def test_result_has_all_contract_f_keys(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        required = [
            "claim_id",
            "status",
            "denial_reason",
            "paid_amount",
            "billed_amount",
            "cas_code",
            "cas_amount",
            "member_id",
            "date_of_service",
            "procedures",
        ]
        assert all(k in result for k in required)

    def test_claim_id_matches_fhir_id(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        assert result["claim_id"] == mock_fhir["id"]

    def test_timely_filing_exceeded_is_denied(self, mock_fhir):
        # DOS within coverage (MCKMEMBR0001 covered from 2023-01-01) but >180 days old
        mock_fhir["item"][0]["servicedDate"] = "2025-01-01"
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "denied"
        assert result["cas_code"] == "CO-29"
        assert result["paid_amount"] == 0.0

    def test_non_covered_cpt_is_denied(self, mock_fhir):
        mock_fhir["item"][0]["productOrService"]["coding"][0]["code"] = "15820"
        mock_fhir["item"][0]["servicedDate"] = str(date.today())
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "denied"
        assert result["cas_code"] == "CO-49"

    def test_experimental_cpt_is_denied(self, mock_fhir):
        mock_fhir["item"][0]["productOrService"]["coding"][0]["code"] = "0191T"
        mock_fhir["item"][0]["servicedDate"] = str(date.today())
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "denied"
        assert result["cas_code"] == "CO-50"

    def test_denial_reason_is_short_code(self, mock_fhir):
        # denial_reason is a machine-readable code; Cortex generates explanations at query time
        mock_fhir["patient"]["reference"] = "Patient/UNKNOWN_MEMBER"
        result = adjudicate_claim(mock_fhir)
        assert result["denial_reason"] == "member_not_found"

    def test_billed_above_auth_threshold_without_prior_auth_is_denied(self, mock_fhir):
        # billed > $1000 requires prior auth; mock_fhir has no _prior_auth field
        mock_fhir["total"]["value"] = 1500.0
        mock_fhir.pop("_prior_auth", None)
        mock_fhir["item"][0]["servicedDate"] = str(date.today())
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "denied"
        assert result["denial_reason"] == "prior_auth_required"

    def test_billed_exactly_at_auth_threshold_without_prior_auth_is_denied(self, mock_fhir):
        # billed == $1000 should NOT trigger auth (condition is strictly >)
        mock_fhir["total"]["value"] = 1000.0
        mock_fhir.pop("_prior_auth", None)
        mock_fhir["item"][0]["servicedDate"] = str(date.today())
        result = adjudicate_claim(mock_fhir)
        # $1000 exactly is NOT > threshold, and 99213 is not in _AUTH_REQUIRED_CPTS
        assert result["denial_reason"] != "prior_auth_required"

    def test_billed_just_below_auth_threshold_without_auth_cpt_is_paid(self, mock_fhir):
        mock_fhir["total"]["value"] = 999.99
        mock_fhir.pop("_prior_auth", None)
        mock_fhir["item"][0]["servicedDate"] = str(date.today())
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "paid"

    def test_mixed_covered_and_non_covered_cpts_denied_on_non_covered(self, mock_fhir):
        # Add a second item with a non-covered CPT (15820) alongside the covered one
        covered_item = mock_fhir["item"][0].copy()
        covered_item["servicedDate"] = str(date.today())
        non_covered_item = {
            "productOrService": {"coding": [{"code": "15820"}]},
            "servicedDate": str(date.today()),
        }
        mock_fhir["item"] = [covered_item, non_covered_item]
        result = adjudicate_claim(mock_fhir)
        assert result["status"] == "denied"
        assert result["denial_reason"] == "service_not_covered"


class TestRemittanceGenerator:
    @pytest.fixture
    def adjudication(self, mock_fhir):
        result = adjudicate_claim(mock_fhir)
        result["payer_id"] = "TESTPAYER01"
        return result

    def test_generates_string(self, adjudication):
        result = generate_835(adjudication)
        assert isinstance(result, str)

    def test_has_isa_segment(self, adjudication):
        result = generate_835(adjudication)
        assert result.startswith("ISA")

    def test_has_835_transaction(self, adjudication):
        result = generate_835(adjudication)
        assert "ST*835" in result

    def test_claim_id_in_835(self, adjudication):
        result = generate_835(adjudication)
        assert adjudication["claim_id"] in result

    def test_paid_amount_in_835(self, adjudication):
        result = generate_835(adjudication)
        assert "280.00" in result

    def test_ends_with_iea_segment(self, adjudication):
        result = generate_835(adjudication)
        assert "IEA" in result
