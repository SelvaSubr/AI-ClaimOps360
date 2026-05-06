"""
Unit tests for ingestion/fhir_mapper.py.
Run: pytest ingestion/tests/test_fhir_mapper.py -v
"""

import json
from pathlib import Path

import pytest

from src.ingestion.edi_parser import parse_837
from src.ingestion.fhir_mapper import (
    _dos_to_iso,
    map_to_fhir_claim,
    unwrap_fhir,
    validate_fhir_claim,
)
from src.ingestion.tests.fixtures.sample_edi import MOCK_EDI_STANDARD


@pytest.fixture
def parsed() -> dict:
    # MOCK_EDI_STANDARD is single-ST; mock_claim_001.edi has multiple ST segments
    # and would cause parse_837 to sum billed_amount across all transactions.
    return parse_837(MOCK_EDI_STANDARD)


@pytest.fixture
def fhir(parsed) -> dict:
    return map_to_fhir_claim(parsed)


FHIR_FILE = Path("sample_data/fhir/mock_fhir_claim_001.json")


class TestUnwrapFhir:
    def test_bundle_returns_five_claims(self):
        import json

        data = json.loads(FHIR_FILE.read_text())
        claims = unwrap_fhir(data)
        assert len(claims) == 5

    def test_bundle_all_resource_types_are_claim(self):
        import json

        data = json.loads(FHIR_FILE.read_text())
        claims = unwrap_fhir(data)
        assert all(c["resourceType"] == "Claim" for c in claims)

    def test_bare_claim_returns_single_item_list(self):
        claim = {"resourceType": "Claim", "id": "TEST-001"}
        assert unwrap_fhir(claim) == [claim]

    def test_unknown_resource_type_returns_empty(self):
        assert unwrap_fhir({"resourceType": "Patient", "id": "P1"}) == []

    def test_bundle_with_no_entries_returns_empty(self):
        assert unwrap_fhir({"resourceType": "Bundle", "entry": []}) == []

    def test_bundle_skips_non_claim_entries(self):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {"resource": {"resourceType": "Patient", "id": "P1"}},
                {"resource": {"resourceType": "Claim", "id": "C1"}},
            ],
        }
        claims = unwrap_fhir(bundle)
        assert len(claims) == 1
        assert claims[0]["id"] == "C1"

    def test_bundle_claim_ids_in_order(self):
        import json

        data = json.loads(FHIR_FILE.read_text())
        ids = [c["id"] for c in unwrap_fhir(data)]
        assert ids == [
            "CLM-MCK-20260101-001",
            "CLM-MCK-20260101-002",
            "CLM-MCK-20260101-003",
            "CLM-MCK-20260101-004",
            "CLM-MCK-20260101-005",
        ]

    def test_bundle_scores_are_null_before_model_scoring(self):
        import json

        data = json.loads(FHIR_FILE.read_text())
        # Scores must be null at ingest time; the model assigns them during streaming inference.
        scores = [c["_denial_risk_score"] for c in unwrap_fhir(data)]
        assert all(s is None for s in scores)


class TestDosToIso:
    def test_ccyymmdd_converts_correctly(self):
        assert _dos_to_iso("20260412") == "2026-04-12"

    def test_already_iso_passthrough(self):
        assert _dos_to_iso("2026-04-12") == "2026-04-12"

    def test_empty_string_passthrough(self):
        assert _dos_to_iso("") == ""


class TestMapToFhirClaim:
    def test_resource_type_is_claim(self, fhir):
        assert fhir["resourceType"] == "Claim"

    def test_id_matches_transaction_id(self, fhir):
        assert fhir["id"] == "CLM-MCK-20260101-001"

    def test_patient_reference_format(self, fhir):
        assert fhir["patient"]["reference"] == "Patient/MCKMEMBR0001"

    def test_provider_npi_in_identifier(self, fhir):
        assert fhir["provider"]["identifier"]["value"] == "9990000001"
        assert "us-npi" in fhir["provider"]["identifier"]["system"]

    def test_diagnosis_array_nonempty(self, fhir):
        assert len(fhir["diagnosis"]) >= 1

    def test_diagnosis_sequence_starts_at_1(self, fhir):
        assert fhir["diagnosis"][0]["sequence"] == 1

    def test_icd10_code_in_diagnosis(self, fhir):
        codes = [d["diagnosisCodeableConcept"]["coding"][0]["code"] for d in fhir["diagnosis"]]
        assert "Z00.00" in codes

    def test_item_array_nonempty(self, fhir):
        assert len(fhir["item"]) >= 1

    def test_cpt_code_in_items(self, fhir):
        codes = [i["productOrService"]["coding"][0]["code"] for i in fhir["item"]]
        assert "99213" in codes

    def test_total_value_matches_billed(self, fhir):
        assert fhir["total"]["value"] == 350.0

    def test_total_currency_usd(self, fhir):
        assert fhir["total"]["currency"] == "USD"

    def test_private_prior_auth_field(self, fhir):
        assert fhir["_prior_auth"] == "AUTHMCK0001"

    def test_private_denial_risk_starts_none(self, fhir):
        assert fhir["_denial_risk_score"] is None

    def test_result_is_json_serializable(self, fhir):
        dumped = json.dumps(fhir)  # Must not raise
        assert "CLM-MCK-20260101-001" in dumped


class TestMapToFhirClaimMulti:
    def test_multiple_diagnoses_have_sequential_sequences(self):
        parsed = parse_837(MOCK_EDI_STANDARD)
        parsed["diagnosis_codes"] = ["Z00.00", "M79.3", "I21.09"]
        fhir = map_to_fhir_claim(parsed)
        seqs = [d["sequence"] for d in fhir["diagnosis"]]
        assert seqs == [1, 2, 3]

    def test_multiple_diagnoses_preserve_codes(self):
        parsed = parse_837(MOCK_EDI_STANDARD)
        parsed["diagnosis_codes"] = ["Z00.00", "M79.3"]
        fhir = map_to_fhir_claim(parsed)
        codes = [d["diagnosisCodeableConcept"]["coding"][0]["code"] for d in fhir["diagnosis"]]
        assert codes == ["Z00.00", "M79.3"]

    def test_multiple_procedure_codes_produce_multiple_items(self):
        parsed = parse_837(MOCK_EDI_STANDARD)
        parsed["procedure_codes"] = ["99213", "93000"]
        fhir = map_to_fhir_claim(parsed)
        assert len(fhir["item"]) == 2
        item_codes = [it["productOrService"]["coding"][0]["code"] for it in fhir["item"]]
        assert "99213" in item_codes
        assert "93000" in item_codes

    def test_multiple_items_have_sequential_sequences(self):
        parsed = parse_837(MOCK_EDI_STANDARD)
        parsed["procedure_codes"] = ["99213", "93000"]
        fhir = map_to_fhir_claim(parsed)
        seqs = [it["sequence"] for it in fhir["item"]]
        assert seqs == [1, 2]

    def test_empty_procedure_codes_gives_empty_item_array(self):
        parsed = parse_837(MOCK_EDI_STANDARD)
        parsed["procedure_codes"] = []
        fhir = map_to_fhir_claim(parsed)
        assert fhir["item"] == []

    def test_empty_diagnosis_codes_gives_empty_diagnosis_array(self):
        parsed = parse_837(MOCK_EDI_STANDARD)
        parsed["diagnosis_codes"] = []
        fhir = map_to_fhir_claim(parsed)
        assert fhir["diagnosis"] == []


class TestValidateFhirClaim:
    def test_valid_claim_returns_empty_list(self, fhir):
        errors = validate_fhir_claim(fhir)
        assert errors == []

    def test_missing_status_detected(self, fhir):
        del fhir["status"]
        assert "status" in validate_fhir_claim(fhir)

    def test_empty_diagnosis_detected(self, fhir):
        fhir["diagnosis"] = []
        assert "diagnosis" in validate_fhir_claim(fhir)
