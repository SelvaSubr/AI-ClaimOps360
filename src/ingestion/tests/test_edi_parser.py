"""
Unit tests for src/ingestion/edi_parser.py.
All tests use the mock_claim_001.edi fixture file.
Run: pytest src/ingestion/tests/test_edi_parser.py -v
"""

from pathlib import Path

import pytest

from src.ingestion.edi_parser import (
    extract_nm1_loop,
    parse_837,
    parse_837_multi,
    parse_isa_envelope,
    split_segments,
)
from src.ingestion.tests.fixtures.sample_edi import MOCK_EDI_STANDARD

EDI_FILE = Path("sample_data/837/mock_claim_001.edi")


@pytest.fixture
def edi_content() -> str:
    return EDI_FILE.read_text()


@pytest.fixture
def parsed() -> dict:
    # MOCK_EDI_STANDARD is single-ST; mock_claim_001.edi has multiple ST segments
    # and would cause parse_837 to sum billed_amount across all transactions.
    return parse_837(MOCK_EDI_STANDARD)


@pytest.fixture
def segments(edi_content) -> list[str]:
    return split_segments(edi_content)


class TestSplitSegments:
    def test_returns_list(self, edi_content):
        segs = split_segments(edi_content)
        assert isinstance(segs, list)

    def test_no_empty_segments(self, edi_content):
        segs = split_segments(edi_content)
        assert all(s.strip() for s in segs)

    def test_first_segment_is_isa(self, edi_content):
        segs = split_segments(edi_content)
        assert segs[0].startswith("ISA")

    def test_strips_whitespace(self):
        content = "  ISA*data~  GS*data~  "
        segs = split_segments(content)
        assert all(not s.startswith(" ") for s in segs)


class TestParseISAEnvelope:
    def test_valid_isa_returns_dict(self, segments):
        result = parse_isa_envelope(segments)
        assert isinstance(result, dict)
        assert "sender_id" in result

    def test_missing_isa_raises(self):
        with pytest.raises(ValueError, match="no ISA segment"):
            parse_isa_envelope(["GS*data", "ST*data"])

    def test_wrong_element_count_raises(self):
        bad_isa = "ISA*00*only*three*elements"
        with pytest.raises(ValueError, match="16 elements"):
            parse_isa_envelope([bad_isa])


class TestParse837:
    def test_transaction_id(self, parsed):
        assert parsed["transaction_id"] == "CLM-MCK-20260101-001"

    def test_billing_npi(self, parsed):
        assert parsed["billing_npi"] == "9990000001"

    def test_patient_member_id(self, parsed):
        assert parsed["patient_member_id"] == "MCKMEMBR0001"

    def test_primary_diagnosis(self, parsed):
        assert "Z00.00" in parsed["diagnosis_codes"]

    def test_procedure_code(self, parsed):
        assert "99213" in parsed["procedure_codes"]

    def test_billed_amount(self, parsed):
        assert parsed["billed_amount"] == 350.0

    def test_prior_auth(self, parsed):
        assert parsed["prior_auth_number"] == "AUTHMCK0001"

    def test_returns_all_required_keys(self, parsed):
        required = [
            "transaction_id",
            "billing_npi",
            "rendering_npi",
            "patient_member_id",
            "date_of_service",
            "diagnosis_codes",
            "procedure_codes",
            "billed_amount",
            "prior_auth_number",
        ]
        assert all(k in parsed for k in required)

    def test_no_prior_auth_returns_none(self):
        # Create EDI without REF*D9 segment
        edi = (
            "ISA*00*          *00*          *ZZ*SENDER0001  *ZZ*RECEIVER001 "
            "*260412*1200*^*00501*000000001*0*T*:~"
            "GS*HC*SENDER*RECV*20260412*1200*1*X*005010X222A1~"
            "ST*837*0001*005010X222A1~"
            "CLM*CLM-TEST-001*350.00***11:B:1*Y*A*Y*I~"
            "HI*ABK:Z00.00~"
            "SV1*HC:99213*350.00*UN*1***1~"
            "NM1*85*2*TESTBILLINGGROUP*****XX*9990000001~"
            "NM1*IL*1*TESTLAST*TESTFIRST****MI*MCKMEMBR0001~"
            "IEA*1*000000001~"
        )
        result = parse_837(edi)
        assert result["prior_auth_number"] is None

    def test_billed_amount_float(self, parsed):
        assert isinstance(parsed["billed_amount"], float)


class TestParse837Multi:
    """
    Exercises parse_837_multi against mock_claim_001.edi which contains
    two ST/SE transaction sets: claims 001-003 in ST#1 and claims 004-005
    in ST#2.
    """

    @pytest.fixture
    def all_claims(self, edi_content) -> list[dict]:
        return parse_837_multi(edi_content)

    def test_returns_five_claims(self, all_claims):
        assert len(all_claims) == 5

    def test_claim_ids_in_order(self, all_claims):
        ids = [c["transaction_id"] for c in all_claims]
        assert ids == [
            "CLM-MCK-20260101-001",
            "CLM-MCK-20260101-002",
            "CLM-MCK-20260101-003",
            "CLM-MCK-20260101-004",
            "CLM-MCK-20260101-005",
        ]

    def test_first_transaction_set_has_three_claims(self, all_claims):
        # Claims 001-003 come from ST#1
        assert all_claims[0]["transaction_id"] == "CLM-MCK-20260101-001"
        assert all_claims[2]["transaction_id"] == "CLM-MCK-20260101-003"

    def test_second_transaction_set_has_two_claims(self, all_claims):
        # Claims 004-005 come from ST#2
        assert all_claims[3]["transaction_id"] == "CLM-MCK-20260101-004"
        assert all_claims[4]["transaction_id"] == "CLM-MCK-20260101-005"

    def test_each_claim_has_own_billed_amount(self, all_claims):
        amounts = [c["billed_amount"] for c in all_claims]
        assert amounts == [350.0, 1200.0, 8500.0, 650.0, 950.0]

    def test_each_claim_has_own_patient(self, all_claims):
        member_ids = [c["patient_member_id"] for c in all_claims]
        assert member_ids == [
            "MCKMEMBR0001",
            "MCKMEMBR0002",
            "MCKMEMBR0003",
            "MCKMEMBR0004",
            "MCKMEMBR0005",
        ]

    def test_each_claim_has_own_diagnosis(self, all_claims):
        primary_dx = [c["diagnosis_codes"][0] for c in all_claims]
        assert primary_dx == ["Z00.00", "M79.3", "I21.09", "J06.9", "M54.5"]

    def test_billing_npi_shared_across_transaction_sets(self, all_claims):
        # NM1*85 is repeated in both ST/SE but should resolve to the same NPI
        npis = {c["billing_npi"] for c in all_claims}
        assert npis == {"9000000007"}

    def test_prior_auth_present_only_on_claims_with_ref_d9(self, all_claims):
        # Claims 001, 002, 004, 005 have REF*D9; only 003 does not
        assert all_claims[0]["prior_auth_number"] == "AUTHMCK0001"
        assert all_claims[1]["prior_auth_number"] == "EXPIREDAUTH002"
        assert all_claims[2]["prior_auth_number"] is None
        assert all_claims[3]["prior_auth_number"] == "AUTHMCK0004"
        assert all_claims[4]["prior_auth_number"] == "PARTIALAUTH005"

    def test_single_claim_file_returns_list_of_one(self):
        result = parse_837_multi(MOCK_EDI_STANDARD)
        assert len(result) == 1
        assert result[0]["transaction_id"] == "CLM-MCK-20260101-001"

    def test_st_se_mismatch_falls_back_and_returns_claims(self):
        # Remove the SE segment so ST count (1) != SE count (0) — triggers fallback
        edi_no_se = (
            "~".join(seg for seg in MOCK_EDI_STANDARD.split("~") if not seg.startswith("SE")) + "~"
        )
        result = parse_837_multi(edi_no_se)
        assert len(result) >= 1
        assert result[0]["transaction_id"] == "CLM-MCK-20260101-001"


class TestParse837EdgeCases:
    def test_non_numeric_sv1_billed_defaults_to_zero(self):
        # Replace the valid billed amount with a non-numeric string in SV1
        bad_edi = MOCK_EDI_STANDARD.replace(
            "SV1*HC:99213:25**350.00*UN*1***1~",
            "SV1*HC:99213:25**BADAMT*UN*1***1~",
        )
        result = parse_837(bad_edi)
        assert result["billed_amount"] == 0.0
        assert "99213" in result["procedure_codes"]

    def test_multiple_diagnoses_parsed_in_order(self):
        # Inject a second HI segment with ABF (secondary diagnosis)
        multi_dx_edi = MOCK_EDI_STANDARD.replace(
            "HI*ABK:Z00.00~",
            "HI*ABK:Z00.00*ABF:M79.3~",
        )
        result = parse_837(multi_dx_edi)
        assert result["diagnosis_codes"] == ["Z00.00", "M79.3"]

    def test_multiple_sv1_segments_sum_billed_amount(self):
        # Two SV1 lines — billed_amount should be the sum of both
        multi_sv1_edi = MOCK_EDI_STANDARD.replace(
            "SV1*HC:99213:25**350.00*UN*1***1~",
            "SV1*HC:99213:25**200.00*UN*1***1~SV1*HC:93000:25**150.00*UN*1***1~",
        )
        result = parse_837(multi_sv1_edi)
        assert result["billed_amount"] == pytest.approx(350.0)
        assert "99213" in result["procedure_codes"]
        assert "93000" in result["procedure_codes"]
