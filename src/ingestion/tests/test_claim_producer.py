"""
Unit tests for src/ingestion/claim_producer.py.
Uses unittest.mock to avoid needing a running Kafka broker.
Run: pytest ingestion/tests/test_claim_producer.py -v
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from src.ingestion.claim_producer import _delivery_callback, produce_claim, produce_fhir_bundle

EDI_FILE = Path("sample_data/837/mock_claim_001.edi")
FHIR_FILE = Path("sample_data/fhir/mock_fhir_claim_001.json")


class TestDeliveryCallback:
    def test_no_error_does_not_raise(self):
        msg = MagicMock()
        msg.topic.return_value = "claims.raw"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        _delivery_callback(None, msg)  # Must not raise

    def test_error_does_not_raise(self):
        err = MagicMock()
        err.__str__ = lambda _: "broker unavailable"
        _delivery_callback(err, None)  # Must not raise


EXPECTED_CLAIM_IDS = [
    b"CLM-MCK-20260101-001",
    b"CLM-MCK-20260101-002",
    b"CLM-MCK-20260101-003",
    b"CLM-MCK-20260101-004",
    b"CLM-MCK-20260101-005",
]


class TestProduceClaim:
    def test_missing_file_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            produce_claim("/nonexistent/path/claim.edi")

    @patch("src.ingestion.claim_producer.Producer")
    def test_produce_returns_list_of_five_fhir_claims(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        result = produce_claim(str(EDI_FILE))
        assert isinstance(result, list)
        assert len(result) == 5
        assert all(c["resourceType"] == "Claim" for c in result)

    @patch("src.ingestion.claim_producer.Producer")
    def test_produce_claim_ids_in_order(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        result = produce_claim(str(EDI_FILE))
        assert [c["id"] for c in result] == [k.decode() for k in EXPECTED_CLAIM_IDS]

    @patch("src.ingestion.claim_producer.Producer")
    def test_produce_calls_kafka_produce_five_times(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_claim(str(EDI_FILE))
        assert mock_p.produce.call_count == 5

    @patch("src.ingestion.claim_producer.Producer")
    def test_produce_keys_match_claim_ids(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_claim(str(EDI_FILE))
        actual_keys = [c.kwargs["key"] for c in mock_p.produce.call_args_list]
        assert actual_keys == EXPECTED_CLAIM_IDS

    @patch("src.ingestion.claim_producer.Producer")
    def test_produce_calls_flush_once(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_claim(str(EDI_FILE))
        assert mock_p.flush.call_count == 1

    @patch("src.ingestion.claim_producer.Producer")
    def test_all_values_are_valid_fhir_json(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_claim(str(EDI_FILE))
        for c in mock_p.produce.call_args_list:
            parsed = json.loads(c.kwargs["value"].decode("utf-8"))
            assert parsed["resourceType"] == "Claim"


class TestProduceFhirBundle:
    def test_missing_file_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            produce_fhir_bundle("/nonexistent/path/bundle.json")

    @patch("src.ingestion.claim_producer.Producer")
    def test_returns_five_claims_from_bundle(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        result = produce_fhir_bundle(str(FHIR_FILE))
        assert len(result) == 5
        assert all(c["resourceType"] == "Claim" for c in result)

    @patch("src.ingestion.claim_producer.Producer")
    def test_produces_five_kafka_messages(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_fhir_bundle(str(FHIR_FILE))
        assert mock_p.produce.call_count == 5

    @patch("src.ingestion.claim_producer.Producer")
    def test_kafka_keys_match_claim_ids(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_fhir_bundle(str(FHIR_FILE))
        actual_keys = [c.kwargs["key"] for c in mock_p.produce.call_args_list]
        assert actual_keys == [k for k in EXPECTED_CLAIM_IDS]

    @patch("src.ingestion.claim_producer.Producer")
    def test_calls_flush_once(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_fhir_bundle(str(FHIR_FILE))
        assert mock_p.flush.call_count == 1

    @patch("src.ingestion.claim_producer.Producer")
    def test_all_messages_are_valid_fhir_json(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_fhir_bundle(str(FHIR_FILE))
        for c in mock_p.produce.call_args_list:
            parsed = json.loads(c.kwargs["value"].decode("utf-8"))
            assert parsed["resourceType"] == "Claim"

    @patch("src.ingestion.claim_producer.Producer")
    def test_scores_are_null_before_model_scoring(self, MockProducer):
        mock_p = MagicMock()
        MockProducer.return_value = mock_p
        produce_fhir_bundle(str(FHIR_FILE))
        # Scores must be null in the Kafka message; the model assigns them during streaming inference.
        scores = [
            json.loads(c.kwargs["value"].decode("utf-8"))["_denial_risk_score"]
            for c in mock_p.produce.call_args_list
        ]
        assert all(s is None for s in scores)

    def test_empty_bundle_raises_value_error(self, tmp_path):
        empty = tmp_path / "empty.json"
        empty.write_text('{"resourceType": "Bundle", "entry": []}')
        with pytest.raises(ValueError):
            produce_fhir_bundle(str(empty))
