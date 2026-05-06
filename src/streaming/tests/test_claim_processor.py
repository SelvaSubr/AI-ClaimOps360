"""
Unit tests for streaming/claim_processor.py
Mocks Kafka Consumer + Producer — no live broker needed.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

MOCK_FHIR_CLAIM = {
    "resourceType": "Claim",
    "id": "CLM-TEST-001",
    "status": "active",
    "patient": {"reference": "Patient/MEMBRMCK0001"},
    "total": {"value": 350.0, "currency": "USD"},
    "item": [{"productOrService": {"coding": [{"code": "99213"}]}}],
}

MOCK_ADJUDICATION = {
    "claim_id": "CLM-TEST-001",
    "status": "paid",
    "paid_amount": 280.0,
    "billed_amount": 350.0,
    "cas_amount": 70.0,
    "cas_code": "CO-45",
    "denial_reason": None,
    "member_id": "MEMBRMCK0001",
    "date_of_service": "2024-01-01",
}


class TestProcessClaims:

    def _make_mock_message(self, payload: dict) -> MagicMock:
        msg = MagicMock()
        msg.error.return_value = None
        msg.key.return_value = b"CLM-TEST-001"
        msg.value.return_value = json.dumps(payload).encode("utf-8")
        return msg

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.adjudicate_claim")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_processes_one_claim_returns_stats(
        self, mock_validate, mock_eligibility, mock_adjudicate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": True, "reason": "active"}
        mock_adjudicate.return_value = MOCK_ADJUDICATION
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer_cls.return_value = MagicMock()

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["processed"] == 1

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.adjudicate_claim")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_paid_claim_increments_paid_counter(
        self, mock_validate, mock_eligibility, mock_adjudicate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": True, "reason": "active"}
        adj = {**MOCK_ADJUDICATION, "status": "paid"}
        mock_adjudicate.return_value = adj
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer_cls.return_value = MagicMock()

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["paid"] == 1
        assert result["denied"] == 0

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.adjudicate_claim")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_denied_claim_increments_denied_counter(
        self, mock_validate, mock_eligibility, mock_adjudicate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": True, "reason": "active"}
        adj = {**MOCK_ADJUDICATION, "status": "denied"}
        mock_adjudicate.return_value = adj
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer_cls.return_value = MagicMock()

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["denied"] == 1
        assert result["paid"] == 0

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    def test_empty_topic_returns_zero_stats(self, mock_consumer_cls, mock_producer_cls):
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        mock_consumer_cls.return_value = mock_consumer
        mock_producer_cls.return_value = MagicMock()

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result == {"processed": 0, "paid": 0, "denied": 0, "rejected": 0}

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.adjudicate_claim")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_adjudication_result_produced_to_kafka(
        self, mock_validate, mock_eligibility, mock_adjudicate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": True, "reason": "active"}
        mock_adjudicate.return_value = MOCK_ADJUDICATION
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from src.streaming.claim_processor import process_claims

        process_claims(max_messages=10)

        assert mock_producer.produce.call_count == 1
        call_kwargs = mock_producer.produce.call_args
        assert "CLM-TEST-001" in str(call_kwargs)

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_invalid_claim_routes_to_rejected(
        self, mock_validate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (False, ["missing provider"])
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["rejected"] == 1
        assert result["processed"] == 0

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_ineligible_member_routes_to_rejected(
        self, mock_validate, mock_eligibility, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": False, "reason": "not_enrolled"}
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["rejected"] == 1
        assert result["processed"] == 0

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.adjudicate_claim")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_exception_during_processing_does_not_increment_processed(
        self, mock_validate, mock_eligibility, mock_adjudicate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": True, "reason": "active"}
        mock_adjudicate.side_effect = RuntimeError("simulated downstream failure")
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer_cls.return_value = MagicMock()

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["processed"] == 0
        assert result.get("paid", 0) == 0
        assert result.get("denied", 0) == 0

    @patch("src.streaming.claim_processor.Producer")
    @patch("src.streaming.claim_processor.Consumer")
    @patch("src.streaming.claim_processor.adjudicate_claim")
    @patch("src.streaming.claim_processor.check_eligibility")
    @patch("src.streaming.claim_processor.validate_fhir_claim")
    def test_unexpected_adjudication_status_creates_key_in_stats(
        self, mock_validate, mock_eligibility, mock_adjudicate, mock_consumer_cls, mock_producer_cls
    ):
        mock_validate.return_value = (True, [])
        mock_eligibility.return_value = {"eligible": True, "reason": "active"}
        pending_adj = {**MOCK_ADJUDICATION, "status": "pending"}
        mock_adjudicate.return_value = pending_adj
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [
            self._make_mock_message(MOCK_FHIR_CLAIM),
            None,
        ]
        mock_consumer_cls.return_value = mock_consumer
        mock_producer_cls.return_value = MagicMock()

        from src.streaming.claim_processor import process_claims

        result = process_claims(max_messages=10)

        assert result["processed"] == 1
        assert result.get("pending") == 1
        assert result.get("paid", 0) == 0
