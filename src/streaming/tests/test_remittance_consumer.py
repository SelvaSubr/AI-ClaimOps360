"""
Unit tests for src/streaming/remittance_consumer.py.

Kafka consumer and PySpark Delta writes are mocked — no live broker or
SparkSession required.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Set required env vars before module import (module-level constants evaluated at import time).
os.environ.setdefault("KAFKA_MAX_MESSAGES", "100")
os.environ.setdefault("GOLD_PATH", "/tmp/test_gold")  # nosec B108
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_REMITTANCE_CONSUMER_GROUP", "test-remittance-consumer")
os.environ.setdefault("KAFKA_TOPIC_REMITTANCE", "remittance.835")

from src.streaming.remittance_consumer import (  # noqa: E402
    _parse_record,
    consume_remittance_to_delta,
)

SAMPLE_ADJUDICATION = {
    "claim_id": "CLM-001",
    "member_id": "MBR0001",
    "date_of_service": "2026-04-12",
    "status": "paid",
    "billed_amount": 400.0,
    "paid_amount": 280.0,
    "cas_amount": 120.0,
    "cas_code": "CO-45",
    "denial_reason": "",
}


def _make_kafka_message(key: str, value: dict) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = "remittance.835"
    msg.key.return_value = key.encode("utf-8")
    msg.value.return_value = json.dumps(value).encode("utf-8")
    msg.offset.return_value = 0
    return msg


class TestParseRecord:
    def test_maps_all_schema_fields(self):
        row = _parse_record(SAMPLE_ADJUDICATION)
        assert row["claim_id"] == "CLM-001"
        assert row["status"] == "paid"
        assert row["paid_amount"] == 280.0
        assert row["billed_amount"] == 400.0
        assert row["cas_amount"] == 120.0
        assert row["cas_code"] == "CO-45"
        assert row["member_id"] == "MBR0001"
        assert row["date_of_service"] == "2026-04-12"

    def test_adds_processed_ts(self):
        row = _parse_record(SAMPLE_ADJUDICATION)
        assert isinstance(row["processed_ts"], datetime)

    def test_amounts_are_float(self):
        row = _parse_record(SAMPLE_ADJUDICATION)
        assert isinstance(row["paid_amount"], float)
        assert isinstance(row["billed_amount"], float)
        assert isinstance(row["cas_amount"], float)

    def test_denied_claim_maps_correctly(self):
        denied = {
            **SAMPLE_ADJUDICATION,
            "status": "denied",
            "paid_amount": 0.0,
            "cas_code": "OA-23",
            "denial_reason": "member_not_found",
        }
        row = _parse_record(denied)
        assert row["status"] == "denied"
        assert row["paid_amount"] == 0.0
        assert row["denial_reason"] == "member_not_found"

    def test_missing_optional_fields_default_to_none(self):
        row = _parse_record({"claim_id": "CLM-X", "status": "pending"})
        assert row["claim_id"] == "CLM-X"
        assert row["member_id"] is None
        assert row["paid_amount"] == 0.0


class TestConsumeRemittanceToDelta:
    def _mock_consumer(self, messages: list) -> MagicMock:
        consumer = MagicMock()
        consumer.poll.side_effect = messages + [None]
        return consumer

    def _mock_spark(self, row_count: int = 1) -> MagicMock:
        spark = MagicMock()
        df = MagicMock()
        df.write.format.return_value.mode.return_value.save.return_value = None
        spark.createDataFrame.return_value = df
        spark.read.format.return_value.load.return_value.count.return_value = row_count
        return spark

    def test_returns_zero_on_empty_topic(self, tmp_path):
        mock_consumer = self._mock_consumer([])
        with (
            patch("src.streaming.remittance_consumer._make_consumer", return_value=mock_consumer),
            patch("src.streaming.remittance_consumer._create_spark"),
        ):
            result = consume_remittance_to_delta(gold_path=str(tmp_path), max_messages=10)
        assert result == 0

    def test_writes_delta_on_valid_message(self, tmp_path):
        msg = _make_kafka_message("CLM-001", SAMPLE_ADJUDICATION)
        mock_consumer = self._mock_consumer([msg])
        mock_spark = self._mock_spark(row_count=1)
        with (
            patch("src.streaming.remittance_consumer._make_consumer", return_value=mock_consumer),
            patch("src.streaming.remittance_consumer._create_spark", return_value=mock_spark),
            patch("src.streaming.remittance_consumer.DeltaTable.isDeltaTable", return_value=False),
        ):
            result = consume_remittance_to_delta(gold_path=str(tmp_path), max_messages=10)
        assert result == 1
        mock_spark.createDataFrame.assert_called_once()

    def test_commits_offset_per_message(self, tmp_path):
        msg = _make_kafka_message("CLM-001", SAMPLE_ADJUDICATION)
        mock_consumer = self._mock_consumer([msg])
        mock_spark = self._mock_spark()
        with (
            patch("src.streaming.remittance_consumer._make_consumer", return_value=mock_consumer),
            patch("src.streaming.remittance_consumer._create_spark", return_value=mock_spark),
            patch("src.streaming.remittance_consumer.DeltaTable.isDeltaTable", return_value=False),
        ):
            consume_remittance_to_delta(gold_path=str(tmp_path), max_messages=10)
        mock_consumer.commit.assert_called_once_with(message=msg)

    def test_handles_malformed_json_without_raising(self, tmp_path):
        bad_msg = MagicMock()
        bad_msg.error.return_value = None
        bad_msg.topic.return_value = "remittance.835"
        bad_msg.key.return_value = b"CLM-BAD"
        bad_msg.value.return_value = b"not-valid-json"
        bad_msg.offset.return_value = 0
        mock_consumer = self._mock_consumer([bad_msg])
        with (
            patch("src.streaming.remittance_consumer._make_consumer", return_value=mock_consumer),
            patch("src.streaming.remittance_consumer._create_spark"),
        ):
            result = consume_remittance_to_delta(gold_path=str(tmp_path), max_messages=10)
        assert result == 0

    def test_writes_to_gold_remittance_subpath(self, tmp_path):
        msg = _make_kafka_message("CLM-001", SAMPLE_ADJUDICATION)
        mock_consumer = self._mock_consumer([msg])
        mock_spark = self._mock_spark()
        with (
            patch("src.streaming.remittance_consumer._make_consumer", return_value=mock_consumer),
            patch("src.streaming.remittance_consumer._create_spark", return_value=mock_spark),
            patch("src.streaming.remittance_consumer.DeltaTable.isDeltaTable", return_value=False),
        ):
            consume_remittance_to_delta(gold_path=str(tmp_path), max_messages=10)
        save_call = mock_spark.createDataFrame.return_value.write.format.return_value
        save_call.mode.return_value.save.assert_called_once()
        saved_path = save_call.mode.return_value.save.call_args.args[0]
        assert saved_path.endswith("/remittance")
