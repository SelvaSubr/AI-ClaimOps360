"""
Unit tests for src/streaming/transaction_archiver.py.

Kafka consumer is mocked — no live broker required.
File I/O uses pytest's tmp_path fixture — no real KAFKA_ARCHIVE_PATH needed.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# Must be set before importing the module — constants are evaluated at import time.
os.environ.setdefault("KAFKA_MAX_MESSAGES", "100")
os.environ.setdefault("KAFKA_ARCHIVE_PATH", "/tmp/test_archive")  # nosec B108
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_ARCHIVE_CONSUMER_GROUP", "test-archiver-group")
os.environ.setdefault("KAFKA_TOPIC_CLAIMS_VALIDATED", "claims.validated")
os.environ.setdefault("KAFKA_TOPIC_REVIEW_QUEUE", "claims.review_queue")
os.environ.setdefault("KAFKA_TOPIC_REJECTED", "claims.rejected")

from src.streaming.transaction_archiver import _archive_path, archive_topics  # noqa: E402


def _make_kafka_message(topic: str, key: str, value: dict) -> MagicMock:
    """Return a mock confluent_kafka Message with the given topic/key/value."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = topic
    msg.key.return_value = key.encode("utf-8")
    msg.value.return_value = json.dumps(value).encode("utf-8")
    return msg


class TestArchivePath:
    def test_archive_path_structure(self, tmp_path):
        """_archive_path returns {base}/{topic}/{YYYYMMDD}.ndjson and creates dirs."""
        date_str = "20260415"
        p = _archive_path(str(tmp_path), "claims.validated", date_str)
        assert p.parent.exists()
        assert p.name == f"{date_str}.ndjson"
        assert "claims.validated" in str(p)


class TestArchiveTopics:
    def _mock_consumer_with_messages(self, messages: list) -> MagicMock:
        """Build a mock Consumer whose poll() returns messages then None."""
        consumer = MagicMock()
        consumer.poll.side_effect = messages + [None]
        return consumer

    def test_archive_creates_output_file(self, tmp_path):
        """archive_topics writes an NDJSON file when a message is received."""
        topic = "claims.validated"
        msg = _make_kafka_message(topic, "CLM-001", {"resourceType": "Claim", "id": "CLM-001"})
        mock_consumer = self._mock_consumer_with_messages([msg])

        with patch("src.streaming.transaction_archiver._make_consumer", return_value=mock_consumer):
            counts = archive_topics(archive_path=str(tmp_path), max_messages=10)

        today = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
        output_file = tmp_path / topic / f"{today}.ndjson"
        assert output_file.exists(), f"Expected archive file at {output_file}"
        assert counts[topic] == 1

    def test_archive_empty_topic_no_exception(self, tmp_path):
        """archive_topics returns cleanly with zero counts when the topic is empty."""
        mock_consumer = self._mock_consumer_with_messages([])  # poll → None immediately

        with patch("src.streaming.transaction_archiver._make_consumer", return_value=mock_consumer):
            counts = archive_topics(archive_path=str(tmp_path), max_messages=10)

        for topic in ["claims.validated", "claims.review_queue", "claims.rejected"]:
            assert counts[topic] == 0

    def test_archive_output_path_is_date_partitioned(self, tmp_path):
        """Archive files are written to {archive_path}/{topic}/{YYYYMMDD}.ndjson."""
        topic = "claims.rejected"
        msg = _make_kafka_message(topic, "CLM-002", {"id": "CLM-002"})
        mock_consumer = self._mock_consumer_with_messages([msg])

        with patch("src.streaming.transaction_archiver._make_consumer", return_value=mock_consumer):
            archive_topics(archive_path=str(tmp_path), max_messages=10)

        today = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
        # Path must follow {base}/{topic}/{YYYYMMDD}.ndjson — date in filename
        expected = tmp_path / topic / f"{today}.ndjson"
        assert expected.exists()
        # Filename is an 8-digit date stamp (YYYYMMDD)
        assert expected.stem.isdigit() and len(expected.stem) == 8

    def test_archive_message_written_as_valid_json(self, tmp_path):
        """Each archived line is valid JSON containing the claim key."""
        topic = "claims.review_queue"
        payload = {"resourceType": "Claim", "id": "CLM-MCK-20260101-005", "status": "active"}
        msg = _make_kafka_message(topic, "CLM-MCK-20260101-005", payload)
        mock_consumer = self._mock_consumer_with_messages([msg])

        with patch("src.streaming.transaction_archiver._make_consumer", return_value=mock_consumer):
            archive_topics(archive_path=str(tmp_path), max_messages=10)

        today = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
        output_file = tmp_path / topic / f"{today}.ndjson"
        line = output_file.read_text().strip()
        record = json.loads(line)

        assert record["key"] == "CLM-MCK-20260101-005"
        assert isinstance(record["value"], dict)
        assert record["value"]["id"] == "CLM-MCK-20260101-005"
        assert "ts" in record

    def test_returns_empty_when_all_topics_none(self, tmp_path):
        """Guard: if all topic env vars resolve to None, return {} without connecting."""
        with patch(
            "src.streaming.transaction_archiver.os.getenv",
            return_value=None,
        ):
            result = archive_topics(archive_path=str(tmp_path))
        assert result == {}

    def test_archive_multiple_topics_counted_separately(self, tmp_path):
        """Messages on different topics are counted and filed independently."""
        validated_msg = _make_kafka_message("claims.validated", "CLM-001", {"id": "CLM-001"})
        rejected_msg = _make_kafka_message("claims.rejected", "CLM-002", {"id": "CLM-002"})
        mock_consumer = self._mock_consumer_with_messages([validated_msg, rejected_msg])

        with patch("src.streaming.transaction_archiver._make_consumer", return_value=mock_consumer):
            counts = archive_topics(archive_path=str(tmp_path), max_messages=10)

        assert counts["claims.validated"] == 1
        assert counts["claims.rejected"] == 1
        assert counts["claims.review_queue"] == 0
