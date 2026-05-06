"""
Unit tests for streaming/claim_consumer.py.

Mocks Kafka (no real broker) and Delta write (no real Spark).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.claim_consumer import consume_to_bronze

# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_kafka_msg(
    claim_id: str,
    payload: dict,
    offset: int = 0,
    topic: str = "claims.raw",
) -> MagicMock:
    """Build a mock confluent_kafka Message with the given payload."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.key.return_value = claim_id.encode("utf-8")
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    msg.offset.return_value = offset
    msg.topic.return_value = topic
    return msg


def _fhir_claim(claim_id: str) -> dict:
    """Return a minimal FHIR R4 Claim resource dict."""
    return {
        "resourceType": "Claim",
        "id": claim_id,
        "status": "active",
        "use": "claim",
        "patient": {"reference": f"Patient/{claim_id}"},
    }


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def mock_current_timestamp():
    """Patch pyspark current_timestamp so tests never need an active SparkContext."""
    mock_col = MagicMock()
    with patch("src.streaming.claim_consumer.current_timestamp", return_value=mock_col):
        yield mock_col


@pytest.fixture()
def mock_spark() -> MagicMock:
    """Return a mock SparkSession with a chainable DataFrame stub."""
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    spark = MagicMock()
    spark.createDataFrame.return_value = mock_df
    return spark


@pytest.fixture()
def mock_consumer_factory():
    """
    Return a factory that builds a mock Kafka Consumer yielding given messages.

    After all messages are exhausted the consumer returns None (poll timeout).
    """

    def _factory(messages: list[MagicMock]) -> MagicMock:
        consumer = MagicMock()
        consumer.poll.side_effect = messages + [None]
        return consumer

    return _factory


# ── Tests: messages consumed and written to Bronze ─────────────────────────────


class TestConsumeMessagesWrittenToBronze:
    """Messages consumed from Kafka are written to Delta Bronze."""

    def test_returns_count_equal_to_messages_received(
        self,
        mock_consumer_factory: object,
        mock_spark: MagicMock,
        tmp_path: object,
    ) -> None:
        """consume_to_bronze returns the exact number of Kafka messages processed."""
        msgs = [
            _make_kafka_msg(f"CLM-00{i}", _fhir_claim(f"CLM-00{i}"), offset=i) for i in range(3)
        ]
        mock_consumer = mock_consumer_factory(msgs)

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark", return_value=mock_spark),
        ):
            result = consume_to_bronze(bronze_path=str(tmp_path), max_messages=10)

        assert result == 3

    def test_delta_append_called_once(
        self,
        mock_consumer_factory: object,
        mock_spark: MagicMock,
        tmp_path: object,
    ) -> None:
        """A single Delta append write is issued after consuming all messages."""
        msgs = [_make_kafka_msg("CLM-001", _fhir_claim("CLM-001"), offset=0)]
        mock_consumer = mock_consumer_factory(msgs)
        mock_df = mock_spark.createDataFrame.return_value

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark", return_value=mock_spark),
        ):
            consume_to_bronze(bronze_path=str(tmp_path), max_messages=10)

        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format.return_value.mode.assert_called_once_with("append")

    def test_fhir_json_passed_to_createDataFrame(
        self,
        mock_consumer_factory: object,
        mock_spark: MagicMock,
        tmp_path: object,
    ) -> None:
        """The raw FHIR JSON string from the Kafka message is handed to Spark."""
        payload = _fhir_claim("CLM-XYZ")
        msgs = [_make_kafka_msg("CLM-XYZ", payload, offset=0)]
        mock_consumer = mock_consumer_factory(msgs)

        captured_rows: list = []

        def _capture(rows, schema=None):  # noqa: ANN001
            captured_rows.extend(rows)
            return mock_spark.createDataFrame.return_value

        mock_spark.createDataFrame.side_effect = _capture

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark", return_value=mock_spark),
        ):
            consume_to_bronze(bronze_path=str(tmp_path), max_messages=10)

        assert len(captured_rows) == 1
        claim_id, fhir_json, _ingest_ts, received_ts = captured_rows[0]
        assert claim_id == "CLM-XYZ"
        assert json.loads(fhir_json)["id"] == "CLM-XYZ"
        assert isinstance(received_ts, datetime)

    def test_returns_zero_when_no_messages(self, tmp_path: object) -> None:
        """consume_to_bronze returns 0 and skips Spark when the topic is empty."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None  # immediate timeout

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark") as mock_create_spark,
        ):
            result = consume_to_bronze(bronze_path=str(tmp_path), max_messages=10)

        assert result == 0
        mock_create_spark.assert_not_called()

    def test_consumer_is_always_closed(self, tmp_path: object) -> None:
        """consumer.close() is called even if an exception occurs mid-poll."""
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = RuntimeError("broker gone")

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark"),
            pytest.raises(RuntimeError),
        ):
            consume_to_bronze(bronze_path=str(tmp_path), max_messages=10)

        mock_consumer.close.assert_called_once()


# ── Tests: max_messages limit ──────────────────────────────────────────────────


class TestMaxMessagesLimit:
    """max_messages cap is respected regardless of how many messages are available."""

    def test_stops_after_max_messages(
        self,
        mock_spark: MagicMock,
        tmp_path: object,
    ) -> None:
        """Only max_messages records are written even if more are available."""
        # 10 messages available but cap is 3
        msgs = [
            _make_kafka_msg(f"CLM-{i:03d}", _fhir_claim(f"CLM-{i:03d}"), offset=i)
            for i in range(10)
        ]
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = msgs  # never returns None — cap must stop it

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark", return_value=mock_spark),
        ):
            result = consume_to_bronze(bronze_path=str(tmp_path), max_messages=3)

        assert result == 3

    def test_max_messages_one(
        self,
        mock_spark: MagicMock,
        tmp_path: object,
    ) -> None:
        """max_messages=1 writes exactly one record."""
        msgs = [
            _make_kafka_msg(f"CLM-{i:03d}", _fhir_claim(f"CLM-{i:03d}"), offset=i) for i in range(5)
        ]
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = msgs

        captured: list = []

        def _cap(rows, schema=None):  # noqa: ANN001
            captured.extend(rows)
            return mock_spark.createDataFrame.return_value

        mock_spark.createDataFrame.side_effect = _cap

        with (
            patch("src.streaming.claim_consumer.Consumer", return_value=mock_consumer),
            patch("src.streaming.claim_consumer.create_spark", return_value=mock_spark),
        ):
            result = consume_to_bronze(bronze_path=str(tmp_path), max_messages=1)

        assert result == 1
        assert len(captured) == 1

    def test_env_max_messages_respected(
        self,
        mock_spark: MagicMock,
        tmp_path: object,
    ) -> None:
        """MAX_MESSAGES module constant gates the default cap."""
        import src.streaming.claim_consumer as mod

        original = mod.MAX_MESSAGES
        try:
            mod.MAX_MESSAGES = 2
            msgs = [
                _make_kafka_msg(f"CLM-{i:03d}", _fhir_claim(f"CLM-{i:03d}"), offset=i)
                for i in range(10)
            ]
            mock_consumer = MagicMock()
            mock_consumer.poll.side_effect = msgs

            with (
                patch(
                    "src.streaming.claim_consumer.Consumer",
                    return_value=mock_consumer,
                ),
                patch(
                    "src.streaming.claim_consumer.create_spark",
                    return_value=mock_spark,
                ),
            ):
                result = consume_to_bronze(bronze_path=str(tmp_path), max_messages=mod.MAX_MESSAGES)

            assert result == 2
        finally:
            mod.MAX_MESSAGES = original
