"""Kafka topic archiver — drains output topics to per-topic NDJSON files, one file per date."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import structlog
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger()

KAFKA_ARCHIVE_PATH = os.getenv("KAFKA_ARCHIVE_PATH")
MAX_MESSAGES = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))


def _make_consumer(topics: list[str]) -> "Consumer":
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": f"claims-archive-{uuid.uuid4().hex[:8]}",  # ephemeral — always reads from offset 0
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        }
    )
    consumer.subscribe(topics)
    return consumer


def _archive_path(base: str, topic: str, date_str: str) -> Path:  # noqa: return type is Path
    """Return the NDJSON file path for a topic + date, creating parent dirs."""
    p = Path(base) / topic / f"{date_str}.ndjson"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def archive_topics(
    archive_path: str = KAFKA_ARCHIVE_PATH,
    max_messages: int = MAX_MESSAGES,
) -> dict[str, int]:
    """
    Consume messages from all 5 pipeline topics and append to NDJSON archives.

    Args:
        archive_path: Root directory for archive files (default: $KAFKA_ARCHIVE_PATH).
        max_messages: Maximum messages to consume per invocation (default: $KAFKA_MAX_MESSAGES).

    Returns:
        dict[str, int]: Message counts written per topic.
    """
    topics = [
        os.getenv("KAFKA_TOPIC_RAW", "claims.raw"),
        os.getenv("KAFKA_TOPIC_VALIDATED", "claims.validated"),
        os.getenv("KAFKA_TOPIC_REVIEW_QUEUE", "claims.review_queue"),
        os.getenv("KAFKA_TOPIC_REJECTED", "claims.rejected"),
        os.getenv("KAFKA_TOPIC_REMITTANCE", "remittance.835"),
    ]
    topics = [t for t in topics if t is not None]
    if not topics:
        log.warning("no_topics_configured")
        return {}

    consumer = _make_consumer(topics)
    today = datetime.now(tz=timezone.utc).strftime("%Y%m%d")

    handles: dict[str, object] = {
        t: _archive_path(archive_path, t, today).open("a", encoding="utf-8") for t in topics
    }
    counts: dict[str, int] = {t: 0 for t in topics}

    total = 0
    try:
        while total < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                log.info("archiver_poll_timeout", total_so_far=total)
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("archiver_kafka_error", error=str(msg.error()))
                break

            topic = msg.topic()
            key = msg.key().decode("utf-8") if msg.key() else ""
            raw = msg.value().decode("utf-8") if msg.value() else ""
            ts = datetime.now(tz=timezone.utc).isoformat()

            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                value = raw  # keep as string if not valid JSON

            record = json.dumps({"key": key, "value": value, "ts": ts})
            handles[topic].write(record + "\n")
            counts[topic] = counts.get(topic, 0) + 1
            total += 1

    finally:
        for fh in handles.values():
            fh.close()
        consumer.close()

    for topic, count in counts.items():
        if count:
            log.info(
                "topic_archived",
                topic=topic,
                count=count,
                path=str(_archive_path(archive_path, topic, today)),
            )
    log.info("archive_complete", total=total, **counts)
    return counts


if __name__ == "__main__":
    result = archive_topics()
    log.info("archive_topics_complete", **result)
