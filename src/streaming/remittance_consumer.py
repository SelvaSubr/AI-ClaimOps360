"""Kafka consumer: remittance.835 topic → Delta Gold remittance table."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import structlog
from confluent_kafka import Consumer, KafkaError
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

load_dotenv()
log = structlog.get_logger()

GOLD_PATH: str = os.getenv("GOLD_PATH", "/tmp/delta/gold")  # nosec B108
MAX_MESSAGES: int = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))

# Schema matches GOLD.REMITTANCE DDL in snowflake/setup.sql.
REMITTANCE_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), False),
        StructField("member_id", StringType(), True),
        StructField("date_of_service", StringType(), True),
        StructField("status", StringType(), False),
        StructField("billed_amount", DoubleType(), True),
        StructField("paid_amount", DoubleType(), True),
        StructField("cas_amount", DoubleType(), True),
        StructField("cas_code", StringType(), True),
        StructField("denial_reason", StringType(), True),
        StructField("processed_ts", TimestampType(), True),
    ]
)


def _create_spark() -> SparkSession:
    """Create or reuse a SparkSession (delegates to claim_consumer.create_spark)."""
    from src.streaming.claim_consumer import create_spark as _base

    return _base("remittance-consumer")


def _make_consumer() -> Consumer:
    """Create a Kafka consumer subscribed to the remittance.835 topic."""
    return Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": os.getenv("KAFKA_REMITTANCE_CONSUMER_GROUP"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        }
    )


def _parse_record(payload: dict) -> dict:
    """Map adjudication JSON payload to a REMITTANCE_SCHEMA row dict."""
    return {
        "claim_id": payload.get("claim_id"),
        "member_id": payload.get("member_id"),
        "date_of_service": payload.get("date_of_service"),
        "status": payload.get("status"),
        "billed_amount": float(payload.get("billed_amount", 0.0)),
        "paid_amount": float(payload.get("paid_amount", 0.0)),
        "cas_amount": float(payload.get("cas_amount", 0.0)),
        "cas_code": payload.get("cas_code"),
        "denial_reason": payload.get("denial_reason"),
        "processed_ts": datetime.now(tz=timezone.utc),
    }


def consume_remittance_to_delta(
    gold_path: str = GOLD_PATH,
    max_messages: int = MAX_MESSAGES,
) -> int:
    """Consume remittance.835 JSON records and append to Delta Gold remittance table.

    Reads JSON adjudication dicts from the remittance.835 Kafka topic (produced by
    claim_processor.py) and writes them to {gold_path}/remittance/ as Delta Parquet.
    Offsets are committed manually after each record for at-least-once delivery.

    Args:
        gold_path:    Root path for Delta Gold tables. Remittance is written to
                      {gold_path}/remittance/ — the path read by make snowflake-load.
        max_messages: Maximum messages to consume per invocation.

    Returns:
        int: Total rows in the remittance Delta table after this run (0 if no records).
    """
    spark = _create_spark()
    consumer = _make_consumer()
    topic = os.getenv("KAFKA_TOPIC_REMITTANCE", "remittance.835")
    consumer.subscribe([topic])
    remittance_path = f"{gold_path}/remittance"

    records: list[dict] = []
    total_seen = 0

    try:
        while total_seen < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                log.info("remittance_poll_timeout", records_seen=total_seen)
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("remittance_consumer_error", error=str(msg.error()))
                break

            total_seen += 1
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                records.append(_parse_record(payload))
                consumer.commit(message=msg)
                log.info(
                    "remittance_record_consumed",
                    claim_id=payload.get("claim_id"),
                    status=payload.get("status"),
                )
            except (json.JSONDecodeError, Exception) as exc:
                log.error("remittance_parse_error", error=str(exc), offset=msg.offset())

    finally:
        consumer.close()

    if not records:
        log.info("remittance_consumer_no_records", topic=topic)
        return 0

    df = spark.createDataFrame(records, schema=REMITTANCE_SCHEMA)
    if DeltaTable.isDeltaTable(spark, remittance_path):
        dt = DeltaTable.forPath(spark, remittance_path)
        (
            dt.alias("target")
            .merge(df.alias("source"), "target.claim_id = source.claim_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(remittance_path)
    total_rows = spark.read.format("delta").load(remittance_path).count()
    log.info("remittance_delta_written", path=remittance_path, total_rows=total_rows)
    return total_rows


if __name__ == "__main__":
    result = consume_remittance_to_delta()
    log.info("remittance_consumer_complete", rows=result)
