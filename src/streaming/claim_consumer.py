"""Kafka consumer: reads FHIR claims from claims.raw, writes to Delta Bronze table."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import structlog
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

load_dotenv()
log = structlog.get_logger()

BRONZE_PATH = os.getenv("BRONZE_PATH")
MAX_MESSAGES = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))

BRONZE_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), False),
        StructField("fhir_json", StringType(), False),
        StructField("ingest_ts", TimestampType(), True),
        StructField("received_ts", TimestampType(), True),
    ]
)


def create_spark(app_name: str = "claims-bronze-consumer") -> SparkSession:
    """Create SparkSession with Delta Lake extensions configured."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "50")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
        .getOrCreate()
    )


def consume_to_bronze(
    bronze_path: str = BRONZE_PATH,
    max_messages: int = MAX_MESSAGES,
) -> int:
    """
    Consume FHIR claims from Kafka claims.raw and append to Delta Bronze table.

    Args:
        bronze_path:  Path where Delta Bronze table is stored.
        max_messages: Maximum number of messages to consume per invocation.

    Returns:
        int: Number of records written to Bronze.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": os.getenv("KAFKA_CONSUMER_GROUP"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        }
    )
    consumer.subscribe([os.getenv("KAFKA_TOPIC_CLAIMS_RAW")])

    records: list[tuple] = []
    last_msg = None
    try:
        while len(records) < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                log.info("consumer_poll_timeout", records_so_far=len(records))
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("kafka_error", error=str(msg.error()))
                break
            claim_id = msg.key().decode("utf-8") if msg.key() else ""
            fhir_json = msg.value().decode("utf-8")
            received_ts = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            records.append(
                (
                    claim_id,
                    fhir_json,
                    None,  # ingest_ts — set by Spark current_timestamp()
                    received_ts,
                )
            )
            last_msg = msg
    finally:
        # Commit after the loop so one offset covers the whole batch.
        if last_msg is not None:
            consumer.commit(message=last_msg, asynchronous=False)
        consumer.close()

    if not records:
        log.info("consumer_no_messages")
        return 0

    spark = create_spark()
    df = spark.createDataFrame(records, schema=BRONZE_SCHEMA).withColumn(
        "ingest_ts", current_timestamp()
    )
    df.write.format("delta").mode("append").save(bronze_path)
    log.info("bronze_write_complete", count=len(records), path=bronze_path)
    return len(records)


if __name__ == "__main__":
    n = consume_to_bronze()
    log.info("consume_to_bronze_complete", records_written=n)
