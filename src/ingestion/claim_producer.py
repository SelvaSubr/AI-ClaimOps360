"""Kafka producer: reads EDI 837 file, maps to FHIR, sends to claims.raw topic."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import structlog
from confluent_kafka import KafkaError, Producer
from dotenv import load_dotenv

from src.ingestion.edi_parser import parse_837_multi
from src.ingestion.fhir_mapper import map_to_fhir_claim, unwrap_fhir

load_dotenv()
log = structlog.get_logger()


def _delivery_callback(err: KafkaError | None, msg) -> None:
    """Kafka delivery callback — called once per message on success or failure."""
    if err:
        log.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic() if msg else "unknown",
        )
    else:
        log.info(
            "kafka_delivery_success",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def produce_claim(edi_file_path: str) -> list[dict]:
    """
    Parse all claims from an EDI 837P file, map each to FHIR, and produce
    every claim to the claims.raw Kafka topic.

    Handles single-claim files (one CLM in one ST/SE) as well as real-world
    files with multiple ST/SE transaction sets and up to 85,000 CLM segments.

    Args:
        edi_file_path: Path to the EDI 837P file on disk.

    Returns:
        list[dict]: FHIR Claim resources that were sent (Contract B),
                    one per CLM segment.

    Raises:
        FileNotFoundError: if edi_file_path does not exist.
        ValueError: if EDI content is invalid.
    """
    path = Path(edi_file_path)
    if not path.exists():
        raise FileNotFoundError(f"EDI file not found: {edi_file_path}")

    with open(path) as f:
        edi_content = f.read()

    parsed_claims = parse_837_multi(edi_content)

    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "acks": "all",
            "enable.idempotence": True,
            "retries": 3,
            "broker.address.family": "v4",
        }
    )

    topic = os.getenv("KAFKA_TOPIC_CLAIMS_RAW")
    fhir_claims = []
    for parsed in parsed_claims:
        fhir_claim = map_to_fhir_claim(parsed)
        producer.produce(
            topic=topic,
            key=fhir_claim["id"].encode("utf-8"),
            value=json.dumps(fhir_claim).encode("utf-8"),
            callback=_delivery_callback,
        )
        fhir_claims.append(fhir_claim)
        log.info("claim_produced", claim_id=fhir_claim["id"], topic=topic)

    producer.flush(timeout=10)
    return fhir_claims


def produce_fhir_bundle(fhir_file_path: str) -> list[dict]:
    """
    Read a FHIR R4 Bundle (or bare Claim) JSON file, unwrap all Claim resources,
    and produce one Kafka message per Claim to the claims.raw topic.

    Args:
        fhir_file_path: Path to a FHIR JSON file on disk.

    Returns:
        list[dict]: FHIR Claim resources that were sent, one per Claim in the Bundle.

    Raises:
        FileNotFoundError: if fhir_file_path does not exist.
        ValueError: if the file contains no Claim resources.
    """
    path = Path(fhir_file_path)
    if not path.exists():
        raise FileNotFoundError(f"FHIR file not found: {fhir_file_path}")

    with open(path) as f:
        data = json.load(f)

    claims = unwrap_fhir(data)
    if not claims:
        raise ValueError(f"No Claim resources found in {fhir_file_path}")

    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "acks": "all",
            "enable.idempotence": True,
            "retries": 3,
            "broker.address.family": "v4",
        }
    )

    topic = os.getenv("KAFKA_TOPIC_CLAIMS_RAW")
    for fhir_claim in claims:
        producer.produce(
            topic=topic,
            key=fhir_claim["id"].encode("utf-8"),
            value=json.dumps(fhir_claim).encode("utf-8"),
            callback=_delivery_callback,
        )
        log.info("fhir_claim_produced", claim_id=fhir_claim["id"], topic=topic)

    producer.flush(timeout=10)
    return claims


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce claims to Kafka")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Path to EDI 837 file")
    group.add_argument("--fhir-file", dest="fhir_file", help="Path to FHIR Bundle JSON file")
    args = parser.parse_args()
    if args.fhir_file:
        results = produce_fhir_bundle(args.fhir_file)
    else:
        results = produce_claim(args.file)
    log.info("produce_complete", count=len(results), claims=[c["id"] for c in results])
