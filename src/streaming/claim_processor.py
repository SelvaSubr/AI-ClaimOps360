"""
Full claims processing loop: Kafka claims.raw → adjudicate → Kafka remittance.835.

Consumes FHIR claims in order, validates structure, checks member eligibility,
runs payer adjudication, generates EDI 835, and produces to remittance.835.
Claims that fail validation or eligibility are routed to claims.rejected.
"""

from __future__ import annotations

import json
import os
from datetime import date

import structlog
from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

from src.ai_validation.validator import validate_fhir_claim
from src.payer_simulation.eligibility_checker import check_eligibility
from src.payer_simulation.payer_engine import adjudicate_claim

load_dotenv()
log = structlog.get_logger()

# Topic name constants — pinned for testability
TOPIC_RAW = os.getenv("KAFKA_TOPIC_CLAIMS_RAW")
TOPIC_REMITTANCE = os.getenv("KAFKA_TOPIC_REMITTANCE")
TOPIC_REJECTED = os.getenv("KAFKA_TOPIC_REJECTED")

MAX_MESSAGES = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))


def _make_producer() -> Producer:
    """Create an idempotent Kafka producer with acks=all."""
    return Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "acks": "all",
            "enable.idempotence": True,
            "broker.address.family": "v4",
        }
    )


def _make_consumer() -> Consumer:
    """Create a Kafka consumer with manual offset commit for at-least-once delivery."""
    return Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": os.getenv("KAFKA_PAYER_CONSUMER_GROUP"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        }
    )


def _extract_member_dos(fhir_claim: dict) -> tuple[str, date]:
    """Extract member_id and date_of_service from a FHIR R4 Claim for eligibility check.

    Args:
        fhir_claim: FHIR R4 Claim resource dict (Contract B).

    Returns:
        tuple: (member_id str, date_of_service date)
    """
    patient_ref = fhir_claim.get("patient", {}).get("reference", "")
    member_id = patient_ref.split("/")[-1] if "/" in patient_ref else patient_ref
    items = fhir_claim.get("item", [])
    dos_str = items[0].get("servicedDate", "") if items else ""
    try:
        dos = date.fromisoformat(dos_str) if dos_str else date.today()
    except ValueError:
        dos = date.today()
    return member_id, dos


def process_claims(max_messages: int = MAX_MESSAGES) -> dict:
    """Consume FHIR claims from claims.raw, adjudicate, and produce to remittance.835 or claims.rejected.

    Args:
        max_messages: Maximum number of Kafka messages to consume per invocation.

    Returns:
        dict: {'processed': n, 'paid': n, 'denied': n, 'rejected': n}
    """
    consumer = _make_consumer()
    producer = _make_producer()
    consumer.subscribe([os.getenv("KAFKA_TOPIC_CLAIMS_RAW")])

    remittance_topic = os.getenv("KAFKA_TOPIC_REMITTANCE")
    rejected_topic = os.getenv("KAFKA_TOPIC_REJECTED")
    stats: dict[str, int] = {"processed": 0, "paid": 0, "denied": 0, "rejected": 0}
    total_seen = 0

    try:
        while total_seen < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("consumer_error", error=str(msg.error()))
                continue

            total_seen += 1

            try:
                fhir_claim = json.loads(msg.value().decode("utf-8"))
                claim_id = fhir_claim.get("id", "UNKNOWN")

                is_valid, errors = validate_fhir_claim(fhir_claim)
                if not is_valid:
                    rejection = {
                        "claim_id": claim_id,
                        "reason": "validation_failed",
                        "errors": errors,
                    }
                    producer.produce(
                        topic=rejected_topic,
                        key=claim_id.encode("utf-8"),
                        value=json.dumps(rejection).encode("utf-8"),
                    )
                    stats["rejected"] += 1
                    log.warning("claim_rejected_validation", claim_id=claim_id, errors=errors)
                    consumer.commit(message=msg)
                    continue

                member_id, dos = _extract_member_dos(fhir_claim)
                elig = check_eligibility(member_id, dos)
                if not elig["eligible"]:
                    rejection = {
                        "claim_id": claim_id,
                        "reason": "eligibility_failed",
                        "eligibility_reason": elig["reason"],
                        "member_id": member_id,
                    }
                    producer.produce(
                        topic=rejected_topic,
                        key=claim_id.encode("utf-8"),
                        value=json.dumps(rejection).encode("utf-8"),
                    )
                    stats["rejected"] += 1
                    log.warning(
                        "claim_rejected_eligibility",
                        claim_id=claim_id,
                        member_id=member_id,
                        reason=elig["reason"],
                    )
                    consumer.commit(message=msg)
                    continue

                adjudication = adjudicate_claim(fhir_claim)

                # JSON dict, not EDI 835 — remittance_consumer writes typed Parquet without a parser.
                remittance_record = {
                    k: adjudication[k]
                    for k in (
                        "claim_id",
                        "member_id",
                        "date_of_service",
                        "status",
                        "billed_amount",
                        "paid_amount",
                        "cas_amount",
                        "cas_code",
                        "denial_reason",
                    )
                }
                producer.produce(
                    topic=remittance_topic,
                    key=adjudication["claim_id"].encode("utf-8"),
                    value=json.dumps(remittance_record).encode("utf-8"),
                )

                stats["processed"] += 1
                stats[adjudication["status"]] = stats.get(adjudication["status"], 0) + 1
                consumer.commit(message=msg)
                log.info(
                    "claim_processed",
                    claim_id=adjudication["claim_id"],
                    status=adjudication["status"],
                    paid=adjudication["paid_amount"],
                )

            except Exception as e:
                log.error(
                    "claim_processing_failed",
                    claim_id=claim_id,
                    error=str(e),
                    offset=msg.offset(),
                )
                consumer.commit(message=msg)
                stats["failed"] = stats.get("failed", 0) + 1
                continue

    finally:
        producer.flush(timeout=10)
        consumer.close()

    log.info("processing_complete", **stats)
    return stats


if __name__ == "__main__":
    result = process_claims()
    log.info("process_claims_complete", **result)
