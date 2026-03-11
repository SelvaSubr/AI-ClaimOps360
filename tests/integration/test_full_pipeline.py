"""
End-to-end integration test: full claims processing pipeline.
Requires: Kafka running (make kafka-up), V1 model trained (make train).
Marked with @pytest.mark.integration — excluded from fast unit test runs.
Run: pytest tests/integration/test_full_pipeline.py -v
"""

import json
import os
import time
from pathlib import Path

import pytest
import structlog
from pyspark.sql import SparkSession

log = structlog.get_logger()

EDI_FILE = Path("sample_data/837/mock_claim_001.edi")
FHIR_FILE = Path("sample_data/fhir/mock_fhir_claim_001.json")

BRONZE_PATH_INT = os.getenv("BRONZE_PATH_INT")
SILVER_PATH_INT = os.getenv("SILVER_PATH_INT")
GOLD_PATH_INT = os.getenv("GOLD_PATH_INT")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    from src.streaming.claim_consumer import create_spark

    s = create_spark("integration-test")
    yield s
    s.stop()


@pytest.fixture(scope="module", autouse=True)
def cleanup_delta(spark):
    import shutil

    for p in [BRONZE_PATH_INT, SILVER_PATH_INT, GOLD_PATH_INT]:
        shutil.rmtree(p, ignore_errors=True)
    yield
    for p in [BRONZE_PATH_INT, SILVER_PATH_INT, GOLD_PATH_INT]:
        shutil.rmtree(p, ignore_errors=True)


@pytest.mark.integration
class TestIngestionStage:
    def test_edi_file_parseable(self):
        from src.ingestion.edi_parser import parse_837_multi

        # EDI file has 5 ST/SE transaction sets — use parse_837_multi and check first claim
        claims = parse_837_multi(EDI_FILE.read_text())
        result = claims[0]
        assert result["transaction_id"] == "CLM-MCK-20260101-001"
        assert result["billing_npi"] == "9990000001"
        assert result["billed_amount"] == 350.0

    def test_edi_file_has_five_claims(self):
        from src.ingestion.edi_parser import parse_837_multi

        claims = parse_837_multi(EDI_FILE.read_text())
        assert len(claims) == 5

    def test_fhir_mapping_produces_valid_resource(self):
        from src.ingestion.edi_parser import parse_837_multi
        from src.ingestion.fhir_mapper import map_to_fhir_claim, validate_fhir_claim

        parsed = parse_837_multi(EDI_FILE.read_text())[0]
        fhir = map_to_fhir_claim(parsed)
        assert fhir["resourceType"] == "Claim"
        assert validate_fhir_claim(fhir) == []


@pytest.mark.integration
class TestBronzeStage:
    def test_kafka_produce_and_consume_to_bronze(self, spark):
        from unittest.mock import patch

        from src.ingestion.claim_producer import produce_claim
        from src.streaming.claim_consumer import consume_to_bronze

        # Produce to Kafka (requires live Kafka)
        produce_claim(str(EDI_FILE))
        time.sleep(2)  # Allow message to commit

        # Consume to Bronze
        count = consume_to_bronze(bronze_path=BRONZE_PATH_INT, max_messages=10)
        assert count >= 1

        # Verify Bronze table
        bronze = spark.read.format("delta").load(BRONZE_PATH_INT)
        assert bronze.count() >= 1
        assert "claim_id" in bronze.columns
        assert "fhir_json" in bronze.columns

    def test_bronze_claim_id_matches(self, spark):
        bronze = spark.read.format("delta").load(BRONZE_PATH_INT)
        ids = [row.claim_id for row in bronze.select("claim_id").collect()]
        assert "CLM-MCK-20260101-001" in ids

    def test_bronze_fhir_json_is_valid_fhir(self, spark):
        bronze = spark.read.format("delta").load(BRONZE_PATH_INT)
        row = bronze.first()
        fhir = json.loads(row.fhir_json)
        assert fhir["resourceType"] == "Claim"


@pytest.mark.integration
class TestSilverStage:
    def test_bronze_to_silver_transforms_correctly(self, spark):
        from src.streaming.bronze_to_silver import bronze_to_silver

        count = bronze_to_silver(bronze_path=BRONZE_PATH_INT, silver_path=SILVER_PATH_INT)
        assert count >= 1

    def test_silver_has_correct_schema(self, spark):
        silver = spark.read.format("delta").load(SILVER_PATH_INT)
        required = [
            "claim_id",
            "billing_npi",
            "denial_risk_score",
            "human_review_required",
            "provider_explanation",
        ]
        assert all(c in silver.columns for c in required)

    def test_silver_ai_columns_populated(self, spark):
        silver = spark.read.format("delta").load(SILVER_PATH_INT)
        row = silver.filter(silver.claim_id == "CLM-MCK-20260101-001").first()
        assert row is not None
        assert row.denial_risk_score is not None
        assert isinstance(row.denial_risk_score, int)
        assert 0 <= row.denial_risk_score <= 100

    def test_provider_explanation_is_nonempty(self, spark):
        silver = spark.read.format("delta").load(SILVER_PATH_INT)
        row = silver.filter(silver.claim_id == "CLM-MCK-20260101-001").first()
        assert row.provider_explanation
        assert len(row.provider_explanation) > 20

    def test_silver_billing_npi_correct(self, spark):
        silver = spark.read.format("delta").load(SILVER_PATH_INT)
        row = silver.filter(silver.claim_id == "CLM-MCK-20260101-001").first()
        assert row.billing_npi == "9990000001"

    def test_silver_billed_amount_correct(self, spark):
        silver = spark.read.format("delta").load(SILVER_PATH_INT)
        row = silver.filter(silver.claim_id == "CLM-MCK-20260101-001").first()
        assert row.billed_amount == 350.0


@pytest.mark.integration
class TestAIValidationStage:
    def test_denial_risk_scorer_on_fhir_file(self):
        from src.ai_validation.denial_risk_scorer import (
            calculate_denial_risk,
            reset_duplicate_store,
        )

        reset_duplicate_store()
        fhir = json.loads(FHIR_FILE.read_text())
        result = calculate_denial_risk(fhir)
        assert "denial_risk_score" in result
        assert "provider_explanation" in result
        assert result["ai_decision_only"] is False
        assert result["reconsideration_right"] is True

    def test_cms_fields_always_present(self):
        from src.ai_validation.denial_risk_scorer import (
            calculate_denial_risk,
            reset_duplicate_store,
        )

        reset_duplicate_store()
        fhir = json.loads(FHIR_FILE.read_text())
        result = calculate_denial_risk(fhir)
        cms_fields = [
            "human_review_required",
            "ai_decision_only",
            "reconsideration_right",
            "decision_basis",
            "primary_driver",
        ]
        assert all(f in result for f in cms_fields)


@pytest.mark.integration
class TestPayerSimulationStage:
    def test_eligible_claim_adjudicates_as_paid(self):
        from src.ingestion.fhir_mapper import unwrap_fhir
        from src.payer_simulation.payer_engine import adjudicate_claim

        data = json.loads(FHIR_FILE.read_text())
        fhir = unwrap_fhir(data)[0]  # First Claim from Bundle
        result = adjudicate_claim(fhir)
        assert result["status"] == "paid"
        assert result["paid_amount"] == 280.0
        assert result["cas_code"] == "CO-45"

    def test_835_remittance_generated(self):
        from src.ingestion.fhir_mapper import unwrap_fhir
        from src.payer_simulation.payer_engine import adjudicate_claim
        from src.payer_simulation.remittance_generator import generate_835

        data = json.loads(FHIR_FILE.read_text())
        fhir = unwrap_fhir(data)[0]  # First Claim from Bundle
        adj = adjudicate_claim(fhir)
        edi_835 = generate_835(adj)
        assert edi_835.startswith("ISA")
        assert "ST*835" in edi_835
        assert "CLM-MCK-20260101-001" in edi_835


@pytest.mark.integration
class TestGoldStage:
    def test_silver_to_gold_creates_3_tables(self, spark):
        from src.streaming.silver_to_gold import silver_to_gold

        counts = silver_to_gold(silver_path=SILVER_PATH_INT, gold_path=GOLD_PATH_INT)
        assert "claims_summary" in counts
        assert "provider_summary" in counts
        assert "denial_summary" in counts
        assert counts["claims_summary"] >= 1
        assert counts["provider_summary"] >= 1

    def test_gold_claims_summary_has_risk_tier(self, spark):
        gold = spark.read.format("delta").load(f"{GOLD_PATH_INT}/claims_summary")
        row = gold.first()
        assert row.risk_tier in ("LOW", "MEDIUM", "HIGH")

    def test_gold_provider_summary_aggregates_correctly(self, spark):
        gold = spark.read.format("delta").load(f"{GOLD_PATH_INT}/provider_summary")
        row = gold.filter(gold.billing_npi == "9990000001").first()
        assert row is not None
        assert row.claim_count >= 1
        assert row.total_billed >= 350.0
