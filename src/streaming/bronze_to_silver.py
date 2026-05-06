"""Bronze → Silver: parse fhir_json, score denial risk (DENIAL_MODEL_VERSION), MERGE-upsert on claim_id."""

from __future__ import annotations

import json
import os

import structlog
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number, to_date, udf
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

load_dotenv()
log = structlog.get_logger()

BRONZE_PATH: str | None = os.getenv("BRONZE_PATH")
SILVER_PATH: str | None = os.getenv("SILVER_PATH")

# Fields extracted from fhir_json by the UDF (source_system is stamped here).
# Includes 7 CMS-mandatory AI columns scored inline during transformation.
_FIELDS_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("billing_npi", StringType(), True),
        StructField("primary_diagnosis_code", StringType(), True),
        StructField("procedure_code", StringType(), True),
        StructField("billed_amount", DoubleType(), True),
        StructField("service_date", StringType(), True),  # cast to DateType after select
        StructField("prior_auth_number", StringType(), True),
        StructField("fhir_resource_type", StringType(), True),
        StructField("payer_id", StringType(), True),
        StructField("source_system", StringType(), True),
        # ── 7 CMS-mandatory AI columns ────────────────────────────────────────
        StructField("denial_risk_score", IntegerType(), True),
        StructField("human_review_required", BooleanType(), True),
        StructField("provider_explanation", StringType(), True),
        StructField("ai_decision_only", BooleanType(), True),
        StructField("reconsideration_right", BooleanType(), True),
        StructField("primary_driver", StringType(), True),
        StructField("decision_basis", StringType(), True),  # JSON-serialized dict
    ]
)

# Silver target schema — base columns + 7 CMS AI columns (20 total).
SILVER_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), True),
        StructField("billing_npi", StringType(), True),
        StructField("primary_diagnosis_code", StringType(), True),
        StructField("procedure_code", StringType(), True),
        StructField("billed_amount", DoubleType(), True),
        StructField("service_date", DateType(), True),
        StructField("prior_auth_number", StringType(), True),
        StructField("fhir_resource_type", StringType(), True),
        StructField("payer_id", StringType(), True),
        StructField("ingest_ts", TimestampType(), True),
        StructField("processed_ts", TimestampType(), True),
        StructField("source_system", StringType(), True),
        # ── 7 CMS-mandatory AI columns ────────────────────────────────────────
        StructField("denial_risk_score", IntegerType(), True),
        StructField("human_review_required", BooleanType(), True),
        StructField("provider_explanation", StringType(), True),
        StructField("ai_decision_only", BooleanType(), True),
        StructField("reconsideration_right", BooleanType(), True),
        StructField("primary_driver", StringType(), True),
        StructField("decision_basis", StringType(), True),  # JSON-serialized dict
    ]
)


def _extract_fields_fn(fhir_json_str: str) -> dict | None:
    """Extract Silver fields from a FHIR R4 Claim JSON string and score denial risk.

    Parses the raw JSON, promotes scalar values into typed Silver columns, and
    calls calculate_denial_risk() to populate all 7 CMS-mandatory AI columns.
    Returns None on any parse failure so bad records are filtered downstream
    rather than crashing the job.

    Args:
        fhir_json_str: Raw FHIR JSON string stored in the Bronze fhir_json column.

    Returns:
        dict matching _FIELDS_SCHEMA (base fields + 7 CMS AI columns), or None.
    """
    result = None
    try:
        c = json.loads(fhir_json_str)

        patient_ref = c.get("patient", {}).get("reference", "")
        patient_id = patient_ref.replace("Patient/", "") if patient_ref else None

        # First diagnosis code (primary)
        primary_dx: str | None = None
        diagnoses = c.get("diagnosis", [])
        if diagnoses:
            try:
                primary_dx = diagnoses[0]["diagnosisCodeableConcept"]["coding"][0]["code"]
            except (KeyError, IndexError):
                pass

        # First procedure / service code
        procedure_code: str | None = None
        items = c.get("item", [])
        if items:
            try:
                procedure_code = items[0]["productOrService"]["coding"][0]["code"]
            except (KeyError, IndexError):
                pass

        # Payer identifier — try insurer.identifier.value then insurer.reference
        insurer = c.get("insurer", {})
        payer_id: str = insurer.get("identifier", {}).get("value", "") or insurer.get(
            "reference", ""
        )

        # ── CMS AI columns ───────────────────────────────────────────────────
        try:
            from src.ai_validation.denial_risk_scorer import calculate_denial_risk  # noqa: PLC0415

            cms = calculate_denial_risk(c)
        except Exception as _score_exc:
            log.warning("cms_scoring_failed", error=str(_score_exc))
            cms = {
                "denial_risk_score": 0,
                "human_review_required": False,
                "provider_explanation": "scoring_unavailable",
                "ai_decision_only": False,
                "reconsideration_right": True,
                "primary_driver": "unknown",
                "decision_basis": {},
            }

        result = {
            "claim_id": c.get("id", ""),
            "patient_id": patient_id,
            "billing_npi": c.get("provider", {}).get("identifier", {}).get("value", ""),
            "primary_diagnosis_code": primary_dx,
            "procedure_code": procedure_code,
            "billed_amount": float(c.get("total", {}).get("value", 0)),
            "service_date": c.get("created", ""),
            "prior_auth_number": c.get("_prior_auth"),
            "fhir_resource_type": c.get("resourceType", ""),
            "payer_id": payer_id,
            "source_system": "kafka/fhir",
            # ── 7 CMS AI columns ──────────────────────────────────────────────
            "denial_risk_score": int(cms.get("denial_risk_score", 0)),
            "human_review_required": bool(cms.get("human_review_required", False)),
            "provider_explanation": str(cms.get("provider_explanation", "")),
            "ai_decision_only": bool(cms.get("ai_decision_only", False)),
            "reconsideration_right": bool(cms.get("reconsideration_right", True)),
            "primary_driver": str(cms.get("primary_driver", "")),
            "decision_basis": json.dumps(cms.get("decision_basis", {})),
        }
    except Exception as exc:
        log.warning("fhir_parse_error", error=str(exc))
    return result


_extract_fields_udf = udf(_extract_fields_fn, _FIELDS_SCHEMA)


def bronze_to_silver(
    bronze_path: str = BRONZE_PATH,  # noqa: B008
    silver_path: str = SILVER_PATH,  # noqa: B008
) -> int:
    """Transform Delta Bronze to Silver with typed columns and idempotent MERGE.

    Reads fhir_json from Bronze, extracts base Silver columns plus all 7
    CMS-mandatory AI scoring columns, and MERGE-upserts into Silver on
    claim_id.  Running this function multiple times with identical source
    data leaves the Silver row count unchanged (idempotent).

    After every merge (not on initial write), runs:
        OPTIMIZE delta.`<silver_path>` ZORDER BY (claim_id, service_date)

    Args:
        bronze_path: Path to the Delta Bronze table (default: $BRONZE_PATH).
        silver_path: Path to the Delta Silver table (default: $SILVER_PATH).

    Returns:
        int: Row count in Silver table after the merge.
    """
    from src.streaming.claim_consumer import create_spark as _cs  # reuse shared builder

    spark: SparkSession = _cs("claims-bronze-to-silver")

    bronze = spark.read.format("delta").load(bronze_path)

    _dedup_window = Window.partitionBy("claim_id").orderBy(col("received_ts").desc())

    silver_df = (
        bronze.withColumn("_f", _extract_fields_udf(col("fhir_json")))
        .select(
            col("_f.claim_id").alias("claim_id"),
            col("_f.patient_id").alias("patient_id"),
            col("_f.billing_npi").alias("billing_npi"),
            col("_f.primary_diagnosis_code").alias("primary_diagnosis_code"),
            col("_f.procedure_code").alias("procedure_code"),
            col("_f.billed_amount").alias("billed_amount"),
            to_date(col("_f.service_date"), "yyyy-MM-dd").alias("service_date"),
            col("_f.prior_auth_number").alias("prior_auth_number"),
            col("_f.fhir_resource_type").alias("fhir_resource_type"),
            col("_f.payer_id").alias("payer_id"),
            col("ingest_ts"),  # pass-through from Bronze
            col("received_ts"),  # used for deterministic dedup ordering
            current_timestamp().alias("processed_ts"),
            col("_f.source_system").alias("source_system"),
            # ── 7 CMS AI columns ──────────────────────────────────────────────
            col("_f.denial_risk_score").alias("denial_risk_score"),
            col("_f.human_review_required").alias("human_review_required"),
            col("_f.provider_explanation").alias("provider_explanation"),
            col("_f.ai_decision_only").alias("ai_decision_only"),
            col("_f.reconsideration_right").alias("reconsideration_right"),
            col("_f.primary_driver").alias("primary_driver"),
            col("_f.decision_basis").alias("decision_basis"),
        )
        .filter(col("claim_id").isNotNull() & (col("claim_id") != ""))
        .withColumn("_rn", row_number().over(_dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn", "received_ts")
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        dt = DeltaTable.forPath(spark, silver_path)
        (
            dt.alias("target")
            .merge(silver_df.alias("source"), "target.claim_id = source.claim_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        spark.sql(
            f"OPTIMIZE delta.`{silver_path}` ZORDER BY (claim_id, service_date)"
        )  # nosec B608
    else:
        silver_df.write.format("delta").mode("overwrite").save(silver_path)

    count: int = spark.read.format("delta").load(silver_path).count()
    log.info("silver_complete", count=count, path=silver_path)
    return count


if __name__ == "__main__":
    n = bronze_to_silver()
    log.info("bronze_to_silver_done", row_count=n)
