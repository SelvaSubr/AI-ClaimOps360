"""Silver → Gold: aggregates Silver Delta into claims_summary, provider_summary, and denial_summary."""

from __future__ import annotations

import os

import structlog
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), "..", "simulation.config"),
    override=False,
)

log = structlog.get_logger()

SILVER_PATH: str = os.getenv("SILVER_PATH", "/tmp/delta/silver")  # nosec B108
GOLD_PATH: str = os.getenv("GOLD_PATH", "/tmp/delta/gold")  # nosec B108


def create_spark() -> SparkSession:
    """Create or reuse a SparkSession with Delta Lake extensions configured.

    Delegates to claim_consumer.create_spark to avoid duplicate Spark config.

    Returns:
        SparkSession: Active session tagged 'claims-silver-to-gold'.
    """
    from src.streaming.claim_consumer import create_spark as _cs

    return _cs("claims-silver-to-gold")


def silver_to_gold(
    silver_path: str = SILVER_PATH,  # noqa: B008
    gold_path: str = GOLD_PATH,  # noqa: B008
) -> dict[str, int]:
    """Aggregate Silver Delta table into three Gold analytics tables.

    Reads Silver Delta, produces three overwrite Gold Delta tables:

    1. claims_summary — one row per claim_id:
       claim_id, billing_npi, billed_amount, service_date, prior_auth_number,
       risk_tier (LOW / MEDIUM / HIGH based on billed_amount).

    2. provider_summary — one row per billing_npi:
       claim_count, total_billed, avg_billed, unique_patients, prior_auth_count,
       denial_rate (prior_auth_count / claim_count).

    3. denial_summary — one row per primary_diagnosis_code:
       claim_count, avg_billed, total_billed, prior_auth_count, denial_rate.
       Ordered by claim_count descending.

    Args:
        silver_path: Path to the Delta Silver table (default: $SILVER_PATH env,
                     then /tmp/delta/silver).  # nosec B108
        gold_path:   Path prefix for Gold Delta tables (default: $GOLD_PATH env,
                     then /tmp/delta/gold).  # nosec B108

    Returns:
        dict[str, int]: Row counts for each Gold table written,
            e.g. {'claims_summary': 10, 'provider_summary': 5, 'denial_summary': 3}.
    """
    spark = create_spark()
    silver = spark.read.format("delta").load(silver_path)

    log.info("silver_to_gold_start", silver_path=silver_path, gold_path=gold_path)

    RISK_TIER_HIGH_USD = float(os.getenv("RISK_TIER_HIGH_USD", "5000"))
    RISK_TIER_MEDIUM_USD = float(os.getenv("RISK_TIER_MEDIUM_USD", "1000"))

    # ── Gold Table 1: claims_summary ───────────────────────────────────────────
    claims_summary = silver.select(
        "claim_id",
        "billing_npi",
        "billed_amount",
        "service_date",
        "prior_auth_number",
    ).withColumn(
        "risk_tier",
        F.expr(
            f"CASE WHEN billed_amount >= {RISK_TIER_HIGH_USD} THEN 'HIGH'"
            f" WHEN billed_amount >= {RISK_TIER_MEDIUM_USD} THEN 'MEDIUM'"
            " ELSE 'LOW' END"
        ),
    )
    claims_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/claims_summary")
    claims_count = spark.read.format("delta").load(f"{gold_path}/claims_summary").count()
    log.info("gold_table_written", table="claims_summary", row_count=claims_count)

    # ── Gold Table 2: provider_summary ─────────────────────────────────────────
    provider_summary = (
        silver.groupBy("billing_npi")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("billed_amount").alias("total_billed"),
            F.avg("billed_amount").alias("avg_billed"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.sum(F.when(F.col("prior_auth_number").isNotNull(), 1).otherwise(0)).alias(
                "prior_auth_count"
            ),
        )
        .withColumn(
            "denial_rate",
            F.col("prior_auth_count") / F.col("claim_count"),
        )
    )
    provider_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/provider_summary")
    provider_count = spark.read.format("delta").load(f"{gold_path}/provider_summary").count()
    log.info("gold_table_written", table="provider_summary", row_count=provider_count)

    # ── Gold Table 3: denial_summary ───────────────────────────────────────────
    denial_summary = (
        silver.groupBy("primary_diagnosis_code")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.avg("billed_amount").alias("avg_billed"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum(F.when(F.col("prior_auth_number").isNotNull(), 1).otherwise(0)).alias(
                "prior_auth_count"
            ),
        )
        .withColumn(
            "denial_rate",
            F.col("prior_auth_count") / F.col("claim_count"),
        )
        .orderBy(F.col("claim_count").desc())
    )
    denial_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/denial_summary")
    denial_count = spark.read.format("delta").load(f"{gold_path}/denial_summary").count()
    log.info("gold_table_written", table="denial_summary", row_count=denial_count)

    counts = {
        "claims_summary": claims_count,
        "provider_summary": provider_count,
        "denial_summary": denial_count,
    }
    log.info("gold_complete", **counts)
    return counts


if __name__ == "__main__":
    result = silver_to_gold()
    for table, count in result.items():
        log.info("gold_written", table=table, row_count=count)
