"""Structured Streaming inference: scores FHIR claims from claims.raw, routes to output topics by risk tier."""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests
import structlog
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, StringType

from src.payer_simulation.eligibility_checker import check_eligibility

load_dotenv()
log = structlog.get_logger()

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR")

BASE_DIR = Path(os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[2])))
MODEL_V1_PATH = BASE_DIR / "src/ai_validation/models/denial_model_v1.pkl"
MODEL_V2_PATH = BASE_DIR / "src/ai_validation/models/denial_model_v2.json"

# Routing thresholds — must match bronze_to_silver.py HUMAN_REVIEW_THRESHOLD
THRESHOLD_HIGH = int(os.getenv("REVIEW_SCORE_THRESHOLD", "70"))
THRESHOLD_MEDIUM = int(os.getenv("LOW_RISK_THRESHOLD", "40"))
# < THRESHOLD_MEDIUM → auto-approved (claims.validated)

ENDPOINT_URL = os.getenv("DATABRICKS_MODEL_ENDPOINT")
ENDPOINT_ENABLED = (os.getenv("DATABRICKS_MODEL_ENDPOINT_ENABLED") or "false").lower() == "true"

# Topic names — sourced from env to match EventHub names
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "claims.raw")  # input — claims ingested here
TOPIC_AUTO = os.getenv("KAFKA_TOPIC_VALIDATED", "claims.validated")  # score < 40
TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW_QUEUE", "claims.review_queue")  # 40 <= score < 70
TOPIC_REJECTED = os.getenv("KAFKA_TOPIC_REJECTED", "claims.rejected")  # score >= 70 or parse errors

# Module-level cache — hot-switch DENIAL_MODEL_VERSION without restarting the Spark job.
_MODEL_CACHE: dict = {"version": None, "model": None}


def _load_local_model() -> tuple[object, str]:
    """Return a cached model, reloading only when DENIAL_MODEL_VERSION changes.

    Reads DENIAL_MODEL_VERSION at call time so the model version can be
    switched by updating the env var — no job restart required.

    v1 → denial_model_v1.pkl (sklearn Pipeline)
    v2 → denial_model_v2.json (LightGBM Booster)

    Returns:
        Tuple of (fitted model object, version string).
    """
    version = os.getenv("DENIAL_MODEL_VERSION")
    if _MODEL_CACHE["version"] != version:
        if version == "v2":
            import lightgbm as lgb  # noqa: PLC0415

            _MODEL_CACHE["model"] = lgb.Booster(model_file=str(MODEL_V2_PATH))
        else:
            import joblib  # noqa: PLC0415

            _MODEL_CACHE["model"] = joblib.load(str(MODEL_V1_PATH))
        _MODEL_CACHE["version"] = version
        log.info("local_model_loaded", version=version)
    return _MODEL_CACHE["model"], version


def _build_features(claim_json: str) -> list[float]:
    """Extract the 6 model features from a FHIR JSON string."""
    try:
        from datetime import date  # noqa: PLC0415

        c = json.loads(claim_json)
        member = c.get("patient", {}).get("reference", "").replace("Patient/", "")
        is_eligible = check_eligibility(member, date.today())["eligible"]
        return [
            float(c.get("total", {}).get("value", 0)),
            float(1 if c.get("_prior_auth") else 0),
            float(len(c.get("diagnosis", []))),
            float(len(c.get("item", []))),
            float(1 if is_eligible else 0),
            0.0,
        ]
    except Exception as e:
        log.warning("build_features_failed", error=str(e))
        return [0.0, 0.0, 1.0, 1.0, 0.0, 0.0]


def _score_via_endpoint(claim_json: str) -> float:
    """Call Databricks Model Serving endpoint to get denial risk probability.

    When the endpoint is disabled, falls back to local model scoring using
    the version selected by DENIAL_MODEL_VERSION (v1 pkl or v2 json).
    The env var is read at call time so the version can be changed without
    restarting the job.

    Returns:
        float: Denial risk probability 0.0–1.0. Returns 0.5 on any error
               (routes to review_queue — conservative safe default).
    """
    if not ENDPOINT_ENABLED or not ENDPOINT_URL:
        # local model fallback — DENIAL_MODEL_VERSION hot-switchable without job restart
        try:
            import numpy as np  # noqa: PLC0415

            model, version = _load_local_model()
            features = _build_features(claim_json)
            X = np.array(features).reshape(1, -1)
            if version == "v2":
                return float(model.predict(X)[0])
            return float(model.predict_proba(X)[0][1])
        except Exception as e:
            log.warning("local_model_score_failed", error=str(e))
            return 0.5
    try:
        features = _build_features(claim_json)
        resp = requests.post(
            ENDPOINT_URL,
            headers={"Authorization": f'Bearer {os.getenv("DATABRICKS_TOKEN") or ""}'},
            json={"inputs": [features]},
            timeout=10,
        )
        resp.raise_for_status()
        return float(resp.json()["predictions"][0])
    except Exception as e:
        log.warning("endpoint_score_failed", error=str(e))
        return 0.5  # Conservative: route to review_queue on error


def _route_topic(score_float: float) -> str:
    """Map a 0.0-1.0 probability to a Kafka output topic name."""
    score = int(score_float * 100)
    if score >= THRESHOLD_HIGH:
        return TOPIC_REJECTED
    if score >= THRESHOLD_MEDIUM:
        return TOPIC_REVIEW
    return TOPIC_AUTO


score_udf = udf(lambda j: _score_via_endpoint(j), FloatType())
route_udf = udf(lambda s: _route_topic(s), StringType())


def _create_streaming_spark(app_name: str = "claims-streaming-inference") -> SparkSession:
    """Create SparkSession with Delta Lake + Kafka Structured Streaming packages."""
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0," "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def run_streaming_inference(
    checkpoint_dir: str = CHECKPOINT_DIR,
    timeout_seconds: int = 120,
) -> None:
    """Start Spark Structured Streaming job that scores and routes FHIR claims.

    Reads from Kafka claims.raw, scores each claim via the local model or
    Databricks endpoint, and routes to one of three output topics based on
    denial_risk_score threshold.  Runs until timeout_seconds (dev) or
    indefinitely when timeout_seconds=0 (production).
    """
    spark = _create_streaming_spark("claims-streaming-inference")

    kafka_opts = {
        "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "subscribe": os.getenv("KAFKA_TOPIC_CLAIMS_RAW"),
        "startingOffsets": os.getenv("KAFKA_STREAMING_STARTING_OFFSETS", "latest"),
        "failOnDataLoss": "false",
    }

    raw_stream = (
        spark.readStream.format("kafka")
        .options(**kafka_opts)
        .load()
        .selectExpr("CAST(key AS STRING) AS claim_id", "CAST(value AS STRING) AS claim_json")
    )

    scored = raw_stream.withColumn("risk_score", score_udf(col("claim_json"))).withColumn(
        "output_topic", route_udf(col("risk_score"))
    )

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        for topic in [TOPIC_AUTO, TOPIC_REVIEW, TOPIC_REJECTED]:
            subset = batch_df.filter(col("output_topic") == topic)
            if subset.count() == 0:
                continue
            (
                subset.selectExpr("claim_id AS key", "claim_json AS value")
                .write.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                )
                .option("topic", topic)
                .save()
            )
            log.info("batch_routed", batch_id=batch_id, topic=topic, count=subset.count())

    query = (
        scored.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
    )
    log.info("streaming_inference_started")
    query.awaitTermination(timeout=timeout_seconds)


if __name__ == "__main__":
    run_streaming_inference()
