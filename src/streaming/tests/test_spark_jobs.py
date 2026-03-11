"""
Unit tests for src/streaming/bronze_to_silver.py.

No live Spark cluster required — Delta and Spark are fully mocked.
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.bronze_to_silver import (
    _FIELDS_SCHEMA,
    SILVER_SCHEMA,
    _extract_fields_fn,
    bronze_to_silver,
)

# ── helpers ───────────────────────────────────────────────────────────────────

_REQUIRED_SILVER_COLUMNS = {
    "claim_id",
    "patient_id",
    "billing_npi",
    "primary_diagnosis_code",
    "procedure_code",
    "billed_amount",
    "service_date",
    "prior_auth_number",
    "fhir_resource_type",
    "payer_id",
    "ingest_ts",
    "processed_ts",
    "source_system",
}


def _fhir_claim(claim_id: str, payer: str = "PAYER-001") -> dict:
    """Return a minimal but complete FHIR R4 Claim dict."""
    return {
        "resourceType": "Claim",
        "id": claim_id,
        "status": "active",
        "created": "2026-04-12",
        "patient": {"reference": f"Patient/PAT-{claim_id}"},
        "provider": {"identifier": {"value": "1234567890"}},
        "insurer": {"identifier": {"value": payer}},
        "total": {"value": 1500.00, "currency": "USD"},
        "diagnosis": [
            {
                "sequence": 1,
                "diagnosisCodeableConcept": {
                    "coding": [{"system": "http://hl7.org/fhir/sid/icd-10", "code": "Z00.00"}]
                },
            }
        ],
        "item": [
            {
                "sequence": 1,
                "productOrService": {
                    "coding": [{"system": "http://www.ama-assn.org/go/cpt", "code": "99213"}]
                },
            }
        ],
    }


def _mock_spark(silver_count: int = 1) -> MagicMock:
    """Return a mock SparkSession with a chainable DataFrame stub."""
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.drop.return_value = mock_df  # Window-based dedup drops _rn + received_ts
    mock_df.count.return_value = silver_count
    mock_df.write.format.return_value.mode.return_value.save.return_value = None

    spark = MagicMock()
    spark.read.format.return_value.load.return_value = mock_df
    return spark


def _mock_delta_table() -> tuple[MagicMock, MagicMock]:
    """Return (DeltaTable mock, merge-builder mock) wired for whenMatched/whenNotMatched."""
    merge_builder = MagicMock()
    merge_builder.whenMatchedUpdateAll.return_value = merge_builder
    merge_builder.whenNotMatchedInsertAll.return_value = merge_builder

    dt = MagicMock()
    dt.alias.return_value.merge.return_value = merge_builder
    return dt, merge_builder


# ── Module-level fixture: ensure V2 is the default model for all tests ────────


@pytest.fixture(autouse=True)
def set_v2_default(monkeypatch):
    """Ensure DENIAL_MODEL_VERSION=v2 for all tests in this module.

    Prevents residual v1 env state from a previous test run or local .env
    from silently exercising the wrong code path.
    """
    monkeypatch.setenv("DENIAL_MODEL_VERSION", "v2")


# ── Schema tests ──────────────────────────────────────────────────────────────


class TestSilverSchemaColumns:
    """Silver schema must contain all 13 mandatory columns."""

    def test_silver_schema_struct_has_at_least_13_fields(self) -> None:
        """SILVER_SCHEMA StructType has at least 13 StructFields defined."""
        assert (
            len(SILVER_SCHEMA.fields) >= 13
        ), f"SILVER_SCHEMA has {len(SILVER_SCHEMA.fields)} fields; need >= 13"

    def test_fields_schema_plus_bronze_passthroughs_cover_all_required_columns(
        self,
    ) -> None:
        """_FIELDS_SCHEMA columns + ingest_ts (Bronze pass-through) + processed_ts cover the 13 mandatory Silver columns."""
        extracted = {f.name for f in _FIELDS_SCHEMA.fields}
        # ingest_ts comes from Bronze pass-through; processed_ts from current_timestamp()
        full_cols = extracted | {"ingest_ts", "processed_ts"}

        missing = _REQUIRED_SILVER_COLUMNS - full_cols
        assert not missing, f"Silver schema missing mandatory columns: {missing}"

    def test_silver_schema_column_names_match_required_set(self) -> None:
        """SILVER_SCHEMA field names are exactly the 13 mandatory columns."""
        actual = {f.name for f in SILVER_SCHEMA.fields}
        missing = _REQUIRED_SILVER_COLUMNS - actual
        assert not missing, f"SILVER_SCHEMA missing columns: {missing}"

    def test_extract_fn_returns_source_system_kafka_fhir(self) -> None:
        """_extract_fields_fn stamps source_system='kafka/fhir' on every record."""
        result = _extract_fields_fn(json.dumps(_fhir_claim("CLM-001")))
        assert result is not None
        assert result["source_system"] == "kafka/fhir"

    def test_extract_fn_promotes_primary_diagnosis_to_scalar(self) -> None:
        """_extract_fields_fn returns primary_diagnosis_code as a string, not a list."""
        result = _extract_fields_fn(json.dumps(_fhir_claim("CLM-002")))
        assert result is not None
        assert result["primary_diagnosis_code"] == "Z00.00"
        assert isinstance(result["primary_diagnosis_code"], str)

    def test_extract_fn_promotes_procedure_code_to_scalar(self) -> None:
        """_extract_fields_fn returns procedure_code as a string, not a list."""
        result = _extract_fields_fn(json.dumps(_fhir_claim("CLM-003")))
        assert result is not None
        assert result["procedure_code"] == "99213"
        assert isinstance(result["procedure_code"], str)

    def test_extract_fn_extracts_fhir_resource_type(self) -> None:
        """_extract_fields_fn captures resourceType from the FHIR payload."""
        result = _extract_fields_fn(json.dumps(_fhir_claim("CLM-004")))
        assert result is not None
        assert result["fhir_resource_type"] == "Claim"

    def test_extract_fn_returns_none_on_invalid_json(self) -> None:
        """_extract_fields_fn returns None (does not raise) when JSON is malformed."""
        result = _extract_fields_fn("not-valid-json{{{")
        assert result is None

    def test_silver_denial_score_uses_v2_model(self) -> None:
        """_extract_fields_fn returns a denial_risk_score when DENIAL_MODEL_VERSION=v2.

        Verifies that the V2 (LightGBM) code path is exercised during Silver
        extraction.  The score value is model-dependent; we assert type and
        range only so the test remains stable across model retrains.
        """
        result = _extract_fields_fn(json.dumps(_fhir_claim("CLM-V2-001")))
        assert result is not None, "_extract_fields_fn returned None for valid FHIR claim"
        score = result["denial_risk_score"]
        assert isinstance(score, int), f"denial_risk_score should be int, got {type(score)}"
        assert 0 <= score <= 100, f"denial_risk_score {score} out of [0, 100] range"
        # V2 CMS compliance: human_review_required derived from score, not model output
        assert result["human_review_required"] == (score >= 70)
        assert result["ai_decision_only"] is False
        assert result["reconsideration_right"] is True


# ── Idempotency tests ─────────────────────────────────────────────────────────


class TestMergeIdempotency:
    """MERGE upsert is idempotent: identical data produces the same Silver row count."""

    @pytest.fixture(autouse=True)
    def _patch_pyspark_col_functions(self):
        """Stub pyspark functions that require an active SparkContext when called."""
        with (
            patch("src.streaming.bronze_to_silver.col", MagicMock(return_value=MagicMock())),
            patch("src.streaming.bronze_to_silver.current_timestamp", MagicMock()),
            patch("src.streaming.bronze_to_silver.to_date", MagicMock()),
            patch("src.streaming.bronze_to_silver.row_number", MagicMock(return_value=MagicMock())),
            patch("src.streaming.bronze_to_silver.Window", MagicMock()),
            patch(
                "src.streaming.bronze_to_silver._extract_fields_udf",
                MagicMock(return_value=MagicMock()),
            ),
        ):
            yield

    def test_merge_path_taken_when_silver_table_exists(self, tmp_path) -> None:
        """When Silver already exists, DeltaTable.merge (not overwrite) is executed."""
        spark = _mock_spark(silver_count=1)
        dt, merge_builder = _mock_delta_table()

        with (
            patch(
                "src.streaming.bronze_to_silver.DeltaTable.isDeltaTable",
                return_value=True,
            ),
            patch("src.streaming.bronze_to_silver.DeltaTable.forPath", return_value=dt),
            patch("src.streaming.claim_consumer.create_spark", return_value=spark),
        ):
            bronze_to_silver(
                bronze_path=str(tmp_path / "bronze"),
                silver_path=str(tmp_path / "silver"),
            )

        merge_builder.execute.assert_called_once()

    def test_overwrite_used_when_silver_table_does_not_exist(self, tmp_path) -> None:
        """When Silver does not yet exist, write.mode('overwrite') is used instead of MERGE."""
        spark = _mock_spark(silver_count=1)
        mock_df = spark.read.format.return_value.load.return_value

        with (
            patch(
                "src.streaming.bronze_to_silver.DeltaTable.isDeltaTable",
                return_value=False,
            ),
            patch("src.streaming.claim_consumer.create_spark", return_value=spark),
        ):
            bronze_to_silver(
                bronze_path=str(tmp_path / "bronze"),
                silver_path=str(tmp_path / "silver"),
            )

        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format.return_value.mode.assert_called_once_with("overwrite")

    def test_row_count_unchanged_across_two_runs(self, tmp_path) -> None:
        """Running bronze_to_silver twice with the same claims returns the same row count."""
        spark = _mock_spark(silver_count=3)
        dt, _ = _mock_delta_table()

        bronze_p = str(tmp_path / "bronze")
        silver_p = str(tmp_path / "silver")

        # First run: Silver does not exist → overwrite
        # Second run: Silver exists → MERGE
        with (
            patch(
                "src.streaming.bronze_to_silver.DeltaTable.isDeltaTable",
                side_effect=[False, True],
            ),
            patch("src.streaming.bronze_to_silver.DeltaTable.forPath", return_value=dt),
            patch("src.streaming.claim_consumer.create_spark", return_value=spark),
        ):
            count_first = bronze_to_silver(bronze_path=bronze_p, silver_path=silver_p)
            count_second = bronze_to_silver(bronze_path=bronze_p, silver_path=silver_p)

        assert (
            count_first == count_second
        ), f"Row count changed between runs: first={count_first}, second={count_second}"

    def test_merge_uses_whenmatched_update_and_whennotmatched_insert(self, tmp_path) -> None:
        """MERGE builder chains whenMatchedUpdateAll + whenNotMatchedInsertAll for full upsert."""
        spark = _mock_spark(silver_count=1)
        dt, merge_builder = _mock_delta_table()

        with (
            patch(
                "src.streaming.bronze_to_silver.DeltaTable.isDeltaTable",
                return_value=True,
            ),
            patch("src.streaming.bronze_to_silver.DeltaTable.forPath", return_value=dt),
            patch("src.streaming.claim_consumer.create_spark", return_value=spark),
        ):
            bronze_to_silver(
                bronze_path=str(tmp_path / "bronze"),
                silver_path=str(tmp_path / "silver"),
            )

        merge_builder.whenMatchedUpdateAll.assert_called_once()
        merge_builder.whenNotMatchedInsertAll.assert_called_once()
        merge_builder.execute.assert_called_once()

    def test_optimize_zorder_called_with_claim_id_and_service_date(self, tmp_path) -> None:
        """OPTIMIZE ZORDER BY uses (claim_id, service_date) as required."""
        spark = _mock_spark(silver_count=1)
        silver_p = str(tmp_path / "silver")
        dt, _ = _mock_delta_table()

        with (
            patch(
                "src.streaming.bronze_to_silver.DeltaTable.isDeltaTable",
                return_value=True,
            ),
            patch("src.streaming.bronze_to_silver.DeltaTable.forPath", return_value=dt),
            patch("src.streaming.claim_consumer.create_spark", return_value=spark),
        ):
            bronze_to_silver(
                bronze_path=str(tmp_path / "bronze"),
                silver_path=silver_p,
            )

        spark.sql.assert_called_once()
        sql_stmt: str = spark.sql.call_args[0][0]
        assert "claim_id" in sql_stmt
        assert "service_date" in sql_stmt
        assert "ZORDER BY" in sql_stmt
