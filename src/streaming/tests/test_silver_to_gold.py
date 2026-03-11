"""
Unit tests for src/streaming/silver_to_gold.py.

No live Spark cluster required — SparkSession, Delta I/O, and pyspark.sql.functions
are fully mocked so tests run without a JVM.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.streaming.silver_to_gold import GOLD_PATH, SILVER_PATH, create_spark, silver_to_gold

# ── helpers ───────────────────────────────────────────────────────────────────


def _mock_spark(row_count: int = 3) -> MagicMock:
    """Return a mock SparkSession with chainable DataFrame stubs."""
    mock_df = MagicMock()
    # groupBy().agg() returns the same mock df so subsequent .withColumn / .orderBy work
    mock_df.groupBy.return_value.agg.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.orderBy.return_value = mock_df
    mock_df.write.format.return_value.mode.return_value.save.return_value = None
    mock_df.count.return_value = row_count

    spark = MagicMock()
    # Both Silver read and post-write count reads go through the same mock
    spark.read.format.return_value.load.return_value = mock_df
    return spark


def _patch_F():
    """Return a context-manager that patches pyspark.sql.functions (F) in silver_to_gold."""
    return patch("src.streaming.silver_to_gold.F", MagicMock())


# ── Module-level defaults ─────────────────────────────────────────────────────


class TestModuleDefaults:
    """SILVER_PATH and GOLD_PATH fall back to /tmp paths when env vars are unset."""

    def test_silver_path_contains_silver(self) -> None:
        """SILVER_PATH contains 'silver' substring as a sensible default."""
        assert "silver" in SILVER_PATH.lower()

    def test_gold_path_contains_gold(self) -> None:
        """GOLD_PATH contains 'gold' substring as a sensible default."""
        assert "gold" in GOLD_PATH.lower()


# ── create_spark ─────────────────────────────────────────────────────────────


class TestCreateSpark:
    """create_spark delegates to claim_consumer.create_spark."""

    def test_delegates_to_claim_consumer_create_spark(self) -> None:
        """create_spark calls _cs with the 'claims-silver-to-gold' app name."""
        mock_session = MagicMock()
        with patch(
            "src.streaming.claim_consumer.create_spark", return_value=mock_session
        ) as mock_cs:
            result = create_spark()

        mock_cs.assert_called_once_with("claims-silver-to-gold")
        assert result is mock_session


# ── silver_to_gold — happy path ───────────────────────────────────────────────


class TestSilverToGoldHappyPath:
    """silver_to_gold writes all three Gold tables and returns correct counts."""

    @pytest.fixture(autouse=True)
    def _patch_pyspark_functions(self):
        """Patch pyspark.sql.functions so tests run without a JVM/SparkContext."""
        with _patch_F():
            yield

    @pytest.fixture()
    def spark(self) -> MagicMock:
        return _mock_spark(row_count=4)

    @pytest.fixture()
    def result(self, spark: MagicMock, tmp_path) -> dict[str, int]:
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            return silver_to_gold(
                silver_path=str(tmp_path / "silver"),
                gold_path=str(tmp_path / "gold"),
            )

    def test_returns_dict_with_three_keys(self, result: dict) -> None:
        """Return value is a dict with exactly the three Gold table names as keys."""
        assert set(result.keys()) == {
            "claims_summary",
            "provider_summary",
            "denial_summary",
        }

    def test_all_counts_are_non_negative_integers(self, result: dict) -> None:
        """Every returned count is a non-negative integer."""
        for table, count in result.items():
            assert isinstance(count, int), f"{table} count is not int: {count!r}"
            assert count >= 0, f"{table} count is negative: {count}"

    def test_silver_delta_table_is_read(self, spark: MagicMock, tmp_path) -> None:
        """silver_to_gold reads from the supplied silver_path as a delta table."""
        silver_p = str(tmp_path / "silver")
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            silver_to_gold(silver_path=silver_p, gold_path=str(tmp_path / "gold"))

        spark.read.format.assert_called_with("delta")

    def test_groupby_billing_npi_called_for_provider_summary(
        self, spark: MagicMock, tmp_path
    ) -> None:
        """groupBy('billing_npi') is called to produce provider_summary."""
        mock_df = spark.read.format.return_value.load.return_value
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            silver_to_gold(
                silver_path=str(tmp_path / "silver"),
                gold_path=str(tmp_path / "gold"),
            )

        groupby_calls = [str(c) for c in mock_df.groupBy.call_args_list]
        assert any("billing_npi" in c for c in groupby_calls)

    def test_groupby_primary_diagnosis_called_for_denial_summary(
        self, spark: MagicMock, tmp_path
    ) -> None:
        """groupBy('primary_diagnosis_code') is called to produce denial_summary."""
        mock_df = spark.read.format.return_value.load.return_value
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            silver_to_gold(
                silver_path=str(tmp_path / "silver"),
                gold_path=str(tmp_path / "gold"),
            )

        groupby_calls = [str(c) for c in mock_df.groupBy.call_args_list]
        assert any("primary_diagnosis_code" in c for c in groupby_calls)

    def test_three_delta_saves_are_executed(self, spark: MagicMock, tmp_path) -> None:
        """Exactly three Delta write-save operations are performed (one per Gold table)."""
        mock_df = spark.read.format.return_value.load.return_value
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            silver_to_gold(
                silver_path=str(tmp_path / "silver"),
                gold_path=str(tmp_path / "gold"),
            )

        save_mock = mock_df.write.format.return_value.mode.return_value.save
        assert save_mock.call_count == 3

    def test_gold_subpaths_contain_table_names(self, spark: MagicMock, tmp_path) -> None:
        """Each Gold save path includes the expected table name."""
        mock_df = spark.read.format.return_value.load.return_value
        gold_p = str(tmp_path / "gold")
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            silver_to_gold(silver_path=str(tmp_path / "silver"), gold_path=gold_p)

        save_paths = [
            c.args[0]
            for c in mock_df.write.format.return_value.mode.return_value.save.call_args_list
        ]
        expected_tables = {"claims_summary", "provider_summary", "denial_summary"}
        for table in expected_tables:
            assert any(
                table in p for p in save_paths
            ), f"No save path found for '{table}'. Paths: {save_paths}"

    def test_all_saves_use_delta_overwrite_mode(self, spark: MagicMock, tmp_path) -> None:
        """All three writes use format('delta') and mode('overwrite')."""
        mock_df = spark.read.format.return_value.load.return_value
        with patch("src.streaming.claim_consumer.create_spark", return_value=spark):
            silver_to_gold(
                silver_path=str(tmp_path / "silver"),
                gold_path=str(tmp_path / "gold"),
            )

        mode_calls = [str(c) for c in mock_df.write.format.return_value.mode.call_args_list]
        assert all("overwrite" in c for c in mode_calls)
