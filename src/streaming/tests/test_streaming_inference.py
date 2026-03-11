"""
Unit tests for src/streaming/streaming_inference.py.

No live Spark cluster, Kafka broker, or model endpoint required — all
external dependencies are mocked.

Run: pytest src/streaming/tests/test_streaming_inference.py -v
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.streaming_inference import (
    _MODEL_CACHE,
    MODEL_V1_PATH,
    MODEL_V2_PATH,
    THRESHOLD_HIGH,
    THRESHOLD_MEDIUM,
    TOPIC_AUTO,
    TOPIC_RAW,
    TOPIC_REJECTED,
    TOPIC_REVIEW,
    _build_features,
    _load_local_model,
    _route_topic,
    _score_via_endpoint,
)

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_fhir_json(
    claim_id: str = "CLM-MCK-20260101-001",
    member: str = "MCKMEMBR0001",
    total: float = 350.0,
    prior_auth: str | None = "AUTHMCK0001",
    dx_code: str = "Z00.00",
    cpt_code: str = "99213",
) -> str:
    """Build a minimal FHIR Claim JSON string for streaming inference tests."""
    return json.dumps(
        {
            "resourceType": "Claim",
            "id": claim_id,
            "status": "active",
            "created": "2026-04-12",
            "patient": {"reference": f"Patient/{member}"},
            "provider": {"identifier": {"value": "9990000001"}},
            "total": {"value": total, "currency": "USD"},
            "_prior_auth": prior_auth,
            "diagnosis": [
                {
                    "sequence": 1,
                    "diagnosisCodeableConcept": {
                        "coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": dx_code}]
                    },
                }
            ],
            "item": [
                {
                    "sequence": 1,
                    "productOrService": {
                        "coding": [{"system": "http://www.ama-assn.org/go/cpt", "code": cpt_code}]
                    },
                }
            ],
        }
    )


_FHIR_JSON = _make_fhir_json()


@pytest.fixture(autouse=True)
def _reset_model_cache():
    """Clear module-level model cache between tests for isolation."""
    _MODEL_CACHE["version"] = None
    _MODEL_CACHE["model"] = None
    yield
    _MODEL_CACHE["version"] = None
    _MODEL_CACHE["model"] = None


# ── Topic constant tests ──────────────────────────────────────────────────────


class TestTopicConstants:
    """All 5 topic names must exactly match the EventHub names in .env / Makefile."""

    # ── input topic ───────────────────────────────────────────────────────────
    def test_topic_raw_is_claims_raw(self) -> None:
        assert TOPIC_RAW == "claims.raw"

    # ── output topics (risk-tier fan-out) ─────────────────────────────────────
    def test_topic_auto_is_claims_validated(self) -> None:
        assert TOPIC_AUTO == "claims.validated"

    def test_topic_review_is_claims_review_queue(self) -> None:
        assert TOPIC_REVIEW == "claims.review_queue"

    def test_topic_rejected_is_claims_rejected(self) -> None:
        assert TOPIC_REJECTED == "claims.rejected"

    # ── remittance topic (claim_processor fan-out) ────────────────────────────
    def test_topic_remittance_constant_is_in_claim_processor(self) -> None:
        from src.streaming.claim_processor import TOPIC_REMITTANCE

        assert TOPIC_REMITTANCE == "remittance.835"

    # ── routing thresholds ────────────────────────────────────────────────────
    def test_threshold_high_is_70(self) -> None:
        assert THRESHOLD_HIGH == 70

    def test_threshold_medium_is_40(self) -> None:
        assert THRESHOLD_MEDIUM == 40


# ── Routing tests ─────────────────────────────────────────────────────────────


class TestRouting:
    """_route_topic maps 0.0-1.0 probability floats to the correct output topic."""

    def test_score_0_routes_to_auto(self) -> None:
        assert _route_topic(0.0) == TOPIC_AUTO

    def test_score_39_routes_to_auto(self) -> None:
        assert _route_topic(0.39) == TOPIC_AUTO

    def test_score_40_routes_to_review(self) -> None:
        assert _route_topic(0.40) == TOPIC_REVIEW

    def test_score_69_routes_to_review(self) -> None:
        assert _route_topic(0.699) == TOPIC_REVIEW

    def test_score_70_routes_to_rejected(self) -> None:
        assert _route_topic(0.70) == TOPIC_REJECTED

    def test_score_100_routes_to_rejected(self) -> None:
        assert _route_topic(1.0) == TOPIC_REJECTED

    def test_boundary_just_below_medium_is_auto(self) -> None:
        """int(0.399 * 100) == 39 — below THRESHOLD_MEDIUM → TOPIC_AUTO."""
        assert _route_topic(0.399) == TOPIC_AUTO

    def test_boundary_just_below_high_is_review(self) -> None:
        """int(0.699 * 100) == 69 — below THRESHOLD_HIGH → TOPIC_REVIEW."""
        assert _route_topic(0.699) == TOPIC_REVIEW


# ── Feature extraction tests ──────────────────────────────────────────────────


class TestBuildFeatures:
    """_build_features extracts 6 floats from a FHIR JSON string."""

    def test_returns_list_of_six_floats(self) -> None:
        features = _build_features(_FHIR_JSON)
        assert len(features) == 6
        assert all(isinstance(f, float) for f in features)

    def test_billed_amount_extracted(self) -> None:
        features = _build_features(_FHIR_JSON)
        assert features[0] == 350.0

    def test_prior_auth_is_1_when_present(self) -> None:
        features = _build_features(_FHIR_JSON)
        assert features[1] == 1

    def test_prior_auth_is_0_when_absent(self) -> None:
        c = json.loads(_FHIR_JSON)
        del c["_prior_auth"]
        features = _build_features(json.dumps(c))
        assert features[1] == 0

    def test_eligible_member_sets_flag(self) -> None:
        features = _build_features(_FHIR_JSON)
        assert features[4] == 1

    def test_ineligible_member_clears_flag(self) -> None:
        c = json.loads(_FHIR_JSON)
        c["patient"]["reference"] = "Patient/UNKNOWN_MEMBER"
        features = _build_features(json.dumps(c))
        assert features[4] == 0

    def test_invalid_json_returns_conservative_fallback(self) -> None:
        features = _build_features("not-valid-json{{{")
        assert features == [0.0, 0.0, 1.0, 1.0, 0.0, 0.0]


# ── DENIAL_MODEL_VERSION switch tests ────────────────────────────────────────


class TestLocalModelSwitch:
    """_load_local_model hot-switches between v1 (pkl) and v2 (json) via env var."""

    def test_v1_loads_joblib_pkl(self, monkeypatch) -> None:
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v1")
        mock_model = MagicMock()
        with patch("joblib.load", return_value=mock_model) as mock_jl:
            model, version = _load_local_model()
        assert version == "v1"
        assert model is mock_model
        mock_jl.assert_called_once_with(str(MODEL_V1_PATH))

    def test_v2_loads_lightgbm_booster(self, monkeypatch) -> None:
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v2")
        mock_booster = MagicMock()
        with patch("lightgbm.Booster", return_value=mock_booster) as mock_lgb:
            model, version = _load_local_model()
        assert version == "v2"
        assert model is mock_booster
        mock_lgb.assert_called_once_with(model_file=str(MODEL_V2_PATH))

    def test_version_change_triggers_reload(self, monkeypatch) -> None:
        """Switching DENIAL_MODEL_VERSION causes cache miss and reloads the model."""
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v1")
        mock_v1 = MagicMock()
        mock_v2 = MagicMock()

        with patch("joblib.load", return_value=mock_v1):
            model_first, ver_first = _load_local_model()

        assert ver_first == "v1"
        assert model_first is mock_v1

        # Hot-switch: update env var without restarting
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v2")
        with patch("lightgbm.Booster", return_value=mock_v2):
            model_second, ver_second = _load_local_model()

        assert ver_second == "v2"
        assert model_second is mock_v2

    def test_same_version_uses_cache(self, monkeypatch) -> None:
        """Calling _load_local_model twice with same version does not reload."""
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v1")
        mock_model = MagicMock()

        with patch("joblib.load", return_value=mock_model) as mock_jl:
            _load_local_model()
            _load_local_model()
            assert mock_jl.call_count == 1  # loaded once, served from cache


# ── Endpoint scoring tests ────────────────────────────────────────────────────


class TestScoreViaEndpoint:
    """_score_via_endpoint falls back to local model when endpoint is disabled."""

    def test_endpoint_disabled_uses_local_model(self, monkeypatch) -> None:
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v1")
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = [[0.3, 0.25]]

        with (
            patch(
                "src.streaming.streaming_inference.ENDPOINT_ENABLED",
                False,
            ),
            patch(
                "src.streaming.streaming_inference._load_local_model",
                return_value=(mock_model, "v1"),
            ),
        ):
            score = _score_via_endpoint(_FHIR_JSON)

        assert 0.0 <= score <= 1.0
        mock_model.predict_proba.assert_called_once()

    def test_endpoint_disabled_v2_uses_predict(self, monkeypatch) -> None:
        monkeypatch.setenv("DENIAL_MODEL_VERSION", "v2")
        mock_model = MagicMock()
        mock_model.predict.return_value = [0.65]

        with (
            patch("src.streaming.streaming_inference.ENDPOINT_ENABLED", False),
            patch(
                "src.streaming.streaming_inference._load_local_model",
                return_value=(mock_model, "v2"),
            ),
        ):
            score = _score_via_endpoint(_FHIR_JSON)

        assert score == pytest.approx(0.65)
        mock_model.predict.assert_called_once()

    def test_local_model_error_returns_0_5(self) -> None:
        with (
            patch("src.streaming.streaming_inference.ENDPOINT_ENABLED", False),
            patch(
                "src.streaming.streaming_inference._load_local_model",
                side_effect=RuntimeError("disk error"),
            ),
        ):
            score = _score_via_endpoint(_FHIR_JSON)

        assert score == pytest.approx(0.5)

    def test_endpoint_error_returns_0_5(self) -> None:
        with (
            patch("src.streaming.streaming_inference.ENDPOINT_ENABLED", True),
            patch("src.streaming.streaming_inference.ENDPOINT_URL", "http://mock-endpoint"),
            patch("src.streaming.streaming_inference.requests.post") as mock_post,
        ):
            mock_post.side_effect = ConnectionError("timeout")
            score = _score_via_endpoint(_FHIR_JSON)

        assert score == pytest.approx(0.5)

    def test_endpoint_enabled_returns_prediction(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"predictions": [0.82]}

        with (
            patch("src.streaming.streaming_inference.ENDPOINT_ENABLED", True),
            patch("src.streaming.streaming_inference.ENDPOINT_URL", "http://mock-endpoint"),
            patch(
                "src.streaming.streaming_inference.requests.post",
                return_value=mock_resp,
            ),
        ):
            score = _score_via_endpoint(_FHIR_JSON)

        assert score == pytest.approx(0.82)
