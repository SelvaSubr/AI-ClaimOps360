from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

_FAKE_SHAP = np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0])
_FAKE_SV = [np.zeros((1, 6)), _FAKE_SHAP.reshape(1, -1)]


def _make_fake_model():
    m = MagicMock()

    def _score(X):
        return 0.85 if (float(X[0][1]) == 0.0 and float(X[0][0]) >= 1000.0) else 0.45

    m.predict.side_effect = lambda X: [_score(X)]
    m.predict_proba.side_effect = lambda X: [[1.0 - _score(X), _score(X)]]
    return m


@pytest.fixture(autouse=True)
def _mock_explainability_model():
    v1_path = MagicMock()
    v1_path.exists.return_value = True
    v2_path = MagicMock()
    v2_path.exists.return_value = True
    te = MagicMock()
    te.return_value.shap_values.return_value = _FAKE_SV

    with (
        patch("src.ai_validation.explainability.V1_MODEL_PATH", v1_path),
        patch("src.ai_validation.explainability.V2_MODEL_PATH", v2_path),
        patch("joblib.load", return_value=_make_fake_model()),
        patch("lightgbm.Booster", return_value=_make_fake_model()),
        patch("shap.TreeExplainer", te),
    ):
        yield
