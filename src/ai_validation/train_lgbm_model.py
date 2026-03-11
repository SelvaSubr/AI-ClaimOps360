"""ai_validation/train_lgbm_model.py — Phase 2 LightGBM V2 training.

Trains LightGBM with MLflow autolog, saves the booster to JSON, registers
to the MLflow Model Registry as the new Production version, and moves the
previous Production version (V1) to Staging.

Usage:  python -m src.ai_validation.train_lgbm_model   OR   make train-v2
"""

from __future__ import annotations

import os
from pathlib import Path

import joblib
import mlflow
import mlflow.lightgbm
import numpy as np
import structlog
from dotenv import load_dotenv
from lightgbm import LGBMClassifier
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split

from src.ai_validation.mlflow_utils import MODEL_NAME, register_model
from src.ai_validation.train_denial_model import FEATURE_NAMES, generate_training_data

load_dotenv()
log = structlog.get_logger()

V1_MODEL_PATH = Path("src/ai_validation/models/denial_model_v1.pkl")
V2_MODEL_PATH = Path("src/ai_validation/models/denial_model_v2.json")

# Hyperparameters — fixed for V2
_LGBM_PARAMS: dict = {
    "objective": "binary",
    "metric": "auc",
    "n_estimators": 200,
    "learning_rate": 0.05,
    "num_leaves": 31,
    "class_weight": "balanced",
    "random_state": 42,
    "verbose": -1,
}


def _compute_v1_auc(X: np.ndarray, y: np.ndarray) -> float | None:
    """Load the V1 sklearn pipeline and return its 5-fold cross-val AUC.

    Uses the full dataset with stratified cross-validation (same setup as
    train_denial_model.py) to avoid the inflated in-sample test-split AUC.

    Args:
        X: Full feature matrix (all rows, not just a held-out split).
        y: Binary denial labels for all rows.

    Returns:
        float | None: V1 cross-val AUC, or None if the V1 model file is absent.
    """
    if not V1_MODEL_PATH.exists():
        log.warning("v1_model_not_found", path=str(V1_MODEL_PATH))
        return None
    pipeline = joblib.load(V1_MODEL_PATH)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(pipeline, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)
    auc = float(scores.mean())
    log.info("v1_auc_computed", auc=round(auc, 4))
    return auc


def _promote_v2_demote_v1(v2_version: str) -> None:
    """Move current Production version (V1) to Staging; promote V2 to Production.

    Uses stage-based transitions (compatible with Databricks MLflow).

    Args:
        v2_version: Registry version string for the newly registered V2 model.
    """
    client = MlflowClient()

    # Move existing Production holder → Staging before promoting V2
    try:
        prod_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        for mv in prod_versions:
            if mv.version != v2_version:
                client.transition_model_version_stage(
                    name=MODEL_NAME,
                    version=mv.version,
                    stage="Staging",
                    archive_existing_versions=False,
                )
                log.info("v1_moved_to_staging", model=MODEL_NAME, version=mv.version)
    except MlflowException:
        log.info("no_existing_production_version", model=MODEL_NAME)

    # Promote V2 to Production
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=v2_version,
        stage="Production",
        archive_existing_versions=False,
    )
    log.info("v2_promoted_to_production", model=MODEL_NAME, version=v2_version)


def train_lgbm(n_samples: int = 500) -> str:
    """Train LightGBM V2, autolog to MLflow, register and promote to Production.

    Uses the same 6 features and synthetic data generator as V1 (identical
    data contract).  Computes V1 cross-val AUC for a fair side-by-side
    comparison (matches train_denial_model.py — avoids inflated in-sample AUC).
    Registers V2 to the MLflow Model Registry as Production and transitions
    the previous Production version (V1) to Staging.

    Args:
        n_samples: Number of synthetic training rows (minimum 500).

    Returns:
        str: MLflow run ID of the V2 training run.
    """
    FORCE_RETRAIN = os.getenv("FORCE_RETRAIN", "false").lower() == "true"
    if V2_MODEL_PATH.exists() and not FORCE_RETRAIN:
        log.info("model_exists_skipping", path="denial_model_v2.json")
        return ""

    V2_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Generate full dataset; cross-val V1 AUC uses all rows for a fair comparison
    X, y = generate_training_data(n_samples)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    v1_auc = _compute_v1_auc(X, y)
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")

    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME", "denial-risk-lgbm"))
        mlflow.lightgbm.autolog(log_models=True, log_input_examples=True)

        with mlflow.start_run() as run:
            model = LGBMClassifier(**_LGBM_PARAMS)
            model.fit(X_train, y_train, eval_set=[(X_test, y_test)])

            v2_auc = float(roc_auc_score(y_test, model.predict_proba(X_test)[:, 1]))
            mlflow.log_metric("test_auc", v2_auc)
            mlflow.log_param("feature_names", FEATURE_NAMES)
            if v1_auc is not None:
                mlflow.log_metric("v1_auc_baseline", v1_auc)

            model.booster_.save_model(str(V2_MODEL_PATH))
            mlflow.log_artifact(str(V2_MODEL_PATH), artifact_path="model")

            run_id = run.info.run_id

        # ── Registry: register V2, promote to Production, V1 → Staging ──────────
        mv = register_model(run_id, MODEL_NAME, "model")
        _promote_v2_demote_v1(str(mv.version))
    else:
        model = LGBMClassifier(**_LGBM_PARAMS)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
        v2_auc = float(roc_auc_score(y_test, model.predict_proba(X_test)[:, 1]))
        model.booster_.save_model(str(V2_MODEL_PATH))
        run_id = ""

    # ── V1 vs V2 AUC comparison ──────────────────────────────────────────────
    if v1_auc is not None:
        delta = v2_auc - v1_auc
        log.info(
            "model_comparison",
            model=MODEL_NAME,
            v1_auc=round(v1_auc, 4),
            v2_auc=round(v2_auc, 4),
            delta=round(delta, 4),
            v1_stage="Staging",
            v2_stage="Production",
            run_id=run_id,
            artifact=str(V2_MODEL_PATH),
        )
    else:
        log.info(
            "model_comparison",
            model=MODEL_NAME,
            v1_auc=None,
            v2_auc=round(v2_auc, 4),
            run_id=run_id,
            artifact=str(V2_MODEL_PATH),
        )

    log.info("train_lgbm_complete", run_id=run_id, v2_auc=round(v2_auc, 4))
    return run_id


if __name__ == "__main__":
    train_lgbm()
