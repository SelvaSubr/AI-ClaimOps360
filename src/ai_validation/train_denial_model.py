"""ai_validation/train_denial_model.py
Phase 1 denial risk model — sklearn RandomForest + StandardScaler pipeline.
Usage:  python -m src.ai_validation.train_denial_model   OR   make train
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import joblib
import numpy as np
import structlog
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

load_dotenv()
warnings.filterwarnings("ignore", category=UserWarning)
log = structlog.get_logger()

FEATURE_NAMES: list[str] = [
    "billed_amount",
    "prior_auth",
    "diagnosis_code_count",
    "procedure_complexity",
    "provider_specialty_code",
    "days_since_service",
]
MODEL_PATH = Path("src/ai_validation/models/denial_model_v1.pkl")


def generate_training_data(n_samples: int = 500, seed: int = 42) -> tuple[np.ndarray, np.ndarray]:
    """Generate synthetic claims training data with 6 CMS-aligned features.

    Feature semantics:
        billed_amount         — total claim value (USD)
        prior_auth            — 1 = authorization present, 0 = missing
        diagnosis_code_count  — number of ICD-10 diagnosis codes on claim
        procedure_complexity  — CPT complexity score 1.0 (simple) – 5.0 (complex)
        provider_specialty_code — NPI specialty bucket 1-20
        days_since_service    — calendar days between service date and submission

    Args:
        n_samples: Number of synthetic claim rows to generate.
        seed: NumPy random seed for reproducibility.

    Returns:
        tuple[np.ndarray, np.ndarray]: (X, y) feature matrix and binary denial labels.
    """
    rng = np.random.default_rng(seed)

    billed_amount = rng.uniform(50, 2000, n_samples)
    prior_auth = rng.choice([0, 1], n_samples, p=[0.3, 0.7]).astype(float)
    diagnosis_code_count = rng.integers(1, 11, n_samples).astype(float)
    procedure_complexity = rng.uniform(1.0, 5.0, n_samples)
    provider_specialty_code = rng.integers(1, 21, n_samples).astype(float)
    days_since_service = rng.integers(0, 181, n_samples).astype(float)

    X = np.column_stack(
        [
            billed_amount,
            prior_auth,
            diagnosis_code_count,
            procedure_complexity,
            provider_specialty_code,
            days_since_service,
        ]
    )

    # Denial probability: high billed + missing auth dominate; diagnosis count
    # and late submission add signal.  procedure_complexity and
    # provider_specialty_code are noise-like → keeps AUC near target ~0.76.
    denial_prob = (
        0.50 * (billed_amount > 800).astype(float)
        + 0.35 * (prior_auth == 0).astype(float)
        + 0.10 * (diagnosis_code_count > 5).astype(float)
        + 0.05 * (days_since_service > 90).astype(float)
    )
    denial_prob = np.clip(denial_prob, 0.0, 1.0)
    y = rng.binomial(1, denial_prob).astype(int)

    log.info("training_data_generated", n_samples=n_samples, denial_rate=float(y.mean()))
    return X, y


def train_model(n_samples: int = 500) -> Pipeline:
    """Train RF pipeline, cross-validate AUC, save to disk.

    Trains a StandardScaler → RandomForestClassifier pipeline on synthetic
    claims data, evaluates with 5-fold stratified cross-validation, and
    persists the fitted pipeline to MODEL_PATH.

    Args:
        n_samples: Number of synthetic training rows (minimum 500).

    Returns:
        Pipeline: Fitted sklearn Pipeline ready for predict_proba().
    """
    FORCE_RETRAIN = os.getenv("FORCE_RETRAIN", "false").lower() == "true"
    if MODEL_PATH.exists() and not FORCE_RETRAIN:
        log.info("model_exists_skipping", path="denial_model_v1.pkl")
        return joblib.load(MODEL_PATH)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    X, y = generate_training_data(n_samples)

    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "clf",
                RandomForestClassifier(
                    n_estimators=100,
                    max_depth=6,
                    class_weight="balanced",
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(pipeline, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)
    log.info("cross_val_complete", mean_auc=round(float(scores.mean()), 4))

    pipeline.fit(X, y)
    joblib.dump(pipeline, MODEL_PATH)
    auc_mean = round(float(scores.mean()), 4)
    auc_std = round(float(scores.std()), 4)
    log.info("model_saved", path=str(MODEL_PATH), cross_val_auc=auc_mean, cross_val_std=auc_std)

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if tracking_uri:
        import mlflow
        import mlflow.sklearn

        from src.ai_validation.mlflow_utils import MODEL_NAME, register_model

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME", "denial-risk-sklearn"))
        with mlflow.start_run(run_name="v1-sklearn-rf") as run:
            mlflow.log_params(
                {
                    "model_type": "RandomForestClassifier",
                    "n_estimators": 100,
                    "max_depth": 6,
                    "n_samples": n_samples,
                }
            )
            mlflow.log_metrics({"cv_auc_mean": auc_mean, "cv_auc_std": auc_std})
            mlflow.sklearn.log_model(pipeline, artifact_path="model")
            run_id = run.info.run_id
        mv = register_model(run_id, MODEL_NAME, "model", stage="Staging")
        log.info("v1_registered_staging", run_id=run_id, version=mv.version)

    return pipeline


if __name__ == "__main__":
    train_model()
