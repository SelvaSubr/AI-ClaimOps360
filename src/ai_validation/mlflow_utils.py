"""MLflow Model Registry helpers — register, promote, and load denial-risk models."""

from __future__ import annotations

import os

import mlflow
import structlog
from dotenv import load_dotenv
from mlflow.entities.model_registry import ModelVersion
from mlflow.tracking import MlflowClient

load_dotenv()
log = structlog.get_logger()

MLFLOW_VERSION = tuple(int(x) for x in mlflow.__version__.split(".")[:2])
USE_ALIASES = MLFLOW_VERSION >= (2, 9)

MODEL_NAME = "denial-risk-model"


def _get_client() -> MlflowClient:
    """Return an MlflowClient pointed at the configured tracking URI."""
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    return MlflowClient()


def get_latest_run_id(experiment_name: str) -> str | None:
    """
    Return the run_id of the most recently completed run in an experiment.

    Args:
        experiment_name: MLflow experiment path, e.g. '/AI-ClaimOps360/denial-risk-model'

    Returns:
        str | None: Run ID string, or None if no runs exist yet.
    """
    client = _get_client()
    experiment = client.get_experiment_by_name(experiment_name)
    if not experiment:
        log.warning("experiment_not_found", name=experiment_name)
        return None
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        log.info("no_runs_found", experiment=experiment_name)
        return None
    return runs[0].info.run_id


def register_model(
    run_id: str,
    model_name: str = MODEL_NAME,
    model_path: str = "model",
    stage: str | None = None,
) -> ModelVersion:
    """
    Register an MLflow run artifact to the Model Registry.

    Args:
        run_id: MLflow run ID containing the logged model artifact.
        model_name: Registered model name (default: MODEL_NAME).
        model_path: Artifact path within the run (default: 'model').
        stage: Optional stage to transition to after registration ('Staging', 'Production').

    Returns:
        ModelVersion: The registered model version object.
    """
    model_uri = f"runs:/{run_id}/{model_path}"
    mv = mlflow.register_model(model_uri=model_uri, name=model_name)
    log.info("model_registered", run_id=run_id, model_name=model_name, version=mv.version)
    if stage:
        client = _get_client()
        if USE_ALIASES:
            try:
                client.set_registered_model_alias(
                    name=model_name,
                    alias=stage.lower(),
                    version=mv.version,
                )
            except mlflow.exceptions.MlflowException as e:
                if "ENDPOINT_NOT_FOUND" in str(e):
                    client.transition_model_version_stage(
                        name=model_name,
                        version=mv.version,
                        stage=stage,
                        archive_existing_versions=False,
                    )
                else:
                    raise
        else:
            client.transition_model_version_stage(
                name=model_name,
                version=mv.version,
                stage=stage,
                archive_existing_versions=False,
            )
        log.info("model_stage_set", model_name=model_name, version=mv.version, stage=stage)
    return mv


def purge_registered_model(model_name: str = MODEL_NAME) -> None:
    """
    Delete all versions and the registered model entry from MLflow registry.
    Safe to call when the model does not exist (no-op).

    Args:
        model_name: Registered model name to purge (default: MODEL_NAME).
    """
    client = _get_client()
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
        for mv in versions:
            if not USE_ALIASES:
                try:
                    client.transition_model_version_stage(
                        name=model_name, version=mv.version, stage="Archived"
                    )
                except mlflow.exceptions.MlflowException:
                    pass
            client.delete_model_version(name=model_name, version=mv.version)
            log.info("model_version_deleted", model_name=model_name, version=mv.version)
        client.delete_registered_model(model_name)
        log.info("registered_model_deleted", model_name=model_name)
    except mlflow.exceptions.MlflowException as e:
        log.warning("purge_skipped", model_name=model_name, reason=str(e))


def promote_to_production(model_name: str, version: str) -> None:
    """
    Move a registered model version to the Production alias.
    Any previous Production alias holder is archived before promotion.

    Args:
        model_name: Registered model name, e.g. 'denial-risk-model'.
        version: Version string to promote, e.g. '1'.
    """
    client = _get_client()
    try:
        current = client.get_model_version_by_alias(model_name, "Production")
        if current.version != version:
            if not USE_ALIASES:
                client.transition_model_version_stage(
                    name=model_name,
                    version=current.version,
                    stage="Archived",
                )
            log.info("version_archived", model_name=model_name, version=current.version)
            client.delete_registered_model_alias(model_name, "Production")
    except mlflow.exceptions.MlflowException:
        pass
    try:
        client.set_registered_model_alias(model_name, "Production", version)
    except mlflow.exceptions.MlflowException as e:
        if "ENDPOINT_NOT_FOUND" in str(e):
            client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage="Production",
                archive_existing_versions=True,
            )
        else:
            raise
    log.info("version_promoted_to_production", model_name=model_name, version=version)


def get_production_uri(model_name: str) -> str:
    """
    Return the MLflow model URI for the Production alias.
    Used by streaming_inference.py to load the model for scoring.

    Args:
        model_name: Registered model name, e.g. 'denial-risk-model'.

    Returns:
        str: Alias-format URI, e.g. 'models:/denial-risk-model@Production'.
    """
    uri = f"models:/{model_name}@Production"
    log.info("production_model_uri", uri=uri)
    return uri


def log_model_metrics(metrics: dict, params: dict) -> str:
    """
    Log metrics and params to the currently active MLflow run.

    Args:
        metrics: Numeric metrics to log, e.g. {'auc': 0.92, 'f1': 0.88}.
        params: Hyperparameters to log, e.g. {'n_estimators': 100}.

    Returns:
        str: The active run's run_id.
    """
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    run_id: str = mlflow.active_run().info.run_id  # type: ignore[union-attr]
    log.info("metrics_logged", run_id=run_id, metric_keys=list(metrics.keys()))
    return run_id


if __name__ == "__main__":
    import mlflow.sklearn
    from sklearn.dummy import DummyClassifier

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME"))

    log.info("demo_start", model_name=MODEL_NAME)

    with mlflow.start_run(run_name="demo-v1") as run:
        clf = DummyClassifier(strategy="constant", constant=0)
        clf.fit([[0]] * 10, [0] * 10)

        log_model_metrics(
            metrics={"auc": 0.50, "f1": 0.50, "precision": 0.50},
            params={"model_type": "DummyClassifier", "strategy": "constant"},
        )
        mlflow.sklearn.log_model(clf, artifact_path="model")
        demo_run_id = run.info.run_id

    mv = register_model(run_id=demo_run_id, model_name=MODEL_NAME, model_path="model")
    log.info(
        "demo_complete",
        model_name=MODEL_NAME,
        version=mv.version,
        stage="Staging",
        production_uri=get_production_uri(MODEL_NAME),
    )
