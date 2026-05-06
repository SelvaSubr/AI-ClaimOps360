# model_serving.tf
# Databricks Model Serving endpoint provisioning
# Status: Phase-3 implementation (post-MVP)
#
# When implementing:
# - Use databricks_model_serving resource
# - Reference MLflow model: denial-risk-lgbm@production
# - Enable A/B testing between V1 and V2
# - Add auto-scaling based on inference latency
#
# ADR-007 documents the decision to defer this to Phase-3
# pending quota increase for Standard_D4s_v3 VMs.
