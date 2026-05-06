"""
Root pytest configuration.
Provides shared fixtures and environment setup for all test modules.
"""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

load_dotenv()
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
os.environ.setdefault("DENIAL_MODEL_VERSION", "v1")
os.environ.setdefault("DATABRICKS_MODEL_ENDPOINT_ENABLED", "false")


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session")
def edi_file(project_root) -> Path:
    return project_root / "sample_data" / "837" / "mock_claim_001.edi"


@pytest.fixture(scope="session")
def fhir_file(project_root) -> Path:
    return project_root / "sample_data" / "fhir" / "mock_fhir_claim_001.json"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        'integration: marks tests as integration tests (deselect with -m "not integration")',
    )
