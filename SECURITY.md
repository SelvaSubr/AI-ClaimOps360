# Security Policy

## Supported Versions

This is a portfolio/reference implementation. Security fixes are applied
to the `main` branch only.

---

## Dependency Security Status

All fixable vulnerabilities have been remediated as of April 2026.
The following packages were upgraded to resolve known CVEs:

| Package | Previous | Current | CVEs Resolved |
|---|---|---|---|
| mlflow | 2.20.3 | 2.22.4 | 12 (CVE-2025-11201, CVE-2025-15379, and others) |
| cryptography | 46.0.0 | 47.0.0 | 3 (CVE-2026-26007, CVE-2026-34073, CVE-2026-39892) |
| python-dotenv | 1.0.1 | 1.2.2 | 1 (CVE-2026-28684) |

---

## Accepted Risk — Vendor-Constrained

The following CVEs cannot be resolved without breaking hard version
constraints imposed by `snowflake-connector-python` and
`databricks-sql-connector`:

| Package | Version | CVE | Blocked By | Notes |
|---|---|---|---|---|
| pyopenssl | 25.3.0 | CVE-2026-27448 | snowflake-connector-python requires `<26.0.0` | Affects server-side TLS — not applicable in this client-only usage |
| pyopenssl | 25.3.0 | CVE-2026-27459 | snowflake-connector-python requires `<26.0.0` | Same constraint |
| pyarrow | 14.0.2 | PYSEC-2024-161 | databricks-sql-connector requires `<15.0.0` | Affects IPC deserialization of untrusted data — not applicable here |

These are tracked and will be resolved when upstream vendor packages
release compatible versions.

---

## MLflow Residual Advisories

Several MLflow advisories have no stable fix version released yet
(as of April 2026) or require mlflow >=3.x which introduces breaking
API changes incompatible with this project's MLflow 2.x governance layer:

| CVE | Status |
|---|---|
| PYSEC-2025-52 | Fix requires mlflow 3.1.0 — breaking API change |
| CVE-2024-37059 | No fix version published |
| CVE-2026-0545 | No fix version published |
| CVE-2026-33866 | No fix version published |
| CVE-2025-15381 | No fix version published |

All of these affect MLflow's model serving endpoints and tracking server
features. This project uses MLflow exclusively as a client library for
experiment tracking and model registry — the vulnerable server-side
code paths are not exercised.

---

## Reporting a Vulnerability

To report a security issue in this project, please open a
[GitHub Security Advisory](../../security/advisories/new).

Do not open a public issue for security vulnerabilities.
