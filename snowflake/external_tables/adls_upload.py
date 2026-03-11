"""Upload local Delta Parquet files to ADLS Gen2 for Snowflake zero-copy reads.
Reads SILVER_PATH and GOLD_PATH from .env, uploads all .parquet files to the
matching ADLS containers (ADLS_CONTAINER_SILVER, ADLS_CONTAINER_GOLD)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import structlog
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
log = structlog.get_logger()

ACCOUNT_NAME = os.environ["ADLS_ACCOUNT_NAME"]
ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SILVER_PATH = os.environ["SILVER_PATH"]
GOLD_PATH = os.environ["GOLD_PATH"]
CONTAINER_SILVER = os.getenv("ADLS_CONTAINER_SILVER", "silver")
CONTAINER_GOLD = os.getenv("ADLS_CONTAINER_GOLD", "gold")

ACCOUNT_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net"


def _blob_client() -> BlobServiceClient:
    if ACCOUNT_KEY:
        log.info("auth_method", method="account_key")
        return BlobServiceClient(ACCOUNT_URL, credential=ACCOUNT_KEY)
    log.info("auth_method", method="service_principal")
    cred = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    return BlobServiceClient(ACCOUNT_URL, credential=cred)


def _upload_dir(client: BlobServiceClient, local_root: Path, container: str) -> int:
    """Upload all .parquet files under local_root to container, preserving relative paths."""
    if not local_root.exists():
        log.warning("local_path_missing", path=str(local_root))
        return 0

    cc = client.get_container_client(container)
    uploaded = 0
    for f in sorted(local_root.rglob("*.parquet")):
        blob_name = str(f.relative_to(local_root))
        with f.open("rb") as data:
            cc.upload_blob(blob_name, data, overwrite=True)
        log.info("uploaded", container=container, blob=blob_name, size_bytes=f.stat().st_size)
        uploaded += 1
    return uploaded


def _delete_container(client: BlobServiceClient, container: str) -> int:
    cc = client.get_container_client(container)
    # Longest path first: files before parent directory markers
    blobs = sorted([b.name for b in cc.list_blobs()], key=len, reverse=True)
    if not blobs:
        log.info("container_already_empty", container=container)
        return 0
    deleted = 0
    for name in blobs:
        try:
            cc.delete_blob(name)
            log.info("deleted", container=container, blob=name)
            deleted += 1
        except Exception as e:
            log.warning("delete_skipped", container=container, blob=name, error=str(e))
    return deleted


def main() -> None:
    client = _blob_client()
    log.info("adls_upload_start", silver_path=SILVER_PATH, gold_path=GOLD_PATH)

    n_silver = _upload_dir(client, Path(SILVER_PATH), CONTAINER_SILVER)
    log.info("silver_upload_done", files=n_silver)

    n_gold = _upload_dir(client, Path(GOLD_PATH), CONTAINER_GOLD)
    log.info("gold_upload_done", files=n_gold)

    log.info("adls_upload_complete", total_files=n_silver + n_gold)
    if n_silver + n_gold == 0:
        log.error("no_files_uploaded — check SILVER_PATH and GOLD_PATH in .env")
        sys.exit(1)


def clean() -> None:
    client = _blob_client()
    log.info("adls_clean_start")

    n_silver = _delete_container(client, CONTAINER_SILVER)
    log.info("silver_clean_done", deleted=n_silver)

    n_gold = _delete_container(client, CONTAINER_GOLD)
    log.info("gold_clean_done", deleted=n_gold)

    log.info("adls_clean_complete", total_deleted=n_silver + n_gold)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--clean":
        clean()
    else:
        main()
