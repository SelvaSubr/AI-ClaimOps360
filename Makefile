# AI-ClaimOps360 Makefile
# Usage: make <target>
# Requires: Docker Desktop running, .venv activated, .env populated

# ── Load .env variables (for DATABRICKS_CLUSTER_ID etc.) ────────────────────
-include .env

# ── Terraform var mapping (.env → TF_VAR_*) ─────────────────────────────────
export TF_VAR_subscription_id             := $(AZURE_SUBSCRIPTION_ID)
export TF_VAR_tenant_id                   := $(AZURE_TENANT_ID)
export TF_VAR_client_id                   := $(AZURE_CLIENT_ID)
export TF_VAR_client_secret               := $(AZURE_CLIENT_SECRET)
export TF_VAR_service_principal_object_id := $(AZURE_SERVICE_PRINCIPAL_OBJECT_ID)
export TF_VAR_key_vault_name              := $(AZURE_KEY_VAULT_NAME)
export TF_VAR_snowflake_org_name          := $(shell bash -c 'source .env 2>/dev/null && echo $$SNOWFLAKE_ACCOUNT' | cut -d- -f1)
export TF_VAR_snowflake_account_name      := $(shell bash -c 'source .env 2>/dev/null && echo $$SNOWFLAKE_ACCOUNT' | cut -d- -f2-)
export TF_VAR_snowflake_user              := $(shell bash -c 'source .env 2>/dev/null && printf "%s" "$$SNOWFLAKE_USER"')
export TF_VAR_snowflake_password          := $(shell bash -c 'source .env 2>/dev/null && printf "%s" "$$SNOWFLAKE_PASSWORD"')
export TF_VAR_snowflake_role              := $(shell bash -c 'source .env 2>/dev/null && r=$$SNOWFLAKE_TF_ROLE && [ -n "$$r" ] && echo "$$r" || echo "SYSADMIN"')
export TF_VAR_databricks_host             := $(DATABRICKS_HOST)

SRC_DIRS  := src/ingestion src/streaming src/ai_validation src/payer_simulation
TEST_DIRS := src/ingestion/tests src/streaming/tests src/ai_validation/tests \
             src/payer_simulation/tests tests/integration snowflake/tests
DOCKER_FILE := docker/docker-compose.yml

.PHONY: help all-up all-down \
        kafka-up kafka-down kafka-reset kafka-topics \
        cluster-start cluster-stop \
        test test-unit test-integration \
        lint format \
        train train-v1 train-v2 train-all \
        produce consume process-claims remittance-consume pipeline pipeline-v1 archive \
        stream stream-reset verify-routing \
        snowflake-setup snowflake-load adls-upload adls-clean snowflake-clean pipeline-full \
        dbt-run dbt-test \
        tf-init tf-plan tf-apply tf-destroy \
        snowflake-test verify clean

help:
	@echo ''
	@echo 'AI-ClaimOps360 — available targets'
	@echo ''
	@echo '  tf-init         terraform init (infra/)'
	@echo '  tf-plan         terraform plan  — creds injected from .env'
	@echo '  tf-apply        terraform apply — creds injected from .env'
	@echo '  tf-destroy      terraform destroy — creds injected from .env'
	@echo ''
	@echo '  all-up          Start Kafka + train V1 model (ready to run pipeline)'
	@echo '  all-down        Stop all containers and clean local Delta tables'
	@echo ''
	@echo '  kafka-up        Start Kafka + ZooKeeper + Schema Registry + UI'
	@echo '  kafka-down      Stop all Kafka containers'
	@echo '  kafka-reset     Stop + remove volumes (fresh Kafka state, all messages lost)'
	@echo '  kafka-topics    Create all 5 required Kafka topics'
	@echo '  cluster-start   Start Databricks cluster (DATABRICKS_CLUSTER_ID from .env)'
	@echo '  cluster-stop    Terminate Databricks cluster (can restart, not deleted)'
	@echo ''
	@echo '  test            Run all unit + integration tests with coverage (≥70%)'
	@echo '  test-unit       Run unit tests only (no Kafka/Delta required)'
	@echo '  test-integration Run integration tests only (requires Kafka running)'
	@echo '  lint            Check black + isort + flake8 + bandit (no auto-fix)'
	@echo '  format          Auto-format with black + isort'
	@echo ''
	@echo '  train           Train V2 LightGBM model (production default)'
	@echo '  train-v1        Train V1 sklearn RF model (regression/audit baseline)'
	@echo '  train-v2        Train V2 LightGBM model (production) + MLflow autolog'
	@echo '  train-all       Train V1 baseline then V2 production (CI order)'
	@echo '  produce             Produce 5 test claims to claims.raw Kafka topic'
	@echo '  consume             Consume claims.raw and write to Delta Bronze'
	@echo '  process-claims      Payer adjudication: claims.raw → remittance.835 (JSON)'
	@echo '  remittance-consume  remittance.835 → Delta Gold remittance table'
	@echo '  pipeline            Full local pipeline using V2 (production default)'
	@echo '  pipeline-v1         Full local pipeline using V1 (regression/rollback test)'
	@echo '  archive             Drain validated/review/rejected topics to NDJSON files'
	@echo '  stream              Run streaming inference (latest offsets, uses checkpoint)'
	@echo '  stream-reset        Clear checkpoint + reprocess all of claims.raw (earliest)'
	@echo '  verify-routing      Show message counts per output topic'
	@echo '  snowflake-setup     Run setup.sql (infra) + note to run adls_integration.sql'
	@echo '  adls-upload         Sync local Silver + Gold Delta Parquet → ADLS Gen2 containers'
	@echo '  adls-clean          Delete all blobs from ADLS silver + gold containers'
	@echo '  snowflake-load      Refresh ADLS external tables in Snowflake (zero-copy)'
	@echo '  snowflake-clean     Drop all dbt-created tables/views in Snowflake GOLD schema'
	@echo '  pipeline-full       pipeline → adls-upload → snowflake-load (end-to-end)'
	@echo ''
	@echo '  dbt-run         dbt run on both targets (Databricks + Snowflake)'
	@echo '  dbt-test        dbt test on both targets'
	@echo '  snowflake-test  Test Snowflake connection + Cortex availability'
	@echo '  verify          Master verification — all platform CLIs + Python packages'
	@echo '  clean           Remove pycache + all local Delta tables (bronze/silver/gold)'
	@echo ''

# ── COMPOSITE TARGETS ─────────────────────────────────────────────────────────

all-up: kafka-up kafka-topics train cluster-start
	@echo ''
	@echo '=== All systems up ==='
	@echo '  Kafka UI:  http://localhost:8080'
	@echo '  V1 model:  src/ai_validation/models/denial_model_v1.pkl'
	@echo '  Run next:  make pipeline'
	@echo '  Databricks cluster started: $(DATABRICKS_CLUSTER_ID)'
	@echo ''

all-down: kafka-down cluster-stop clean
	@echo ''
	@echo '=== All systems down ==='
	@echo '  Containers stopped. Delta tables cleared.'
	@echo '  Models preserved. Run make all-up to restart.'
	@echo ''

# ── KAFKA ─────────────────────────────────────────────────────────────────────

kafka-up:
	docker compose -f $(DOCKER_FILE) up -d
	@echo 'Waiting for Kafka to be ready...'
	@sleep 20
	@echo 'Kafka UI: http://localhost:8080'

kafka-down:
	docker compose -f $(DOCKER_FILE) down

kafka-reset:
	docker compose -f $(DOCKER_FILE) down -v
	@echo 'Kafka volumes removed. All messages lost. Run make kafka-up for fresh state.'

# Create all 5 topics required by the pipeline
kafka-topics:
	@echo 'Creating Kafka topics...'
	docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --create --if-not-exists \
	    --bootstrap-server kafka:9092 \
	    --replication-factor 1 --partitions 1 \
	    --topic claims.raw
	docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --create --if-not-exists \
	    --bootstrap-server kafka:9092 \
	    --replication-factor 1 --partitions 1 \
	    --topic claims.validated
	docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --create --if-not-exists \
	    --bootstrap-server kafka:9092 \
	    --replication-factor 1 --partitions 1 \
	    --topic claims.rejected
	docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --create --if-not-exists \
	    --bootstrap-server kafka:9092 \
	    --replication-factor 1 --partitions 1 \
	    --topic remittance.835
	docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --create --if-not-exists \
	    --bootstrap-server kafka:9092 \
	    --replication-factor 1 --partitions 1 \
	    --topic claims.review_queue
	@echo 'Topics created: claims.raw, claims.validated, claims.rejected, remittance.835, claims.review_queue'

# ── DATABRICKS ───────────────────────────────────────────────────────────────

cluster-start:
	@if [ -z "$(DATABRICKS_CLUSTER_ID)" ]; then \
	  echo "ERROR: DATABRICKS_CLUSTER_ID not set. Check your .env file."; exit 1; fi
	databricks clusters start $(DATABRICKS_CLUSTER_ID)
	@echo 'Cluster start requested: $(DATABRICKS_CLUSTER_ID)'
	@echo 'Wait ~2 minutes for RUNNING state before running jobs.'

cluster-stop:
	@if [ -z "$(DATABRICKS_CLUSTER_ID)" ]; then \
	  echo "ERROR: DATABRICKS_CLUSTER_ID not set. Check your .env file."; exit 1; fi
	databricks clusters delete $(DATABRICKS_CLUSTER_ID)
	@echo 'Cluster terminated: $(DATABRICKS_CLUSTER_ID)'
	@echo '(Terminated = suspended, not deleted. Restart with make cluster-start)'

# ── TESTS ─────────────────────────────────────────────────────────────────────

test:
	pytest $(TEST_DIRS) \
	  --cov=src/ingestion --cov=src/streaming \
	  --cov=src/ai_validation --cov=src/payer_simulation \
	  --cov-report=term-missing --cov-fail-under=66 -v

test-unit:
	pytest \
	  src/ingestion/tests \
	  src/streaming/tests \
	  src/ai_validation/tests \
	  src/payer_simulation/tests \
	  -m "not integration" \
	  --cov=src/ingestion --cov=src/streaming \
	  --cov=src/ai_validation --cov=src/payer_simulation \
	  --cov-report=term-missing --cov-fail-under=66 -v

test-integration:
	pytest tests/integration/ -v -m integration

# ── CODE QUALITY ──────────────────────────────────────────────────────────────

lint:
	black --check $(SRC_DIRS)
	isort --check-only $(SRC_DIRS)
	flake8 $(SRC_DIRS) --max-line-length=100
	bandit -r $(SRC_DIRS) -ll

format:
	black $(SRC_DIRS)
	isort $(SRC_DIRS)

# ── MODELS ────────────────────────────────────────────────────────────────────

train: train-v2

train-v1:
	python -m src.ai_validation.train_denial_model
	@echo 'V1 model saved: src/ai_validation/models/denial_model_v1.pkl'

train-v2:
	python -m src.ai_validation.train_lgbm_model
	@echo 'V2 model saved: src/ai_validation/models/denial_model_v2.json'

train-all: train-v1 train-v2
	@echo 'Both models trained: V1 (regression baseline) + V2 (production)'

# ── PIPELINE ──────────────────────────────────────────────────────────────────

produce:
	@echo 'Clearing existing messages from output topics...'
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.raw 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.validated 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.review_queue 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.rejected 2>/dev/null
	@sleep 3
	$(MAKE) kafka-topics
	python -m src.ingestion.claim_producer --fhir-file sample_data/fhir/mock_fhir_claim_001.json
	@echo '5 FHIR Bundle claims produced to claims.raw'
	@echo 'Risk profile: CLM-001,004 → claims.validated | CLM-005 → claims.review_queue | CLM-002,003 → claims.rejected'

consume:
	SPARK_LOCAL_IP=127.0.0.1 python -m src.streaming.claim_consumer
	@echo 'Claims consumed to Delta Bronze'

process-claims:
	@echo '--- Payer adjudication: claims.raw → remittance.835 (JSON) ---'
	python -m src.streaming.claim_processor
	@echo '--- Adjudication complete. remittance.835 has JSON adjudication records. ---'

remittance-consume:
	@echo '--- Consuming remittance.835 → Delta Gold remittance ---'
	SPARK_LOCAL_IP=127.0.0.1 python -m src.streaming.remittance_consumer
	@echo '--- Remittance Delta table written: $(GOLD_PATH)/remittance/ ---'

pipeline: train
	@echo '--- Resetting Kafka topics for clean pipeline run ---'
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.raw 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.validated 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.review_queue 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.rejected 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic remittance.835 2>/dev/null
	@sleep 3
	$(MAKE) kafka-topics
	@echo '--- Running full local pipeline with V2 model (production default) ---'
	DENIAL_MODEL_VERSION=v2 python -m src.ingestion.claim_producer --file sample_data/837/mock_claim_pipeline.edi
	SPARK_LOCAL_IP=127.0.0.1 DENIAL_MODEL_VERSION=v2 python -m src.streaming.claim_consumer
	DENIAL_MODEL_VERSION=v2 python -m src.streaming.claim_processor
	SPARK_LOCAL_IP=127.0.0.1 DENIAL_MODEL_VERSION=v2 python -m src.streaming.bronze_to_silver
	SPARK_LOCAL_IP=127.0.0.1 DENIAL_MODEL_VERSION=v2 python -m src.streaming.silver_to_gold
	SPARK_LOCAL_IP=127.0.0.1 python -m src.streaming.remittance_consumer
	@echo '--- Pipeline complete (V2/LightGBM). Silver + Gold remittance tables ready. ---'

pipeline-v1: train-v1
	@echo '--- Resetting Kafka topics for clean pipeline run ---'
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.raw 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.validated 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.review_queue 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic claims.rejected 2>/dev/null
	-docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-topics --delete --bootstrap-server kafka:9092 --topic remittance.835 2>/dev/null
	@sleep 3
	$(MAKE) kafka-topics
	@echo '--- Running full local pipeline with V1 model (regression/rollback test) ---'
	DENIAL_MODEL_VERSION=v1 python -m src.ingestion.claim_producer --file sample_data/837/mock_claim_pipeline.edi
	SPARK_LOCAL_IP=127.0.0.1 DENIAL_MODEL_VERSION=v1 python -m src.streaming.claim_consumer
	DENIAL_MODEL_VERSION=v1 python -m src.streaming.claim_processor
	SPARK_LOCAL_IP=127.0.0.1 DENIAL_MODEL_VERSION=v1 python -m src.streaming.bronze_to_silver
	SPARK_LOCAL_IP=127.0.0.1 DENIAL_MODEL_VERSION=v1 python -m src.streaming.silver_to_gold
	SPARK_LOCAL_IP=127.0.0.1 python -m src.streaming.remittance_consumer
	@echo '--- Pipeline complete (V1/sklearn). Compare Silver scores against V2 baseline. ---'

archive:
	@echo '--- Archiving Kafka output topics to $(KAFKA_ARCHIVE_PATH) ---'
	python -m src.streaming.transaction_archiver
	@echo '--- Archive complete. Files at $(KAFKA_ARCHIVE_PATH)/{topic}/{YYYYMMDD}.ndjson ---'

stream:
	@echo '--- Starting streaming inference (checkpoint: $(CHECKPOINT_DIR)) ---'
	SPARK_LOCAL_IP=127.0.0.1 python -m src.streaming.streaming_inference
	@echo '--- Streaming inference stopped ---'

stream-reset:
	@echo '--- Clearing stale checkpoint at $(CHECKPOINT_DIR) ---'
	rm -rf $(CHECKPOINT_DIR)
	@echo '--- Reprocessing all of claims.raw from earliest offset ---'
	SPARK_LOCAL_IP=127.0.0.1 KAFKA_STREAMING_STARTING_OFFSETS=earliest \
	  python -m src.streaming.streaming_inference
	@echo '--- Stream-reset complete. Check output topic counts with make verify-routing ---'

verify-routing:
	@echo '=== Kafka topic message counts ==='
	@docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 \
	  --topic claims.raw --time -1 2>/dev/null | \
	  awk -F: '{sum+=$$3} END{printf "claims.raw:            %d\n", sum}'
	@docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 \
	  --topic claims.validated --time -1 2>/dev/null | \
	  awk -F: '{sum+=$$3} END{printf "claims.validated:      %d\n", sum}'
	@docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 \
	  --topic claims.review_queue --time -1 2>/dev/null | \
	  awk -F: '{sum+=$$3} END{printf "claims.review_queue:   %d\n", sum}'
	@docker compose -f $(DOCKER_FILE) exec kafka \
	  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 \
	  --topic claims.rejected --time -1 2>/dev/null | \
	  awk -F: '{sum+=$$3} END{printf "claims.rejected:       %d\n", sum}'

snowflake-setup:
	@echo '--- Running setup.sql (infra: DB, schemas, roles, service user) ---'
	snowsql -a $(SNOWFLAKE_ACCOUNT) -u $(SNOWFLAKE_USER) \
	  --variable SNOWFLAKE_USER=$(SNOWFLAKE_USER) \
	  --variable SNOWFLAKE_PASSWORD=$(SNOWFLAKE_PASSWORD) \
	  --variable SNOWFLAKE_ROLE=$(SNOWFLAKE_ROLE) \
	  --variable SNOWFLAKE_DATABASE=$(SNOWFLAKE_DATABASE) \
	  --variable SNOWFLAKE_WAREHOUSE=$(SNOWFLAKE_WAREHOUSE) \
	  -f snowflake/setup.sql
	@echo '--- Run adls_integration.sql as ACCOUNTADMIN to create external tables ---'

snowflake-load:
	@echo '--- Refreshing ADLS external tables in Snowflake (zero-copy) ---'
	python3 -c "\
import os, snowflake.connector; \
from dotenv import load_dotenv; load_dotenv(); \
conn = snowflake.connector.connect( \
    account=os.getenv('SNOWFLAKE_ACCOUNT'), \
    user=os.getenv('SNOWFLAKE_USER'), \
    password=os.getenv('SNOWFLAKE_PASSWORD'), \
    role=os.getenv('SNOWFLAKE_ROLE'), \
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'), \
); \
cur = conn.cursor(); \
cur.execute('ALTER EXTERNAL TABLE CLAIMS_ANALYTICS.SILVER.CLAIMS REFRESH'); \
cur.execute('ALTER EXTERNAL TABLE CLAIMS_ANALYTICS.GOLD.REMITTANCE REFRESH'); \
conn.close(); \
print('SILVER.CLAIMS + GOLD.REMITTANCE refreshed')"
	@echo '--- External tables refreshed ---'

adls-upload:
	@echo '--- Uploading local Delta Parquet → ADLS Gen2 (silver + gold containers) ---'
	python snowflake/external_tables/adls_upload.py
	@echo '--- ADLS upload complete ---'

adls-clean:
	@echo '--- Deleting all blobs from ADLS silver + gold containers ---'
	python snowflake/external_tables/adls_upload.py --clean
	@echo '--- ADLS containers cleared ---'

snowflake-clean:
	@echo '--- Cleaning Snowflake: truncating dbt tables + refreshing external tables ---'
	python3 -c "\
import os, snowflake.connector; \
from dotenv import load_dotenv; load_dotenv(); \
conn = snowflake.connector.connect( \
    account=os.getenv('SNOWFLAKE_ACCOUNT'), \
    user=os.getenv('SNOWFLAKE_USER'), \
    password=os.getenv('SNOWFLAKE_PASSWORD'), \
    role=os.getenv('SNOWFLAKE_ROLE'), \
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'), \
    database='CLAIMS_ANALYTICS', \
); \
cur = conn.cursor(); \
truncate = [ \
    'TRUNCATE TABLE IF EXISTS CLAIMS_ANALYTICS.GOLD.int_claim_adjudication', \
    'TRUNCATE TABLE IF EXISTS CLAIMS_ANALYTICS.GOLD.claims_by_provider', \
    'TRUNCATE TABLE IF EXISTS CLAIMS_ANALYTICS.GOLD.denial_rates', \
    'TRUNCATE TABLE IF EXISTS CLAIMS_ANALYTICS.GOLD.revenue_summary', \
    'TRUNCATE TABLE IF EXISTS CLAIMS_ANALYTICS.GOLD.denial_explanations', \
]; \
refresh = [ \
    'ALTER EXTERNAL TABLE CLAIMS_ANALYTICS.SILVER.CLAIMS REFRESH', \
    'ALTER EXTERNAL TABLE CLAIMS_ANALYTICS.GOLD.REMITTANCE REFRESH', \
]; \
[cur.execute(s) or print(s) for s in truncate]; \
[cur.execute(s) or print(s) for s in refresh]; \
conn.close(); \
print('Snowflake clean complete')"
	@echo '--- Snowflake clean complete ---'

pipeline-full: pipeline adls-upload snowflake-load
	@echo '--- pipeline-full complete: Kafka → Delta → ADLS → Snowflake ---'

# ── DBT ───────────────────────────────────────────────────────────────────────

DBT := $(HOME)/.venvs/dbt-env/bin/dbt

dbt-run:
	cd dbt && $(DBT) run --target databricks && $(DBT) run --target snowflake

dbt-test:
	cd dbt && $(DBT) test --target databricks && $(DBT) test --target snowflake

# ── TERRAFORM ─────────────────────────────────────────────────────────────────
tf-init:
	terraform -chdir=infra init

TF_BLANK := DATABRICKS_TOKEN= DATABRICKS_CLUSTER_ID=

tf-plan:
	$(TF_BLANK) terraform -chdir=infra plan

tf-apply:
	$(TF_BLANK) terraform -chdir=infra apply -auto-approve

tf-apply-databricks:
	$(TF_BLANK) terraform -chdir=infra apply -target=module.databricks -auto-approve

tf-import-databricks-wh:
	$(TF_BLANK) terraform -chdir=infra import \
	  module.databricks.databricks_sql_endpoint.claims_wh \
	  $$(databricks warehouses list --output json | \
	     python3 -c "import sys,json; [print(w['id']) for w in json.load(sys.stdin).get('warehouses',[]) if w.get('name')=='$(DATABRICKS_WAREHOUSE)']")

tf-destroy:
	$(TF_BLANK) terraform -chdir=infra destroy

# ── CONNECTIONS ───────────────────────────────────────────────────────────────

snowflake-test:
	python3 -c "\
import os, snowflake.connector; from dotenv import load_dotenv; load_dotenv(); \
conn = snowflake.connector.connect( \
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'), \
    password=os.getenv('SNOWFLAKE_PASSWORD'), \
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE','CLAIMS_WH')); \
cur = conn.cursor(); cur.execute('SELECT CURRENT_VERSION()'); \
print('Snowflake OK:', cur.fetchone()[0]); \
cur.execute('SELECT SNOWFLAKE.CORTEX.COMPLETE($$mistral-7b$$, $$ping$$)'); \
print('Cortex OK'); conn.close()"

verify:
	@echo '--- Mac tools ---'
	python --version
	java -version
	docker --version
	node --version
	terraform version | head -1
	@echo '--- Platform CLIs ---'
	az account show --query "{subscription:id, state:state}" -o table
	databricks clusters list
	@echo '--- Python packages ---'
	python -c "import pyspark; print('PySpark:', pyspark.__version__)"
	python -c "import shap; print('SHAP:', shap.__version__)"
	python -c "import lightgbm; print('LightGBM:', lightgbm.__version__)"
	python -c "import mlflow; print('MLflow:', mlflow.__version__)"
	python -c "from confluent_kafka import Producer; print('Kafka SDK: OK')"
	python -c "import snowflake.connector; print('Snowflake SDK: OK')"
	python -c "import great_expectations; print('GE:', great_expectations.__version__)"
	python -c "import delta; print('Delta Spark: OK')"

# ── CLEANUP ───────────────────────────────────────────────────────────────────

clean:
	find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name '.pytest_cache' -exec rm -rf {} + 2>/dev/null; true
	find . -name '*.pyc' -delete 2>/dev/null; true
	@[ -n "$(BRONZE_PATH)"       ] && rm -rf "$(BRONZE_PATH)"       || true
	@[ -n "$(SILVER_PATH)"       ] && rm -rf "$(SILVER_PATH)"       || true
	@[ -n "$(GOLD_PATH)"         ] && rm -rf "$(GOLD_PATH)"         || true
	@[ -n "$(BRONZE_PATH_INT)"   ] && rm -rf "$(BRONZE_PATH_INT)"   || true
	@[ -n "$(SILVER_PATH_INT)"   ] && rm -rf "$(SILVER_PATH_INT)"   || true
	@[ -n "$(GOLD_PATH_INT)"     ] && rm -rf "$(GOLD_PATH_INT)"     || true
	@echo 'Cleaned: pycache, Delta tables (bronze/silver/gold)'
