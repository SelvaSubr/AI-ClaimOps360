#!/bin/bash
# setup_claims_project.sh
# =============================================================================
#  Sets up the complete local development environment from scratch.
#  Usage: bash setup_claims_project.sh
#  Prerequisites: macOS with Homebrew installed.
# =============================================================================

set -euo pipefail
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

log()  { echo -e "${BLUE}[setup]${NC} $1"; }
ok()   { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[⚠]${NC} $1"; }
fail() { echo -e "${RED}[✗]${NC} $1"; exit 1; }

log "AI-ClaimOps360 — Local Environment Setup"
log "========================================"
echo ""

# ── Homebrew ─────────────────────────────────────────────────────────
log "Checking Homebrew..."
command -v brew &>/dev/null || fail "Homebrew not installed. Install from https://brew.sh"
ok "Homebrew $(brew --version | head -1)"

# ── Python 3.11 ──────────────────────────────────────────────────────
log "Installing Python 3.11..."
brew install python@3.11 2>/dev/null || true
PYTHON=$(brew --prefix python@3.11)/bin/python3.11
$PYTHON --version | grep -q "3.11" && ok "Python $($PYTHON --version)" || fail "Python 3.11 not found"

# ── Java 11 (required for PySpark) ───────────────────────────────────
log "Installing Java 11..."
brew install openjdk@11 2>/dev/null || true
JAVA_HOME_BREW=$(brew --prefix openjdk@11)
export JAVA_HOME="$JAVA_HOME_BREW"
export PATH="$JAVA_HOME/bin:$PATH"

if java -version 2>&1 | grep -q "11"; then
  ok "Java 11: $JAVA_HOME"
else
  warn "Java 11 not in PATH after install. Adding to ~/.zshrc..."
  echo "" >> ~/.zshrc
  echo "# Java 11 for PySpark (added by setup_claims_project.sh)" >> ~/.zshrc
  echo 'export JAVA_HOME=$(brew --prefix openjdk@11)' >> ~/.zshrc
  echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
  warn "Run: source ~/.zshrc, then re-run this script if Java checks fail."
fi

# ── Docker Desktop ────────────────────────────────────────────────────
log "Checking Docker..."
if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
  ok "Docker $(docker --version)"
else
  warn "Docker not running. Download Docker Desktop: https://www.docker.com/products/docker-desktop"
  warn "Must be running before: make kafka-up"
fi

# ── Azure CLI ─────────────────────────────────────────────────────────
log "Installing Azure CLI..."
brew install azure-cli 2>/dev/null || true
command -v az &>/dev/null && ok "Azure CLI $(az version --query '"azure-cli"' -o tsv)" || warn "az not found"

# ── Databricks CLI (new unified CLI, not legacy pip version) ──────────
log "Installing Databricks CLI..."
if ! command -v databricks &>/dev/null; then
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh 2>/dev/null || true
fi
command -v databricks &>/dev/null && ok "Databricks CLI $(databricks version 2>/dev/null || echo 'installed')" || warn "databricks CLI not found — install manually from docs.databricks.com/dev-tools/cli"

# ── Node.js 18 ───────────────────────────────────────────────────────
log "Installing Node.js 18..."
brew install node@18 2>/dev/null || true
# Link if not already the active node
brew link node@18 --force --overwrite 2>/dev/null || true
NODE_VER=$(node --version 2>/dev/null || echo "NOT FOUND")
if [[ "$NODE_VER" == v18* ]]; then
  ok "Node $NODE_VER"
else
  warn "Node not at 18.x — currently: $NODE_VER"
  warn "Try: brew link node@18 --force --overwrite, then restart terminal"
fi

# ── Terraform ─────────────────────────────────────────────────────────
log "Installing Terraform..."
brew tap hashicorp/tap 2>/dev/null || true
brew install hashicorp/tap/terraform 2>/dev/null || true
command -v terraform &>/dev/null && ok "Terraform $(terraform version | head -1)" || warn "terraform not found"

# ── Python virtual environment (.venv) ───────────────────────────────
log "Creating Python virtual environment (.venv)..."
if [ -d ".venv" ]; then
  warn ".venv already exists — skipping creation"
else
  $PYTHON -m venv .venv
  ok ".venv created"
fi
source .venv/bin/activate
ok "Virtual environment activated: $VIRTUAL_ENV"

# ── Python dependencies ───────────────────────────────────────────────
log "Installing Python dependencies..."
pip install --upgrade pip -q

if [ -f "requirements.txt" ]; then
  pip install -r requirements.txt -q && ok "requirements.txt installed"
else
  warn "requirements.txt not found — skipping core deps"
fi

if [ -f "requirements-dev.txt" ]; then
  pip install -r requirements-dev.txt -q && ok "requirements-dev.txt installed"
else
  warn "requirements-dev.txt not found — skipping dev deps"
fi

# ── pre-commit hooks ──────────────────────────────────────────────────
log "Installing pre-commit hooks..."
if [ -f ".pre-commit-config.yaml" ]; then
  pre-commit install -q && ok "pre-commit (commit stage) hooks installed"
  pre-commit install --hook-type commit-msg -q && ok "commitizen (commit-msg) hook installed"
else
  warn ".pre-commit-config.yaml not found — rename pre-commit-config.yaml to .pre-commit-config.yaml"
fi

# ── detect-secrets baseline ───────────────────────────────────────────
log "Creating detect-secrets baseline..."
if [ ! -f ".secrets.baseline" ]; then
  detect-secrets scan > .secrets.baseline
  ok ".secrets.baseline created"
else
  ok ".secrets.baseline already exists"
fi

# ── dbt virtual environment (separate from main .venv) ───────────────
log "Setting up dbt virtual environment (~/.venvs/dbt-env)..."
mkdir -p ~/.venvs
if [ ! -d "$HOME/.venvs/dbt-env" ]; then
  $PYTHON -m venv ~/.venvs/dbt-env
  source ~/.venvs/dbt-env/bin/activate
  pip install dbt-core==1.7.14 dbt-databricks==1.7.4 dbt-snowflake==1.7.4 -q
  ok "dbt venv created at ~/.venvs/dbt-env"
  deactivate
else
  ok "dbt venv already exists at ~/.venvs/dbt-env"
fi
source .venv/bin/activate  # back to main venv

# ── .env setup ────────────────────────────────────────────────────────
log "Setting up .env..."
if [ -f ".env" ]; then
  ok ".env exists — not overwriting"
else
  if [ -f ".env.example" ]; then
    cp .env.example .env
    chmod 600 .env
    ok ".env created from .env.example — fill in all variables before running"
  else
    warn ".env.example not found — create .env manually from env.example in outputs/"
  fi
fi

# ── Kafka (start + topics) ────────────────────────────────────────────
log "Starting Kafka containers..."
if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
  if [ -f "docker/docker-compose.yml" ]; then
    make kafka-up 2>/dev/null && sleep 15 && ok "Kafka containers started"
    make kafka-topics 2>/dev/null && ok "Kafka topics created" || warn "kafka-topics failed — run 'make kafka-topics' after Kafka is up"
  else
    warn "docker/docker-compose.yml not found — run 'make kafka-up' after scaffold is committed"
  fi
else
  warn "Docker not running — start Docker Desktop, then run: make kafka-up && make kafka-topics"
fi

# ── Summary ───────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo -e "${GREEN}  Setup complete.${NC}"
echo ""
echo "  NEXT STEPS:"
echo "  1. Run: bash verify_versions.sh"
echo "     Update requirements-dev.txt and .pre-commit-config.yaml"
echo "     if any pinned versions differ from actual installed versions."
echo ""
echo "  2. Fill in .env with all 35 variables (see env.example)"
echo "     Then: chmod 600 .env"
echo ""
echo "  3. Complete Databricks setup (§4 Steps 4-7):"
echo "     Install 9 libraries on cluster, restart, ADLS secret scope,"
echo "     MLflow experiment path"
echo ""
echo "  4. source .venv/bin/activate"
echo "     make verify    — master verification block"
echo "     make train     — V1 model (AUC ~0.76)"
echo "     make pipeline  — full end-to-end"
echo ""
echo "  Kafka UI: http://localhost:8080"
echo "════════════════════════════════════════════════════════════"
