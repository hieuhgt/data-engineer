#!/usr/bin/env bash
# One-time setup: create dirs, copy env file, initialize services.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Colors ─────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Prereq check ───────────────────────────────────────────────────────────────
for cmd in docker docker-compose python3 poetry; do
    if ! command -v "$cmd" &>/dev/null; then
        error "$cmd is not installed. See docs/SETUP.md"
        [[ "$cmd" == "poetry" ]] && echo "  Install: curl -sSL https://install.python-poetry.org | python3 -"
        exit 1
    fi
done
info "Prerequisites OK (Poetry $(poetry --version | awk '{print $3}'))"

# ── Env file ───────────────────────────────────────────────────────────────────
if [[ ! -f .env ]]; then
    cp config/env.example .env
    warn ".env created from template — edit it before running the pipeline"
else
    info ".env already exists, skipping"
fi

# ── Local directories ──────────────────────────────────────────────────────────
for dir in data/lake data/test data/checkpoints logs; do
    mkdir -p "$dir"
    info "Created $dir/"
done

# ── Python dependencies (Poetry) ──────────────────────────────────────────────
info "Installing Python dependencies via Poetry…"
# Core + dev. Add --with airflow,warehouse,quality for optional groups.
poetry install --with dev --sync -q
info "Virtual env: $(poetry env info --path)"

# ── Docker stack ───────────────────────────────────────────────────────────────
info "Pulling Docker images (this may take a few minutes)…"
docker-compose pull --quiet

info "Starting infrastructure services…"
docker-compose up -d postgres redis minio zookeeper kafka

info "Waiting for Postgres to be ready…"
for i in $(seq 1 30); do
    if docker-compose exec -T postgres pg_isready -U airflow &>/dev/null; then
        break
    fi
    sleep 2
done

# ── Airflow init ───────────────────────────────────────────────────────────────
info "Initialising Airflow database…"
docker-compose run --rm airflow-webserver airflow db init
docker-compose run --rm airflow-webserver airflow users create \
    --username admin --firstname Admin --lastname User \
    --role Admin --email admin@example.com --password admin

info "Starting remaining services…"
docker-compose up -d

# ── MinIO buckets ──────────────────────────────────────────────────────────────
info "Creating MinIO buckets…"
sleep 5   # let MinIO start
docker-compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker-compose exec -T minio mc mb local/data-lake --ignore-existing 2>/dev/null || \
    warn "Could not create MinIO bucket — do it manually at http://localhost:9001"

# ── Done ───────────────────────────────────────────────────────────────────────
echo ""
info "Setup complete!"
echo ""
echo "  Airflow UI  →  http://localhost:8080  (admin / admin)"
echo "  Spark UI    →  http://localhost:8088"
echo "  MinIO       →  http://localhost:9001  (minioadmin / minioadmin)"
echo "  Grafana     →  http://localhost:3000  (admin / admin)"
echo "  Prometheus  →  http://localhost:9090"
echo ""
echo "  Next: poetry run python scripts/generate_test_data.py --size small"
