#!/usr/bin/env bash
# Stop all services and optionally wipe local data.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

WIPE_DATA=false
WIPE_VOLUMES=false

usage() {
    echo "Usage: $0 [--data] [--volumes]"
    echo "  --data      Also delete data/lake, data/test, logs"
    echo "  --volumes   Also remove Docker volumes (Postgres, MinIO data)"
    exit 0
}

for arg in "$@"; do
    case $arg in
        --data)    WIPE_DATA=true ;;
        --volumes) WIPE_VOLUMES=true ;;
        --help|-h) usage ;;
        *) error "Unknown flag: $arg"; usage ;;
    esac
done

# ── Stop containers ────────────────────────────────────────────────────────────
info "Stopping Docker services…"
if $WIPE_VOLUMES; then
    warn "Removing containers AND volumes (all Postgres + MinIO data will be lost)"
    docker-compose down -v
else
    docker-compose down
fi
info "Containers stopped"

# ── Optional data wipe ─────────────────────────────────────────────────────────
if $WIPE_DATA; then
    warn "Deleting data/, logs/, and /tmp pipeline artefacts…"
    rm -rf data/lake data/test logs
    rm -f /tmp/warehouse/*.parquet /tmp/quality_report.json /tmp/lineage.json
    info "Local data cleared"
fi

# ── Temp files ─────────────────────────────────────────────────────────────────
info "Removing Python cache files…"
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true

echo ""
info "Cleanup complete."
if ! $WIPE_DATA; then
    echo "  data/ and logs/ were kept. Use --data to remove them."
fi
if ! $WIPE_VOLUMES; then
    echo "  Docker volumes were kept. Use --volumes to remove them."
fi
