#!/usr/bin/env bash
# =============================================================================
# setup_postgres.sh
# DAS 839 – NoSQL Systems | Hive Pipeline
#
# One-time PostgreSQL setup for the NASA ETL pipeline.
# Run this once before the first pipeline run.
#
# Usage:
#   bash setup_postgres.sh
# =============================================================================

set -euo pipefail

PGVER=14
PSQL="/usr/lib/postgresql/${PGVER}/bin/psql"
SCHEMA_SQL="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/sql/schema_pg.sql"

echo ""
echo "========================================="
echo "  NASA ETL — PostgreSQL Setup"
echo "========================================="

# ---------------------------------------------------------------------------
# Step 1 — Check PostgreSQL is running
# ---------------------------------------------------------------------------
echo ""
echo "[1/5] Checking PostgreSQL status..."
if ! pg_isready -q 2>/dev/null; then
    echo "  PostgreSQL is not running. Starting it..."
    sudo service postgresql start
    sleep 2
fi
echo "  PostgreSQL is running ✓"

# ---------------------------------------------------------------------------
# Step 2 — Create nasa_user (superuser, no password)
# ---------------------------------------------------------------------------
echo ""
echo "[2/5] Creating nasa_user role..."
sudo -u postgres $PSQL -c "
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nasa_user') THEN
            CREATE ROLE nasa_user WITH LOGIN SUPERUSER PASSWORD '';
            RAISE NOTICE 'Created nasa_user';
        ELSE
            RAISE NOTICE 'nasa_user already exists';
        END IF;
    END
    \$\$;
"
echo "  nasa_user ready ✓"

# ---------------------------------------------------------------------------
# Step 3 — Create nasa_etl database
# ---------------------------------------------------------------------------
echo ""
echo "[3/5] Creating nasa_etl database..."
sudo -u postgres $PSQL -c "
    SELECT 1 FROM pg_database WHERE datname = 'nasa_etl'
" | grep -q 1 && echo "  nasa_etl already exists, dropping..." && \
    sudo -u postgres $PSQL -c "DROP DATABASE nasa_etl;" || true

sudo -u postgres $PSQL -c "CREATE DATABASE nasa_etl OWNER nasa_user;"
echo "  nasa_etl created ✓"

# ---------------------------------------------------------------------------
# Step 4 — Apply schema
# ---------------------------------------------------------------------------
echo ""
echo "[4/5] Applying schema..."
sudo -u postgres $PSQL -d nasa_etl -f "$SCHEMA_SQL"
echo "  Schema applied ✓"

# ---------------------------------------------------------------------------
# Step 5 — Install psycopg2
# ---------------------------------------------------------------------------
echo ""
echo "[5/5] Installing psycopg2..."
pip3 install psycopg2-binary --break-system-packages 2>/dev/null || \
pip3 install psycopg2-binary 2>/dev/null || \
pip3 install --user psycopg2-binary
python3 -c "import psycopg2; print('  psycopg2 ✓')"

# ---------------------------------------------------------------------------
# Done — verify
# ---------------------------------------------------------------------------
echo ""
echo "========================================="
echo "  Setup complete! Verifying..."
echo "========================================="
sudo -u postgres $PSQL -d nasa_etl -c "\dt"

echo ""
echo "All done. You can now run the pipeline:"
echo ""
echo "  bash /home/rsvr_ind/Music/hive_sreenivasa/hive_pipeline/scripts/reset_and_run.sh Jul95"
echo ""
