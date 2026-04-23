#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== dbt Integration Test ==="

# Disable OCSP checks and telemetry for emulator
export SNOWFLAKE_OCSP_RESPONSE_CACHE_SERVER_ENABLED=false
export SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED=false
export SNOWFLAKE_INSECURE_MODE=true
export SF_OCSP_DO_RETRY=false
export CLIENT_SESSION_KEEP_ALIVE=false

# Check dbt is installed
if ! command -v dbt &> /dev/null; then
    echo "ERROR: dbt is not installed"
    echo "Install with: pip install dbt-snowflake"
    exit 1
fi

echo "dbt version: $(dbt --version | head -1)"

# Run dbt debug to verify connection
echo ""
echo "--- dbt debug ---"
dbt debug --profiles-dir . || {
    echo "WARNING: dbt debug failed (expected for some checks against emulator)"
}

# Run dbt run
echo ""
echo "--- dbt run ---"
dbt run --profiles-dir . --project-dir .

echo ""
echo "=== dbt Integration Test PASSED ==="
