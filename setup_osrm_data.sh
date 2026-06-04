#!/usr/bin/env bash
# =============================================================================
#  setup_osrm_data.sh — Automated OSRM data preparation for Morocco
# =============================================================================
#  This script downloads the Morocco OSM extract and runs the three OSRM
#  pre-processing steps (extract → partition → customize) so that the
#  osrm-routed service in docker-compose.yml can start immediately.
#
#  Algorithm: MLD (Multi-Level Dijkstra)
#    → docker-compose must use:  --algorithm mld
#
#  Usage:
#    chmod +x setup_osrm_data.sh
#    ./setup_osrm_data.sh
# =============================================================================

set -euo pipefail   # exit on error, unset vars, pipe failures

# ── Configuration ────────────────────────────────────────────────────────
DATA_DIR="./osrm_data"
PBF_URL="https://download.geofabrik.de/africa/morocco-latest.osm.pbf"
PBF_FILE="morocco-latest.osm.pbf"
OSRM_IMAGE="osrm/osrm-backend:latest"
PROFILE="car"

# ── Step 0: Create osrm_data directory ───────────────────────────────────
echo ""
echo "============================================="
echo "  OSRM Data Setup for Morocco"
echo "============================================="
echo ""

if [ -d "$DATA_DIR" ]; then
    echo "[✓] Directory '$DATA_DIR' already exists."
else
    echo "[+] Creating directory '$DATA_DIR' ..."
    mkdir -p "$DATA_DIR"
    echo "[✓] Directory created."
fi

# ── Step 1: Download Morocco OSM data ────────────────────────────────────
echo ""
echo "---------------------------------------------"
echo "  Step 1/4: Download Morocco map data"
echo "---------------------------------------------"

if [ -f "$DATA_DIR/$PBF_FILE" ]; then
    echo "[✓] File '$PBF_FILE' already exists — skipping download."
else
    echo "[↓] Downloading '$PBF_FILE' from Geofabrik ..."
    echo "    URL: $PBF_URL"
    echo "    This may take a few minutes depending on your connection."
    echo ""

    # Use wget if available, otherwise fall back to curl
    if command -v wget &> /dev/null; then
        wget -O "$DATA_DIR/$PBF_FILE" "$PBF_URL"
    elif command -v curl &> /dev/null; then
        curl -L -o "$DATA_DIR/$PBF_FILE" "$PBF_URL"
    else
        echo "[✗] ERROR: Neither wget nor curl found. Please install one of them."
        exit 1
    fi

    echo "[✓] Download complete."
fi

# ── Step 2: osrm-extract ─────────────────────────────────────────────────
#    Reads the .osm.pbf file and extracts a road-network graph using
#    the car.lua driving profile. Produces .osrm and several side-files.
echo ""
echo "---------------------------------------------"
echo "  Step 2/4: osrm-extract (parse road network)"
echo "---------------------------------------------"
echo "[▶] Running osrm-extract with profile: $PROFILE ..."

docker run --rm -t \
    -v "$(pwd)/$DATA_DIR:/data" \
    "$OSRM_IMAGE" \
    osrm-extract -p /opt/car.lua /data/$PBF_FILE

echo "[✓] osrm-extract complete."

# ── Step 3: osrm-partition ────────────────────────────────────────────────
#    Partitions the graph into cells for the Multi-Level Dijkstra (MLD)
#    algorithm. This is required before osrm-customize.
echo ""
echo "---------------------------------------------"
echo "  Step 3/4: osrm-partition (build MLD cells)"
echo "---------------------------------------------"
echo "[▶] Running osrm-partition ..."

docker run --rm -t \
    -v "$(pwd)/$DATA_DIR:/data" \
    "$OSRM_IMAGE" \
    osrm-partition /data/morocco-latest.osrm

echo "[✓] osrm-partition complete."

# ── Step 4: osrm-customize ───────────────────────────────────────────────
#    Computes the edge weights/metrics for each cell partition.
#    After this step, the data is ready for osrm-routed --algorithm mld.
echo ""
echo "---------------------------------------------"
echo "  Step 4/4: osrm-customize (compute weights)"
echo "---------------------------------------------"
echo "[▶] Running osrm-customize ..."

docker run --rm -t \
    -v "$(pwd)/$DATA_DIR:/data" \
    "$OSRM_IMAGE" \
    osrm-customize /data/morocco-latest.osrm

echo "[✓] osrm-customize complete."

# ── Done ──────────────────────────────────────────────────────────────────
echo ""
echo "============================================="
echo "  ✓ All OSRM processing steps complete!"
echo "============================================="
echo ""
echo "  Generated files are in: $DATA_DIR/"
echo ""
echo "  Next step — start the OSRM service:"
echo "    docker compose up -d osrm-backend"
echo ""
echo "  Then test it:"
echo "    curl 'http://localhost:5000/nearest/v1/driving/-7.59,33.57?number=1'"
echo ""
