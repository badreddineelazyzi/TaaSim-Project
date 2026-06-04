@echo off
echo ==============================================
echo   OSRM Data Setup for Morocco (Windows Native)
echo ==============================================
echo.

if not exist "osrm_data" (
    echo [+] Creating osrm_data folder...
    mkdir osrm_data
)

if not exist "osrm_data\morocco-latest.osm.pbf" (
    echo [!] Downloading Morocco map data...
    echo This is a ~300MB download so it may take a few minutes.
    curl.exe -L -o osrm_data\morocco-latest.osm.pbf -# https://download.geofabrik.de/africa/morocco-latest.osm.pbf
) else (
    echo [OK] File morocco-latest.osm.pbf already exists. Skipping download.
)

echo.
echo ----------------------------------------------
echo Step 2: osrm-extract (parse road network)
echo ----------------------------------------------
docker run --rm -t -v "%cd%\osrm_data:/data" osrm/osrm-backend:latest osrm-extract -p /opt/car.lua /data/morocco-latest.osm.pbf

echo.
echo ----------------------------------------------
echo Step 3: osrm-partition (build MLD cells)
echo ----------------------------------------------
docker run --rm -t -v "%cd%\osrm_data:/data" osrm/osrm-backend:latest osrm-partition /data/morocco-latest.osrm

echo.
echo ----------------------------------------------
echo Step 4: osrm-customize (compute edge weights)
echo ----------------------------------------------
docker run --rm -t -v "%cd%\osrm_data:/data" osrm/osrm-backend:latest osrm-customize /data/morocco-latest.osrm

echo.
echo ==============================================
echo   DONE! OSRM Data is ready.
echo ==============================================
echo Now you can run: 
echo   docker compose up -d osrm-backend
echo.
