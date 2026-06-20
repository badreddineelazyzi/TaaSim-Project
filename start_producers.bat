@echo off
set PYTHONPATH=C:\Users\dell\Desktop\big data project\TaaSim-Project\.venv\Lib\site-packages;C:\Users\dell\Desktop\big data project\TaaSim-Project\.venv-1\Lib\site-packages
set VENV_PYTHON="C:\Users\dell\Desktop\big data project\TaaSim-Project\.venv\Scripts\python.exe"
set SCRIPTS="C:\Users\dell\Desktop\big data project\TaaSim-Project\scripts"
set LOGS="C:\Users\dell\Desktop\big data project\TaaSim-Project"

start "GPS Producer" /B %VENV_PYTHON% %SCRIPTS%\vehicle_gps_producer.py --speed 10 --kafka localhost:9092 --max-trips 50 --seed 42 > %LOGS%\gps_producer.log 2>&1
start "Trip Producer" /B %VENV_PYTHON% %SCRIPTS%\trip_request_producer.py --rate 2.0 --duration 300 --kafka localhost:9092 --seed 123 > %LOGS%\trip_producer.log 2>&1

echo Producers started
