@echo off
REM Quick start script for Mini Airflow project

echo ============================================================
echo Mini Airflow - Place Metric Ingestion Pipeline
echo ============================================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running!
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo [INFO] Docker is running
echo.

echo [INFO] Checking syntax of DAG files...
python check_syntax.py
if errorlevel 1 (
    echo [ERROR] Syntax errors found in DAG files!
    echo Please fix the errors and try again.
    pause
    exit /b 1
)
echo.

echo [INFO] Starting Airflow services...
echo This may take a few minutes on first run...
echo.
docker-compose up -d

if errorlevel 1 (
    echo [ERROR] Failed to start Airflow services!
    pause
    exit /b 1
)

echo.
echo ============================================================
echo ✓ Airflow is starting up!
echo ============================================================
echo.
echo Wait 2-3 minutes for all services to be healthy, then:
echo.
echo 1. Open your browser to: http://localhost:8080
echo 2. Login with:
echo    - Username: airflow
echo    - Password: airflow
echo 3. Find the 'place_metric_ingestion' DAG
echo 4. Toggle it ON and trigger a run
echo.
echo To view logs:
echo   docker-compose logs -f
echo.
echo To stop Airflow:
echo   docker-compose down
echo.
echo ============================================================
pause
