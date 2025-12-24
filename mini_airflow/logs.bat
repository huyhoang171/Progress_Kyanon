@echo off
REM View Airflow logs

echo ============================================================
echo Airflow Logs
echo ============================================================
echo.
echo Press Ctrl+C to exit log viewer
echo.

docker-compose logs -f
