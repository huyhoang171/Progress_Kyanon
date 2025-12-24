@echo off
REM Query the Daily_Metrics table to verify pipeline results

echo ============================================================
echo Querying Daily_Metrics Table
echo ============================================================
echo.

echo [INFO] Connecting to Airflow database...
echo.

docker exec mini_airflow-airflow-worker-1 sqlite3 /opt/airflow/airflow.db "SELECT * FROM Daily_Metrics ORDER BY metric_date DESC LIMIT 10;"

if errorlevel 1 (
    echo.
    echo [ERROR] Failed to query database!
    echo.
    echo Possible reasons:
    echo - Airflow is not running (run start.bat first)
    echo - DAG hasn't run yet
    echo - Worker container name is different
    echo.
    echo Try checking container names with:
    echo   docker ps
    echo.
) else (
    echo.
    echo ============================================================
    echo Query completed successfully
    echo ============================================================
    echo.
)

pause
