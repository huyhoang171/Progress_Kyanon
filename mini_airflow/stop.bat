@echo off
REM Stop Airflow services

echo ============================================================
echo Stopping Mini Airflow Services
echo ============================================================
echo.

docker-compose down

if errorlevel 1 (
    echo [ERROR] Failed to stop services!
    pause
    exit /b 1
)

echo.
echo ============================================================
echo ✓ All services stopped successfully
echo ============================================================
echo.
echo To remove all data (including database), run:
echo   docker-compose down -v
echo.
pause
