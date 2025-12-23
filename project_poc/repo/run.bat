@echo off
REM Windows batch script to run common Docker commands

if "%1"=="" goto help
if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="logs" goto logs
if "%1"=="test" goto test
if "%1"=="lint" goto lint
if "%1"=="pull-images" goto pull_images
goto help

:start
echo Starting Airflow stack...
REM
REM
docker-compose --env-file .env -f docker/docker-compose.yaml up -d --build
timeout /t 20 /nobreak
echo Airflow UI: http://localhost:8080 (airflow/airflow)
goto end

:stop
echo Stopping Airflow stack...
docker-compose -f docker/docker-compose.yaml down
goto end

:restart
call :stop
call :start
goto end

:logs
echo Tailing Airflow logs...
docker-compose -f docker/docker-compose.yaml logs -f
goto end

:test
echo Running tests...
pytest -q || (
    if %ERRORLEVEL% equ 5 (
        echo No tests collected - OK
        exit /b 0
    )
)
goto end

:lint
echo Running linter...
flake8 src tests
goto end

:pull_images
echo Pulling base images...
docker pull apache/airflow:2.10.0-python3.10
goto end

:help
echo Usage: run.bat [command]
echo.
echo Commands:
echo   start       - Start Airflow stack (docker-compose up)
echo   stop        - Stop Airflow stack
echo   restart     - Restart stack
echo   logs        - Follow container logs
echo   test        - Run pytest
echo   lint        - Run flake8
echo   pull-images - Pre-pull Docker images
goto end

:end
