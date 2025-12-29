@echo off
title Google Cloud Function Local Tester
color 0B

echo ======================================================
echo   PROGRAM TO TEST CLOUD FUNCTION (LOCAL)
echo ======================================================

:: 1. Check and install Python libraries
echo [*] Checking Python libraries...
pip install pandas >nul 2>&1
if %errorlevel% neq 0 (
    echo [!] Installing Pandas...
    pip install pandas
)

:: 2. Create temporary run_test.py file to activate logic
echo [*] Preparing test environment...
echo from main_test import transform_gcs_file > run_test.py
echo mock_event = {"bucket": "local-test-bucket", "name": "source_data.json"} >> run_test.py
echo if __name__ == "__main__": transform_gcs_file(mock_event) >> run_test.py

:: 3. Run code
echo [*] Starting data processing...
echo ------------------------------------------------------
python run_test.py
echo ------------------------------------------------------

:: 4. Clean up temporary file
del run_test.py

echo.
echo [*] Completed! Please check the /staging/ directory
echo ======================================================
pause