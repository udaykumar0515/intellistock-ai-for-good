@echo off
REM IntelliStock v2.1 - Startup Script
REM Runs with virtual environment Python to ensure all dependencies are available

echo.
echo ========================================
echo   IntelliStock v2.1
echo   AI-Driven Inventory Management
echo ========================================
echo.

REM Activate virtual environment
call venv\Scripts\activate

REM Check if activation was successful
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    echo Please ensure venv is set up correctly
    pause
    exit /b 1
)

echo Virtual environment activated successfully
echo.
echo Starting IntelliStock...
echo.
echo The application will open in your browser at:
echo   http://localhost:8503
echo.
echo Press Ctrl+C to stop the server
echo.

REM Run Streamlit with venv Python
venv\Scripts\python.exe -m streamlit run Home.py

REM Deactivate venv on exit
deactivate
