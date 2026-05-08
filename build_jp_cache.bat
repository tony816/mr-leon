@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    set "PYTHON=.venv\Scripts\python.exe"
) else (
    set "PYTHON=python"
)

"%PYTHON%" app.py --build-jp-cache
if errorlevel 1 (
    echo.
    echo JP cache build failed.
    pause
    exit /b 1
)

echo.
echo JP cache build complete.
pause
