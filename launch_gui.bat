@echo off
setlocal

cd /d "%~dp0"

where pythonw >nul 2>nul
if not errorlevel 1 (
    start "" pythonw "%~dp0run.pyw"
    exit /b 0
)

where pyw >nul 2>nul
if not errorlevel 1 (
    start "" pyw "%~dp0run.pyw"
    exit /b 0
)

where py >nul 2>nul
if not errorlevel 1 (
    start "" py "%~dp0run.pyw"
    exit /b 0
)

echo Unable to find pythonw/pyw/py. Please install Python or fix the launcher association.
pause
