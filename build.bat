@echo off
setlocal EnableExtensions

cd /d "%~dp0"

echo === Resilient Proxy Build ===
echo.

call :find_python
if errorlevel 1 goto :python_missing

echo [1/4] Using Python: %PYTHON_EXE% %PYTHON_ARGS%
echo [2/4] Installing runtime dependencies...
"%PYTHON_EXE%" %PYTHON_ARGS% -m pip install --disable-pip-version-check -r requirements.txt
if errorlevel 1 goto :build_failed

echo [3/4] Installing PyInstaller...
"%PYTHON_EXE%" %PYTHON_ARGS% -m pip install --disable-pip-version-check pyinstaller
if errorlevel 1 goto :build_failed

echo [4/4] Building dist\Plunger.exe ...
"%PYTHON_EXE%" %PYTHON_ARGS% -m PyInstaller --noconfirm --clean "resilient-proxy.spec"
if errorlevel 1 goto :build_failed

if exist "dist\Plunger.exe" (
    echo.
    echo Build completed successfully:
    echo %CD%\dist\Plunger.exe
    echo.
    pause
    exit /b 0
)

echo.
echo Build finished, but dist\Plunger.exe was not found.
goto :build_failed

:find_python
set "PYTHON_EXE="
set "PYTHON_ARGS="

where py >nul 2>nul
if not errorlevel 1 (
    py -3 -c "import sys" >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_EXE=py"
        set "PYTHON_ARGS=-3"
        exit /b 0
    )

    set "PYTHON_EXE=py"
    exit /b 0
)

where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)

where python3 >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)

exit /b 1

:python_missing
echo [ERROR] Python was not found. Install Python or the Python Launcher first.
echo.
pause
exit /b 1

:build_failed
echo.
echo [ERROR] Build failed.
echo.
pause
exit /b 1
