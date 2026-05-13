@echo off
setlocal
cd /d "%~dp0"

echo Rosreestr Parser setup
echo Current folder: %CD%
echo.

set "PY="
where py >nul 2>nul
if not errorlevel 1 set "PY=py -3"

if "%PY%"=="" (
  where python >nul 2>nul
  if not errorlevel 1 set "PY=python"
)

if "%PY%"=="" (
  echo ERROR: Python was not found.
  echo Install Python and enable "Add python.exe to PATH".
  pause
  exit /b 1
)

echo Using Python command: %PY%
%PY% --version
if errorlevel 1 (
  echo ERROR: Python command failed.
  pause
  exit /b 1
)

echo.
echo Creating virtual environment...
%PY% -m venv .venv
if errorlevel 1 goto FAIL

if not exist ".venv\Scripts\python.exe" (
  echo ERROR: .venv was not created.
  pause
  exit /b 1
)

echo.
echo Installing Python packages...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto FAIL
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto FAIL

echo.
echo Installing Playwright Chromium browser...
".venv\Scripts\python.exe" -m playwright install chromium
if errorlevel 1 goto FAIL

echo.
echo Setup completed successfully.
echo Run 01_RUN.bat
pause
exit /b 0

:FAIL
echo.
echo ERROR: setup failed. Copy the last error lines and send them.
pause
exit /b 1
