@echo off
setlocal
cd /d "%~dp0"

if not exist "config.json" (
  if exist "config.example.json" (
    copy /Y "config.example.json" "config.json" >nul
  )
)

if not exist ".venv\Scripts\python.exe" (
  echo Virtual environment not found. Running automatic setup first...
  call "00_SETUP.bat"
  if errorlevel 1 exit /b 1
)

".venv\Scripts\python.exe" -c "import playwright, openpyxl" >nul 2>nul
if errorlevel 1 (
  echo Required packages are missing. Running automatic setup first...
  call "00_SETUP.bat"
  if errorlevel 1 exit /b 1
)

".venv\Scripts\python.exe" "src\rosreestr_parser_app.py"
if errorlevel 1 (
  echo.
  echo ERROR: application exited with an error.
)
pause
