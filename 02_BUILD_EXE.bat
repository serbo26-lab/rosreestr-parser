@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo ERROR: virtual environment not found.
  echo Run 00_SETUP.bat first.
  pause
  exit /b 1
)

echo Building EXE with PyInstaller...
".venv\Scripts\python.exe" -m PyInstaller --noconfirm --clean --windowed --onedir --name RosreestrParser --collect-all playwright --add-data "template.xlsx;." "src\rosreestr_parser_app.py"
if errorlevel 1 goto FAIL

copy /Y "template.xlsx" "dist\RosreestrParser\template.xlsx" >nul
copy /Y "config.example.json" "dist\RosreestrParser\config.json" >nul

echo.
echo EXE build completed.
echo dist\RosreestrParser\RosreestrParser.exe
echo.
echo You can copy the whole dist\RosreestrParser folder to another place.
pause
exit /b 0

:FAIL
echo.
echo ERROR: EXE build failed. Copy the last error lines and send them.
pause
exit /b 1
