@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo === Rosreestr Parser EXE build ===
echo.

if not exist "src\rosreestr_parser_app.py" (
  echo ERROR: src\rosreestr_parser_app.py not found.
  echo Run this BAT from repository root folder.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo ERROR: virtual environment not found.
  echo Run 00_SETUP.bat first.
  pause
  exit /b 1
)

echo Checking PyInstaller...
".venv\Scripts\python.exe" -m pip show pyinstaller >nul 2>nul
if errorlevel 1 (
  echo Installing PyInstaller...
  ".venv\Scripts\python.exe" -m pip install pyinstaller
  if errorlevel 1 goto FAIL
)

echo Removing old build/dist/release_assets...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
if not exist "release_assets" mkdir "release_assets"
if exist "release_assets\rosreestr_parser.zip" del /q "release_assets\rosreestr_parser.zip"
if exist "release_assets\rosreestr_parser.sha256.txt" del /q "release_assets\rosreestr_parser.sha256.txt"

echo.
echo Building EXE with PyInstaller...
".venv\Scripts\python.exe" -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --windowed ^
  --onedir ^
  --name RosreestrParser ^
  --collect-all playwright ^
  --add-data "docs\wiki_oss.md;docs" ^
  --add-data "docs\wiki_snt.md;docs" ^
  --add-data "template_burmistr.xlsx;." ^
  --add-data "template_roskvartal.xlsx;." ^
  --add-data "template_snt.xlsx;." ^
  --add-data "template_full_info.xlsx;." ^
  "src\rosreestr_parser_app.py"
if errorlevel 1 goto FAIL

if not exist "dist\RosreestrParser\RosreestrParser.exe" (
  echo ERROR: final EXE was not created:
  echo dist\RosreestrParser\RosreestrParser.exe
  goto FAIL
)

echo.
echo Copying runtime files...
if exist "template_burmistr.xlsx" copy /Y "template_burmistr.xlsx" "dist\RosreestrParser\template_burmistr.xlsx" >nul
if exist "template_roskvartal.xlsx" copy /Y "template_roskvartal.xlsx" "dist\RosreestrParser\template_roskvartal.xlsx" >nul
if exist "template_snt.xlsx" copy /Y "template_snt.xlsx" "dist\RosreestrParser\template_snt.xlsx" >nul
if exist "template_full_info.xlsx" copy /Y "template_full_info.xlsx" "dist\RosreestrParser\template_full_info.xlsx" >nul

if exist "dist\RosreestrParser\docs" rmdir /s /q "dist\RosreestrParser\docs"
mkdir "dist\RosreestrParser\docs"
if exist "docs\wiki_oss.md" copy /Y "docs\wiki_oss.md" "dist\RosreestrParser\docs\wiki_oss.md" >nul
if exist "docs\wiki_snt.md" copy /Y "docs\wiki_snt.md" "dist\RosreestrParser\docs\wiki_snt.md" >nul

REM Public release uses config.example.json as runtime config.json.
REM Working config.json must not be committed to the public repository.
if exist "config.example.json" (
  copy /Y "config.example.json" "dist\RosreestrParser\config.json" >nul
) else (
  echo ERROR: config.example.json not found.
  echo Create config.example.json before building the public EXE release.
  goto FAIL
)

if not exist "dist\RosreestrParser\output" mkdir "dist\RosreestrParser\output"
if not exist "dist\RosreestrParser\state" mkdir "dist\RosreestrParser\state"
if not exist "dist\RosreestrParser\profiles" mkdir "dist\RosreestrParser\profiles"

echo Creating release_assets\rosreestr_parser.zip ...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Compress-Archive -Path 'dist\RosreestrParser\*' -DestinationPath 'release_assets\rosreestr_parser.zip' -Force"
if errorlevel 1 goto FAIL

echo Calculating SHA256...
for /f "usebackq tokens=*" %%H in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "(Get-FileHash 'release_assets\rosreestr_parser.zip' -Algorithm SHA256).Hash.ToLower()"`) do set ZIP_SHA=%%H
echo !ZIP_SHA! > "release_assets\rosreestr_parser.sha256.txt"

echo.
echo DONE.
echo.
echo Final EXE:
echo   dist\RosreestrParser\RosreestrParser.exe
echo.
echo Release ZIP:
echo   release_assets\rosreestr_parser.zip
echo.
echo SHA256:
echo   !ZIP_SHA!
echo.
echo IMPORTANT:
echo   Do NOT run anything from the build folder.
echo   The build folder is only PyInstaller temporary files.
echo.
echo Upload release_assets\rosreestr_parser.zip to GitHub Release.
echo Put this SHA256 into latest.json.
echo.

REM Remove PyInstaller temp build folder after success so it is not confused with the final program.
if exist "build" rmdir /s /q "build"

pause
exit /b 0

:FAIL
echo.
echo ERROR: EXE build failed.
echo If a build folder exists, it is only diagnostic temporary files.
echo The runnable program must be in dist\RosreestrParser.
echo.
pause
exit /b 1
