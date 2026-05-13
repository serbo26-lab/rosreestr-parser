@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo === Rosreestr Parser portable EXE build ===
echo.

REM ---------------------------------------------------------------------------
REM This script builds the public portable release archive:
REM   release_assets\rosreestr_parser.zip
REM
REM Do not upload build/ or dist/ to GitHub.
REM Upload only release_assets\rosreestr_parser.zip to GitHub Releases.
REM ---------------------------------------------------------------------------

if not exist "src\rosreestr_parser_app.py" (
  echo ERROR: src\rosreestr_parser_app.py not found.
  echo Run this BAT from the repository root folder.
  goto FAIL
)

if not exist ".venv\Scripts\python.exe" (
  echo ERROR: virtual environment not found.
  echo Run 00_SETUP.bat first.
  goto FAIL
)

if not exist "config.example.json" (
  echo ERROR: config.example.json not found.
  echo Public release needs config.example.json. It will be copied as config.json into the portable EXE folder.
  goto FAIL
)

if not exist "template_burmistr.xlsx" (
  echo ERROR: template_burmistr.xlsx not found.
  goto FAIL
)

if not exist "template_roskvartal.xlsx" (
  echo ERROR: template_roskvartal.xlsx not found.
  goto FAIL
)

if not exist "template_snt.xlsx" (
  echo ERROR: template_snt.xlsx not found.
  goto FAIL
)

if not exist "docs\wiki_oss.md" (
  echo ERROR: docs\wiki_oss.md not found.
  goto FAIL
)

if not exist "docs\wiki_snt.md" (
  echo ERROR: docs\wiki_snt.md not found.
  goto FAIL
)

echo Checking PyInstaller...
".venv\Scripts\python.exe" -m pip show pyinstaller >nul 2>nul
if errorlevel 1 (
  echo Installing PyInstaller...
  ".venv\Scripts\python.exe" -m pip install pyinstaller
  if errorlevel 1 goto FAIL
)

echo Removing old build, dist and release archive...
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
  "src\rosreestr_parser_app.py"
if errorlevel 1 goto FAIL

if not exist "dist\RosreestrParser\RosreestrParser.exe" (
  echo ERROR: final EXE was not created:
  echo dist\RosreestrParser\RosreestrParser.exe
  goto FAIL
)

echo.
echo Copying portable runtime files...

REM Public runtime config. Keep real working config.json out of the public repository.
copy /Y "config.example.json" "dist\RosreestrParser\config.json" >nul

REM Excel templates used by the program. Old template.xlsx is intentionally not copied.
copy /Y "template_burmistr.xlsx" "dist\RosreestrParser\template_burmistr.xlsx" >nul
copy /Y "template_roskvartal.xlsx" "dist\RosreestrParser\template_roskvartal.xlsx" >nul
copy /Y "template_snt.xlsx" "dist\RosreestrParser\template_snt.xlsx" >nul

REM User documentation inside the program. Old docs\wiki.md is intentionally not copied.
if exist "dist\RosreestrParser\docs" rmdir /s /q "dist\RosreestrParser\docs"
mkdir "dist\RosreestrParser\docs"
copy /Y "docs\wiki_oss.md" "dist\RosreestrParser\docs\wiki_oss.md" >nul
copy /Y "docs\wiki_snt.md" "dist\RosreestrParser\docs\wiki_snt.md" >nul

REM README is useful in the release ZIP, but README_FIRST.txt and SOURCE_RUN.md are intentionally not used.
if exist "README.md" copy /Y "README.md" "dist\RosreestrParser\README.md" >nul

REM Runtime folders. They may stay empty in ZIP; the app can also recreate them later.
if not exist "dist\RosreestrParser\output" mkdir "dist\RosreestrParser\output"
if not exist "dist\RosreestrParser\state" mkdir "dist\RosreestrParser\state"
if not exist "dist\RosreestrParser\profiles" mkdir "dist\RosreestrParser\profiles"
if not exist "dist\RosreestrParser\updates" mkdir "dist\RosreestrParser\updates"

REM Safety cleanup in case old local files appeared in dist from previous manual actions.
if exist "dist\RosreestrParser\template.xlsx" del /q "dist\RosreestrParser\template.xlsx"
if exist "dist\RosreestrParser\docs\wiki.md" del /q "dist\RosreestrParser\docs\wiki.md"
if exist "dist\RosreestrParser\README_FIRST.txt" del /q "dist\RosreestrParser\README_FIRST.txt"
if exist "dist\RosreestrParser\SOURCE_RUN.md" del /q "dist\RosreestrParser\SOURCE_RUN.md"

echo Creating release_assets\rosreestr_parser.zip ...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Compress-Archive -Path 'dist\RosreestrParser\*' -DestinationPath 'release_assets\rosreestr_parser.zip' -Force"
if errorlevel 1 goto FAIL

if not exist "release_assets\rosreestr_parser.zip" (
  echo ERROR: release ZIP was not created.
  goto FAIL
)

echo Calculating SHA256...
for /f "usebackq tokens=*" %%H in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "(Get-FileHash 'release_assets\rosreestr_parser.zip' -Algorithm SHA256).Hash.ToLower()"`) do set ZIP_SHA=%%H
if not defined ZIP_SHA (
  echo ERROR: SHA256 calculation failed.
  goto FAIL
)
echo !ZIP_SHA!>"release_assets\rosreestr_parser.sha256.txt"

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
echo Upload release_assets\rosreestr_parser.zip to GitHub Release.
echo Put this SHA256 into latest.json before committing latest.json.
echo.
echo Do NOT upload build/, dist/ or release_assets/ to the repository.
echo.

REM Remove PyInstaller temporary build folder after success.
if exist "build" rmdir /s /q "build"

pause
exit /b 0

:FAIL
echo.
echo ERROR: EXE build failed.
echo The runnable program must be in dist\RosreestrParser after a successful build.
echo build\ is only PyInstaller temporary diagnostics.
echo.
pause
exit /b 1
