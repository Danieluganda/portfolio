@echo off
setlocal
cd /d "%~dp0"

echo.
echo  ================================================
echo   Portfolio Dashboard  ^|  10X Digital Economy
echo  ================================================
echo.
echo  [1/2]  Extracting data from Excel files...
echo.

python extract_data.py
if errorlevel 1 (
    echo.
    echo  ERROR: Data extraction failed.
    echo  Make sure Python ^(with pandas ^& openpyxl^) is installed:
    echo.
    echo      pip install pandas openpyxl
    echo.
    pause
    exit /b 1
)

echo.
echo  [2/2]  Starting dynamic watcher and local web server (http://localhost:8000)...
echo  (Leave this window open to automatically regenerate data when files change)
start http://localhost:8000
python serve_dynamic.py
