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
echo  [2/2]  Opening dashboard in your browser...
start "" "%~dp0dashboard.html"

echo  Done.  Press any key to close this window.
pause >nul
