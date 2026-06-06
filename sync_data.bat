@echo off
setlocal
cd /d "%~dp0"

if not exist "logs" mkdir "logs"

set "STAMP=%date:~-4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%"
set "STAMP=%STAMP: =0%"
set "LOG_FILE=%~dp0logs\portfolio-sync-%STAMP%.log"

echo.
echo  ================================================
echo   Portfolio Dashboard Data Sync
echo  ================================================
echo  Started: %date% %time%
echo  Log: %LOG_FILE%
echo.

echo Portfolio Dashboard Data Sync > "%LOG_FILE%"
echo Started: %date% %time% >> "%LOG_FILE%"
echo Working folder: %CD% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

python extract_data.py >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Data sync failed. See log:
    echo  %LOG_FILE%
    echo.
    echo Failed: %date% %time% >> "%LOG_FILE%"
    exit /b 1
)

echo.
echo  Data sync complete. data.js has been regenerated.
echo  Finished: %date% %time%
echo.
echo Finished: %date% %time% >> "%LOG_FILE%"
exit /b 0
