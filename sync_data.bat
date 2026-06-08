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

python -u extract_data.py >> "%LOG_FILE%" 2>&1
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
echo  Data sync complete. data.js has been regenerated. >> "%LOG_FILE%"

git --version >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Git was not found. See log:
    echo  %LOG_FILE%
    echo.
    echo Failed: %date% %time% >> "%LOG_FILE%"
    exit /b 1
)

for /f "usebackq delims=" %%B in (`git branch --show-current`) do set "BRANCH=%%B"
if "%BRANCH%"=="" set "BRANCH=main"

echo. >> "%LOG_FILE%"
echo Git branch: %BRANCH% >> "%LOG_FILE%"
echo Staging changes... >> "%LOG_FILE%"
git add -A >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Could not stage changes. See log:
    echo  %LOG_FILE%
    echo.
    echo Failed: %date% %time% >> "%LOG_FILE%"
    exit /b 1
)

git diff --cached --quiet
if not errorlevel 1 (
    echo.
    echo  No git changes to commit or push.
    echo  No git changes to commit or push. >> "%LOG_FILE%"
    echo  Finished: %date% %time%
    echo Finished: %date% %time% >> "%LOG_FILE%"
    exit /b 0
)

set "COMMIT_MESSAGE=Auto sync dashboard data - %date% %time%"
echo Committing changes... >> "%LOG_FILE%"
git commit -m "%COMMIT_MESSAGE%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Could not commit changes. See log:
    echo  %LOG_FILE%
    echo.
    echo Failed: %date% %time% >> "%LOG_FILE%"
    exit /b 1
)

echo Pushing to origin/%BRANCH%... >> "%LOG_FILE%"
git push origin "%BRANCH%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Could not push changes. See log:
    echo  %LOG_FILE%
    echo.
    echo Failed: %date% %time% >> "%LOG_FILE%"
    exit /b 1
)

echo.
echo  Git commit and push complete.
echo Git commit and push complete. >> "%LOG_FILE%"
echo  Finished: %date% %time%
echo.
echo Finished: %date% %time% >> "%LOG_FILE%"
exit /b 0
