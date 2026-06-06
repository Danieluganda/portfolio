@echo off
setlocal

set "TASK_NAME=Portfolio Dashboard Data Sync"

echo.
echo  Removing scheduled task: %TASK_NAME%
echo.

schtasks /Delete /TN "%TASK_NAME%" /F
if errorlevel 1 (
    echo.
    echo  No scheduled task was removed. It may not exist.
    echo.
    pause
    exit /b 1
)

echo.
echo  Scheduled sync removed.
echo.
pause
exit /b 0
