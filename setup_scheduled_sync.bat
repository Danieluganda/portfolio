@echo off
setlocal
cd /d "%~dp0"

set "TASK_NAME=Portfolio Dashboard Data Sync"
set "SYNC_SCRIPT=%~dp0sync_data.bat"
set "TASK_SCRIPT=%TEMP%\portfolio-dashboard-sync-task.ps1"

echo.
echo  ================================================
echo   Schedule Portfolio Dashboard Auto Sync
echo  ================================================
echo.
echo  This creates or updates a Windows Task Scheduler job.
echo  The job runs sync_data.bat, which regenerates data.js.
echo.

if not exist "%SYNC_SCRIPT%" (
    echo  ERROR: sync_data.bat was not found in this folder.
    pause
    exit /b 1
)

set "SCHEDULE_TYPE="
set /p "SCHEDULE_TYPE=Run daily or weekly? Type D for daily, W for weekly [D]: "
if /i "%SCHEDULE_TYPE%"=="" set "SCHEDULE_TYPE=D"

set "RUN_TIME="
set /p "RUN_TIME=What time should it run? Use 24-hour HH:MM, example 07:30 [07:30]: "
if "%RUN_TIME%"=="" set "RUN_TIME=07:30"

if /i "%SCHEDULE_TYPE%"=="W" goto weekly
goto daily

:daily
(
echo $Action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument '/c ""%SYNC_SCRIPT%""' -WorkingDirectory '%~dp0'
echo $Trigger = New-ScheduledTaskTrigger -Daily -At '%RUN_TIME%'
echo $Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
echo Register-ScheduledTask -TaskName '%TASK_NAME%' -Action $Action -Trigger $Trigger -Settings $Settings -Force ^| Out-Null
) > "%TASK_SCRIPT%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_SCRIPT%"
goto done

:weekly
echo.
echo  Choose day: MON, TUE, WED, THU, FRI, SAT, SUN
set "RUN_DAY="
set /p "RUN_DAY=Which day of the week? [MON]: "
if "%RUN_DAY%"=="" set "RUN_DAY=MON"
(
echo $dayMap = @{ MON = 'Monday'; TUE = 'Tuesday'; WED = 'Wednesday'; THU = 'Thursday'; FRI = 'Friday'; SAT = 'Saturday'; SUN = 'Sunday' }
echo $Day = $dayMap['%RUN_DAY%'.ToUpper()]
echo if ^(-not $Day^) { throw 'Invalid day. Use MON, TUE, WED, THU, FRI, SAT, or SUN.' }
echo $Action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument '/c ""%SYNC_SCRIPT%""' -WorkingDirectory '%~dp0'
echo $Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $Day -At '%RUN_TIME%'
echo $Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
echo Register-ScheduledTask -TaskName '%TASK_NAME%' -Action $Action -Trigger $Trigger -Settings $Settings -Force ^| Out-Null
) > "%TASK_SCRIPT%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_SCRIPT%"
goto done

:done
if errorlevel 1 (
    echo.
    echo  ERROR: Could not create the scheduled task.
    echo  Try running this file as Administrator if Windows blocks it.
    echo.
    pause
    exit /b 1
)

echo.
echo  Schedule saved successfully.
echo.
echo  Task name: %TASK_NAME%
echo  Script: %SYNC_SCRIPT%
echo  Time: %RUN_TIME%
if /i "%SCHEDULE_TYPE%"=="W" echo  Day: %RUN_DAY%
if /i not "%SCHEDULE_TYPE%"=="W" echo  Frequency: Daily
echo.
echo  The dashboard will update data.js on that schedule.
echo  Reload the browser after a scheduled run to see the latest data.
echo.
pause
exit /b 0
