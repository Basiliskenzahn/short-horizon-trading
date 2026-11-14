@echo off
setlocal
title Hyperliquid recorder
rem ---------------------------------------------------------------------------
rem  Double-click to start recording. Safe to run again at any time:
rem  - first run: downloads the project, creates .venv, installs dependencies
rem  - later runs: updates the project, then starts recording
rem  The PC is kept awake while this window is open (sleep settings return
rem  to normal as soon as it closes). Stop: close the window, or Ctrl+C then Y.
rem ---------------------------------------------------------------------------

set "REPO=%USERPROFILE%\short-horizon-trading"
set "DATA=%REPO%\data"
set "URL=https://github.com/Basiliskenzahn/short-horizon-trading"

rem --- Python -------------------------------------------------------------
set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY where python >nul 2>nul && set "PY=python"
if not defined PY goto :nopython
%PY% -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" || goto :oldpython

rem --- project files ------------------------------------------------------
if exist "%REPO%\.git" goto :update
if exist "%REPO%\pyproject.toml" goto :install
echo Downloading the project to %REPO% ...
where git >nul 2>nul && (git clone "%URL%.git" "%REPO%" || goto :fail) && goto :install
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $z=Join-Path $env:TEMP 'sht.zip'; $d=Join-Path $env:TEMP 'sht'; Invoke-WebRequest '%URL%/archive/refs/heads/main.zip' -OutFile $z; Expand-Archive $z $d -Force; Move-Item (Join-Path $d 'short-horizon-trading-main') '%REPO%'" || goto :fail
goto :install

:update
echo Updating the project ...
git -C "%REPO%" pull --ff-only || echo (update skipped, continuing with the current version)

:install
cd /d "%REPO%"
if not exist ".venv\Scripts\python.exe" (
  echo Creating the Python environment ...
  %PY% -m venv .venv || goto :fail
)
echo Installing dependencies (first run takes a minute) ...
".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check -e ".[research]" || goto :fail

rem --- record, restart automatically after a crash ------------------------
:loop
echo.
echo [%date% %time%] Recording into %DATA%
echo Keep this window open. Logs print one summary line per minute.
".venv\Scripts\python.exe" -m hl_recorder --data "%DATA%" record --keep-awake
echo [%date% %time%] Recorder stopped (exit code %errorlevel%). Restarting in 10 s, close the window to stop.
timeout /t 10 /nobreak >nul
goto :loop

:nopython
echo Python 3 was not found. Install it from https://www.python.org/downloads/
echo (tick "Add python.exe to PATH" in the installer), then double-click this file again.
pause
exit /b 1

:oldpython
echo This project needs Python 3.10 or newer. Install a current version from https://www.python.org/downloads/
pause
exit /b 1

:fail
echo.
echo Setup failed, see the message above.
pause
exit /b 1
