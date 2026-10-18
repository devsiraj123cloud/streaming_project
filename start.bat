@echo off
REM ===================================================================
REM  Digital Twin Operations Console — Windows Launcher
REM  Double-click this file to start the full stack.
REM  Requirements: Docker Desktop must be running.
REM ===================================================================

title Digital Twin — Startup

echo.
echo  ============================================================
echo   IOE413 Real-Time Digital Twin Anomaly Detection
echo   Starting via Docker Compose...
echo  ============================================================
echo.

REM -- Check Docker is available --
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo  [ERROR] Docker is not running or not installed.
    echo.
    echo  Please start Docker Desktop and try again.
    echo  Download Docker Desktop: https://www.docker.com/products/docker-desktop/
    echo.
    pause
    exit /b 1
)

echo  [OK] Docker is running.

REM -- Build and start the application and Kafka broker stack --
echo  [INFO] Building and starting the application with Kafka...
docker compose up --build -d

if %errorlevel% neq 0 (
    echo.
    echo  [ERROR] Docker Compose failed to start.
    echo  Run 'docker compose logs' to see what went wrong.
    echo.
    pause
    exit /b 1
)

echo.
echo  [INFO] Waiting for the dashboard to be ready...

REM -- Poll health endpoint (up to 60 seconds) --
set /a ATTEMPTS=0
:WAIT_LOOP
set /a ATTEMPTS+=1
if %ATTEMPTS% gtr 30 (
    echo  [WARN] Dashboard not responding after 60 s. Check: docker compose logs app
    goto OPEN_BROWSER
)
ping 127.0.0.1 -n 3 >nul
docker exec digital-twin-app python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" >nul 2>&1
if errorlevel 1 goto WAIT_LOOP

:OPEN_BROWSER
echo.
echo  ============================================================
echo   Dashboard ready at: http://localhost:8000
echo  ============================================================
echo.
start "" "http://localhost:8000"

echo  [INFO] The console will remain open. Press Ctrl+C or close
echo  [INFO] this window to keep the stack running in background.
echo.
echo  To stop the stack, run stop.bat
echo.
pause
