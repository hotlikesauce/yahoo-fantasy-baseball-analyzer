@echo off
setlocal

if "%1"=="" goto usage

if "%1"=="run" goto run
if "%1"=="schedule" goto schedule
if "%1"=="single" goto single
if "%1"=="logs" goto logs
if "%1"=="clean" goto clean
if "%1"=="build" goto build
goto usage

:run
echo Running weekly updates (development mode)...
docker-compose -f docker-compose.weekly.yml up --build weekly-updates
goto end

:schedule
echo Running weekly updates (production mode)...
docker-compose -f docker-compose.weekly.prod.yml up --build -d weekly-updates
echo Process started in background. Use 'docker-weekly-manager logs' to monitor.
goto end

:single
if "%2"=="" (
    echo Please specify function name
    echo Example: docker-weekly-manager single get_power_rankings
    goto end
)
echo Running single function: %2
echo Forcing complete rebuild...
docker-compose -f docker-compose.weekly.yml down
docker image rm yahoo-fantasy-baseball-analyzer-weekly-updates 2>nul
docker-compose -f docker-compose.weekly.yml build --no-cache weekly-updates
docker-compose -f docker-compose.weekly.yml run --rm weekly-updates python src/%2.py
goto end

:logs
echo Showing logs...
docker-compose -f docker-compose.weekly.yml logs -f weekly-updates
goto end

:clean
echo Cleaning up Docker resources...
docker-compose -f docker-compose.weekly.yml down -v
docker-compose -f docker-compose.weekly.prod.yml down -v
docker system prune -f
echo Cleanup complete!
goto end

:build
echo Building weekly updates image...
docker-compose -f docker-compose.weekly.yml build weekly-updates
echo Build complete!
goto end

:usage
echo Usage: docker-weekly-manager [command] [options]
echo.
echo Commands:
echo   run       - Run weekly updates in development mode
echo   schedule  - Run weekly updates in production mode (background)
echo   single    - Run a single function (requires function name)
echo   logs      - Show logs from running container
echo   clean     - Clean up Docker resources
echo   build     - Build the weekly updates image
echo.
echo Examples:
echo   docker-weekly-manager run
echo   docker-weekly-manager single get_power_rankings
echo   docker-weekly-manager schedule
echo   docker-weekly-manager logs

:end
endlocal