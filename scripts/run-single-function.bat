@echo off
if "%1"=="" (
    echo Usage: run-single-function.bat [function_name]
    echo Example: run-single-function.bat get_power_rankings
    exit /b 1
)

echo Running single function: %1
docker-compose -f docker-compose.weekly.yml run --rm weekly-updates python src/%1.py