@echo off
echo Running weekly updates...
docker-compose -f docker-compose.weekly.yml up --build weekly-updates
echo Weekly updates completed!