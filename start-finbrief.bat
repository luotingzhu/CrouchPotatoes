@echo off
chcp 65001 >nul
title finbrief - local stack
cd /d "%~dp0"

echo [1/3] Starting PostgreSQL and Redis in WSL...
wsl -d Ubuntu -u root -e service postgresql start
wsl -d Ubuntu -u root -e sh -c "redis-server --daemonize yes"

echo [2/3] Opening http://127.0.0.1:8000/
start "" "http://127.0.0.1:8000/"

echo [3/3] Starting finbrief (Ctrl+C to stop)
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
