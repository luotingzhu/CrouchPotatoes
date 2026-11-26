﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿# FinBrief 一键启动脚本：拉起数据库 -> 启动后端 -> 打印访问地址并停留
# 用法：双击同目录下的 start.bat

param([switch]$NoPause)

$ErrorActionPreference = 'Stop'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$Root    = Split-Path -Parent $MyInvocation.MyCommand.Path
$Backend = Join-Path $Root 'backend'
$Python  = Join-Path $Backend '.venv\Scripts\python.exe'
$Base    = 'http://127.0.0.1:8000'

function Step($msg) { Write-Host ("→ " + $msg) -ForegroundColor Cyan }
function Ok($msg)   { Write-Host ("✓ " + $msg) -ForegroundColor Green }
function Warn($msg) { Write-Host ("! " + $msg) -ForegroundColor Yellow }

try {
    # ---------- 1. 数据库 ----------
    Step '启动数据库（PostgreSQL + Redis）...'
    wsl -e bash -lc "cd /mnt/t/workshops/ent207proj/code/CrouchPotatoes && docker compose up -d" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw '数据库启动失败：请确认 WSL 中已安装 Docker，且当前用户已加入 docker 组'
    }

    Step '等待数据库就绪...'
    $dbReady = $false
    for ($i = 0; $i -lt 60; $i++) {
        $status = wsl -e bash -lc "docker inspect -f '{{.State.Health.Status}}' domel-postgres domel-redis 2>/dev/null | tr '\n' ' '"
        if ($status -match 'healthy\s+healthy') { $dbReady = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $dbReady) { throw '数据库 60 秒内未就绪，请执行 docker compose ps 查看状态' }
    Ok '数据库已就绪'

    # WSL 空闲时会自动关闭虚拟机，连带把 PostgreSQL / Redis 一起停掉（表现为用一会儿就 500）
    # 这里挂一个常驻进程，让 WSL 虚拟机在演示期间保持运行
    $keepAlive = wsl -e bash -lc "pgrep -f '/usr/bin/sleep infinity' >/dev/null && echo yes || echo no"
    if ($keepAlive -notmatch 'yes') {
        Start-Process -FilePath 'wsl.exe' -ArgumentList @('-d', 'Ubuntu', '-e', '/usr/bin/sleep', 'infinity') -WindowStyle Hidden | Out-Null
        Ok '已保持 WSL 常驻（避免数据库随虚拟机休眠掉线）'
    }

    # ---------- 2. 后端 ----------
    $portBusy = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
    if ($portBusy) {
        Warn '8000 端口已被占用，跳过启动后端（直接复用现有服务）'
    }
    else {
        if (-not (Test-Path $Python)) {
            throw "未找到 Python 虚拟环境：$Python`n请先在 backend 目录创建 .venv 并安装依赖"
        }
        Step '启动后端服务（新窗口）...'
        Start-Process -FilePath $Python `
            -ArgumentList @('-m', 'uvicorn', 'app.main:app', '--host', '0.0.0.0', '--port', '8000') `
            -WorkingDirectory $Backend | Out-Null
    }

    Step '等待后端就绪...'
    $apiReady = $false
    for ($i = 0; $i -lt 75; $i++) {
        try {
            Invoke-WebRequest "$Base/api/health" -UseBasicParsing -TimeoutSec 2 | Out-Null
            $apiReady = $true
            break
        }
        catch { Start-Sleep -Milliseconds 800 }
    }
    if (-not $apiReady) { throw '后端 75 秒内未就绪，请查看后端命令行窗口的日志' }
    Ok '后端已就绪'

    # ---------- 3. 地址 ----------
    $lanIp = Get-NetIPConfiguration |
        Where-Object { $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq 'Up' } |
        ForEach-Object { $_.IPv4Address.IPAddress } |
        Select-Object -First 1

    Write-Host ''
    Write-Host '================================================================' -ForegroundColor DarkCyan
    Write-Host '  FinBrief · 电机市场情报 AI 助手' -ForegroundColor White
    Write-Host '================================================================' -ForegroundColor DarkCyan
    Write-Host ('  介绍页    ' + $Base + '/welcome') -ForegroundColor White
    Write-Host ('  系统入口  ' + $Base + '/') -ForegroundColor White
    if ($lanIp) {
        Write-Host ('  局域网    http://' + $lanIp + ':8000/') -ForegroundColor Gray
    }
    Write-Host ''
    Write-Host '  演示账号  demo / demo1234' -ForegroundColor Gray
    Write-Host ''
    Write-Host '  关闭后端：关掉后端命令行窗口' -ForegroundColor DarkGray
    Write-Host '  关闭数据库：在 WSL 执行 docker compose down' -ForegroundColor DarkGray
    Write-Host '================================================================' -ForegroundColor DarkCyan
    Write-Host ''
}
catch {
    Write-Host ''
    Write-Host ('✗ ' + $_.Exception.Message) -ForegroundColor Red
    Write-Host ''
}

if (-not $NoPause) { Read-Host '按回车键关闭此窗口' }
