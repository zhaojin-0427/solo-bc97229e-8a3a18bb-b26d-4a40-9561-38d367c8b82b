#!/usr/bin/env bash
# 本地开发启动接收服务（无 Docker 环境时使用）。
# 依赖：pip install -r requirements.txt
set -euo pipefail
cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1
export RECEIVER_DB="${RECEIVER_DB:-$PWD/data/receiver.db}"
export RECEIVER_PORT="${RECEIVER_PORT:-9000}"
exec python3 -m app.receiver
