#!/bin/bash
cd "$(dirname "$0")"
echo "================================================"
echo "  競輪サイン予想サーバー"
echo "================================================"

# pip依存をチェック
if ! python3 -c "import requests" 2>/dev/null; then
    echo "[INFO] requests ライブラリをインストールします..."
    python3 -m pip install -q -r requirements.txt
fi

# 起動
echo ""
echo "サーバーを起動します..."
echo "ブラウザで http://localhost:8770/ にアクセスしてください"
echo ""
python3 server.py
