#!/bin/sh
# Install the gateway as a service on the home server. Run on the server, from the repo copy:
#     sh gateway/install.sh
# Safe to run again after updating the files. To remove it: sh gateway/install.sh --remove
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
USER_NAME=$(id -un)
STATE="$HOME/.local/state/iptv-gateway"
PY=$(command -v python3)

if [ "${1:-}" = "--remove" ]; then
    sudo systemctl disable --now iptv-gateway.service iptv-lines.timer 2>/dev/null || true
    sudo rm -f /etc/systemd/system/iptv-gateway.service /etc/systemd/system/iptv-lines.service \
        /etc/systemd/system/iptv-lines.timer
    sudo systemctl daemon-reload
    echo "removed; files in $ROOT and $STATE were left in place"
    exit 0
fi

mkdir -p "$STATE"

sudo tee /etc/systemd/system/iptv-gateway.service >/dev/null <<EOF
[Unit]
Description=Home IPTV gateway
After=network-online.target
Wants=network-online.target

[Service]
User=$USER_NAME
ExecStart=$PY $ROOT/gateway/iptv_gateway.py --lines $ROOT/gateway/lines.json --state $STATE --port 8088
Restart=always
RestartSec=3
MemoryMax=1G
Nice=5

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/iptv-lines.service >/dev/null <<EOF
[Unit]
Description=Test public IPTV lines and refresh the gateway's line database
After=network-online.target

[Service]
Type=oneshot
User=$USER_NAME
ExecStart=$PY $ROOT/tools/build_playlist.py --lines $ROOT/gateway/lines.json --out $ROOT/au.m3u --health $STATE/health.json
Nice=10
EOF

sudo tee /etc/systemd/system/iptv-lines.timer >/dev/null <<EOF
[Unit]
Description=Refresh IPTV lines overnight

[Timer]
OnCalendar=*-*-* 03:30:00
RandomizedDelaySec=10m
Persistent=true

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now iptv-gateway.service iptv-lines.timer
sudo systemctl restart iptv-gateway.service
sleep 2
systemctl --no-pager --lines=0 status iptv-gateway.service | sed -n 1,4p
echo "playlist: http://$(hostname -I | awk '{print $1}'):8088/tv.m3u"
