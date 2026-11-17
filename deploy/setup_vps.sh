#!/usr/bin/env bash
# One-shot setup of a recorder server (Ubuntu 22.04/24.04), e.g. a small Tokyo VPS.
# Run as root:  curl -fsSL https://raw.githubusercontent.com/Basiliskenzahn/short-horizon-trading/main/deploy/setup_vps.sh | sudo bash
#
# Creates user `hl`, installs the recorder in /home/hl/short-horizon-trading, records to /home/hl/data
# (all default coins and HL channels + Binance book5 and trades, plus the real-time bookTicker for the
# mid-tier coins; for the majors it would be ~1 GB/day), compacts
# hourly to Parquet, syncs the clock (Amazon Time Sync if available) and prints a latency check.
set -euo pipefail

REPO=https://github.com/Basiliskenzahn/short-horizon-trading
HOME_DIR=/home/hl

echo "== packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3 python3-venv python3-pip chrony > /dev/null

echo "== clock"
if curl -s -m 2 -o /dev/null http://169.254.169.254/ 2>/dev/null; then
  # AWS / Lightsail: Amazon Time Sync Service is exact to well under a millisecond
  grep -q 169.254.169.123 /etc/chrony/chrony.conf || \
    sed -i '1i server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4' /etc/chrony/chrony.conf
fi
systemctl enable --now chrony > /dev/null 2>&1 || true
systemctl restart chrony || true
timedatectl set-timezone UTC || true

echo "== user and code"
id hl > /dev/null 2>&1 || useradd -m -s /bin/bash hl
sudo -u hl bash -c "
  set -e
  cd $HOME_DIR
  [ -d short-horizon-trading ] || git clone -q $REPO
  cd short-horizon-trading && git pull -q
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -e .
  mkdir -p $HOME_DIR/data
"

echo "== service"
sed 's#--data /home/hl/data record#--data /home/hl/data record --binance bn_book5 bn_trades bn_bbo --bn-bbo-coins ASTER FARTCOIN ZRO INIT INJ CRV#' \
  $HOME_DIR/short-horizon-trading/deploy/hl-recorder.service > /etc/systemd/system/hl-recorder.service
systemctl daemon-reload
systemctl enable --now hl-recorder > /dev/null
crontab -u hl $HOME_DIR/short-horizon-trading/deploy/compact.cron

echo "== waiting 60 s for the first data"
sleep 60
echo "recorder: $(systemctl is-active hl-recorder)"
echo "files written so far: $(find $HOME_DIR/data/raw -type f 2>/dev/null | wc -l)"
echo "disk: $(df -h $HOME_DIR | tail -1)"
echo
echo "== clock sync"
chronyc tracking 2>/dev/null | grep -E "Reference ID|System time|Last offset" || true
echo
echo "== latency check"
sudo -u hl $HOME_DIR/short-horizon-trading/.venv/bin/python $HOME_DIR/short-horizon-trading/deploy/latency_check.py 30
echo
echo "Done. Logs: journalctl -u hl-recorder -f    Data: $HOME_DIR/data"
