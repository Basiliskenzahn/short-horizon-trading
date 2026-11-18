#!/bin/bash
# Pack one UTC day of compacted Parquet into /home/ubuntu/export/<day>.tar for download.
set -euo pipefail
d=${1:-$(date -u -d yesterday +%F)}
mkdir -p /home/ubuntu/export
cd /home/hl/data/parquet
ls -d */*/"$d" > /dev/null
tar -cf "/home/ubuntu/export/$d.tar.part" */*/"$d"
mv "/home/ubuntu/export/$d.tar.part" "/home/ubuntu/export/$d.tar"
chown ubuntu:ubuntu "/home/ubuntu/export/$d.tar"
find /home/ubuntu/export -name '*.tar' -mtime +10 -delete
