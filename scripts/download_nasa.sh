#!/usr/bin/env sh
# Tai NASA HTTP access log thang 7/1995 (~20MB nen, 1.891.714 dong) vao data/raw/nasa/.
# Nguon: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
set -e
cd "$(dirname "$0")/.."
mkdir -p data/raw/nasa
out=data/raw/nasa/NASA_access_log_Jul95.gz
if [ -s "$out" ]; then echo "Da co $out, bo qua."; exit 0; fi
curl -fL --retry 3 -o "$out" https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
echo "Da tai $out"
