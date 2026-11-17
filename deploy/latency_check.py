"""Request round-trip times to Hyperliquid and Binance from this machine (stdlib only).

Keeps one HTTPS connection open per venue and times small read-only requests, so the
numbers show network + server time, not TLS handshakes. No account, no orders.

    python3 deploy/latency_check.py            # 30 requests per venue
"""
import http.client
import json
import statistics
import sys
import time

N = int(sys.argv[1]) if len(sys.argv) > 1 else 30
TARGETS = {
    "Hyperliquid  POST /info (allMids)": ("api.hyperliquid.xyz", "POST", "/info", json.dumps({"type": "allMids"})),
    "Binance fut. GET /fapi/v1/time   ": ("fapi.binance.com", "GET", "/fapi/v1/time", None),
}


def run(host, method, path, body):
    conn = http.client.HTTPSConnection(host, timeout=10)
    t0 = time.perf_counter()
    conn.connect()
    connect_ms = (time.perf_counter() - t0) * 1000
    rtts = []
    for _ in range(N):
        t0 = time.perf_counter()
        conn.request(method, path, body=body, headers={"Content-Type": "application/json"})
        conn.getresponse().read()
        rtts.append((time.perf_counter() - t0) * 1000)
        time.sleep(0.2)
    conn.close()
    return connect_ms, rtts


for name, (host, method, path, body) in TARGETS.items():
    try:
        connect_ms, rtts = run(host, method, path, body)
        rtts.sort()
        print(f"{name}  connect {connect_ms:6.1f} ms   request RTT median {statistics.median(rtts):6.1f} ms"
              f"   p10 {rtts[len(rtts) // 10]:6.1f}   p90 {rtts[9 * len(rtts) // 10]:6.1f}")
    except Exception as e:  # noqa: BLE001
        print(f"{name}  failed: {e!r}")
