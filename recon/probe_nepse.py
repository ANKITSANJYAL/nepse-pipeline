"""Recon probe. Answers one question: can we reach NEPSE data today, and how?

Run this BEFORE writing any pipeline code. It makes ~6 requests and prints
what it learned. It does not store anything.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

import httpx

HOSTS = [
    "https://www.nepalstock.com.np",
    "https://newweb.nepalstock.com.np",
]

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "application/json, text/plain, */*"}


def probe(client: httpx.Client, method: str, url: str, **kw: Any) -> None:
    t0 = time.perf_counter()
    try:
        r = client.request(method, url, **kw)
    except Exception as exc:  # noqa: BLE001 - recon, we want the reason printed
        print(f"  {method:4} {url}\n       ERROR {type(exc).__name__}: {exc}")
        return
    dt = (time.perf_counter() - t0) * 1000
    body = r.text[:180].replace("\n", " ")
    print(f"  {method:4} {url}\n       {r.status_code} {dt:6.0f}ms {len(r.content):>7}B  {body}")
    if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
        try:
            data = r.json()
            keys = list(data)[:20] if isinstance(data, dict) else f"list[{len(data)}]"
            print(f"       keys: {keys}")
        except json.JSONDecodeError:
            pass


def main() -> int:
    # HTTP/2 + generous timeout: NEPSE is slow and has had cert issues.
    with httpx.Client(http2=True, timeout=30.0, headers=HEADERS, verify=True, follow_redirects=True) as c:
        for host in HOSTS:
            print(f"\n=== {host} ===")
            probe(c, "GET", f"{host}/robots.txt")
            # Unauthenticated: hands out accessToken + salt1..salt5 + serverTime.
            probe(c, "GET", f"{host}/api/authenticate/prove")
            # Expect 401 -> confirms the Salter token is genuinely required.
            probe(c, "GET", f"{host}/api/nots/nepse-data/market-open")
            probe(c, "GET", f"{host}/api/nots/securityDailyTradeStat/58")
            probe(c, "GET", f"{host}/api/nots")

    print("\n=== library path ===")
    try:
        from nepse import Nepse  # type: ignore[import-not-found]
    except ImportError:
        print("  nepse not installed. Try:")
        print("    pip install git+https://github.com/basic-bgnr/NepseUnofficialApi.git")
        return 0

    n = Nepse()
    n.setTLSVerification(False)  # their cert chain has been broken on and off
    t0 = time.perf_counter()
    print(f"  market open: {n.getMarketStatus()}  ({(time.perf_counter()-t0)*1000:.0f}ms)")

    t0 = time.perf_counter()
    live = n.getPriceVolume()
    print(f"  getPriceVolume -> {len(live)} rows in {(time.perf_counter()-t0)*1000:.0f}ms")
    if live:
        print(f"  fields: {sorted(live[0])}")
        print(f"  sample: {json.dumps(live[0], indent=2)[:600]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
