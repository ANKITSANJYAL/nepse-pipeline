"""Does NEPSE serve a datacenter IP at all?

Everything measured so far came from a residential connection in the US. NEPSE's
documented failure mode is a *silent hang* - no 403, no RST, just a TLS session
that completes and then never answers - so a cloud-IP block would look identical
to a network wobble unless we measure it deliberately.

This probe is the go/no-go for hosting the capture job anywhere (GitHub Actions,
HF Jobs, a VPS). It distinguishes four outcomes that all look like "it broke":

    HUNG      connected, then no bytes before the deadline  -> likely IP-blocked
    REFUSED   connection refused / DNS failure              -> network or firewall
    HTTP_4XX  a real HTTP rejection                         -> reachable, unwelcome
    OK        bytes came back                               -> hosting is viable

Run locally for a baseline before trusting the hosted result:
    PYTHONPATH=src python recon/probe_hosted.py
"""

from __future__ import annotations

import json
import os
import socket
import ssl
import sys
import time
from datetime import datetime, timedelta
from typing import Any

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nepse_pipeline.config import BROWSER_UA, CONFIG, NPT  # noqa: E402

HOST = CONFIG.base_url.replace("https://", "").replace("http://", "").strip("/")
DEADLINE_S = 25.0          # generous: NEPSE routinely takes 2s, never 25s
NO_UA_DEADLINE_S = 15.0    # the control case is expected to time out

results: list[dict[str, Any]] = []


def record(step: str, outcome: str, detail: str, ms: float | None = None) -> None:
    row = {"step": step, "outcome": outcome, "detail": detail, "ms": round(ms) if ms else None}
    results.append(row)
    bar = {"OK": "  ok  ", "HUNG": " HUNG ", "REFUSED": " REFU ", "HTTP_4XX": " 4XX  ",
           "ERROR": " ERR  ", "INFO": " info "}.get(outcome, "  ??  ")
    ms_txt = f"{row['ms']:>6}ms" if row["ms"] else "        "
    print(f"[{bar}] {ms_txt}  {step:<26} {detail}", flush=True)


def classify(exc: Exception) -> str:
    """A hang and a refusal mean opposite things; never collapse them."""
    if isinstance(exc, (httpx.ReadTimeout, httpx.PoolTimeout, socket.timeout)):
        return "HUNG"
    if isinstance(exc, httpx.ConnectTimeout):
        return "HUNG"
    if isinstance(exc, (httpx.ConnectError, socket.gaierror)):
        return "REFUSED"
    return "ERROR"


# --- 0. what does NEPSE actually see us as? -------------------------------
def step_egress() -> None:
    try:
        with httpx.Client(timeout=10) as c:
            ip = c.get("https://api.ipify.org").text.strip()
        record("egress.ip", "INFO", ip)
        try:
            with httpx.Client(timeout=10) as c:
                geo = c.get(f"http://ip-api.com/json/{ip}?fields=country,isp,org,as").json()
            record("egress.network", "INFO",
                   f"{geo.get('country')} · {geo.get('isp')} · {geo.get('as')}")
        except Exception as exc:  # noqa: BLE001 - geo is nice-to-have, not the test
            record("egress.network", "INFO", f"lookup failed: {type(exc).__name__}")
    except Exception as exc:  # noqa: BLE001
        record("egress.ip", "ERROR", f"{type(exc).__name__}: {exc}")


# --- 1. can we even open TLS to the host? ---------------------------------
def step_tls() -> None:
    t0 = time.perf_counter()
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # NEPSE's chain is intermittently broken
        with socket.create_connection((HOST, 443), timeout=DEADLINE_S) as sock:
            with ctx.wrap_socket(sock, server_hostname=HOST) as tls:
                record("tls.handshake", "OK", f"{tls.version()} to {HOST}",
                       (time.perf_counter() - t0) * 1000)
    except Exception as exc:  # noqa: BLE001
        record("tls.handshake", classify(exc), f"{type(exc).__name__}: {exc}",
               (time.perf_counter() - t0) * 1000)


# --- 2. the control case: no browser UA should hang -----------------------
def step_no_ua() -> None:
    """Confirms the diagnosis still holds from this IP.

    A HUNG here plus an OK below means the block is UA-based, not IP-based -
    which is the good outcome. Both hanging means the IP itself is unwelcome.
    """
    t0 = time.perf_counter()
    try:
        with httpx.Client(timeout=NO_UA_DEADLINE_S, verify=False) as c:
            r = c.get(f"{CONFIG.base_url}/api/nots/nepse-data/market-open",
                      headers={"User-Agent": "python-httpx/probe"})
        record("no-ua.control", "OK" if r.status_code == 200 else "HTTP_4XX",
               f"status={r.status_code} bytes={len(r.content)} (expected: a hang)",
               (time.perf_counter() - t0) * 1000)
    except Exception as exc:  # noqa: BLE001
        record("no-ua.control", classify(exc),
               f"{type(exc).__name__} (this is the EXPECTED result)",
               (time.perf_counter() - t0) * 1000)


# --- 3. with a browser UA, unauthenticated -------------------------------
def step_browser_ua() -> None:
    t0 = time.perf_counter()
    try:
        with httpx.Client(timeout=DEADLINE_S, verify=False, follow_redirects=True) as c:
            r = c.get(f"{CONFIG.base_url}/api/nots/nepse-data/market-open",
                      headers={"User-Agent": BROWSER_UA, "Referer": f"{CONFIG.base_url}/"})
        outcome = "OK" if r.status_code == 200 else "HTTP_4XX"
        record("browser-ua.market-open", outcome,
               f"status={r.status_code} bytes={len(r.content)} body={r.text[:90]!r}",
               (time.perf_counter() - t0) * 1000)
    except Exception as exc:  # noqa: BLE001
        record("browser-ua.market-open", classify(exc), f"{type(exc).__name__}: {exc}",
               (time.perf_counter() - t0) * 1000)


# --- 4/5. the real thing: WASM token, then authenticated reads -------------
def step_authenticated() -> None:
    from nepse_pipeline.session import NepseSession

    t0 = time.perf_counter()
    try:
        session = NepseSession()
    except Exception as exc:  # noqa: BLE001
        record("auth.wasm-token", "ERROR", f"{type(exc).__name__}: {exc}",
               (time.perf_counter() - t0) * 1000)
        return

    with session as s:
        t0 = time.perf_counter()
        try:
            st = s.market_status()
            record("auth.market-status", "OK", f"asOf={st.get('asOf')} state={st.get('isOpen')}",
                   (time.perf_counter() - t0) * 1000)
        except Exception as exc:  # noqa: BLE001
            record("auth.market-status", classify(exc), f"{type(exc).__name__}: {exc}",
                   (time.perf_counter() - t0) * 1000)
            return

        t0 = time.perf_counter()
        try:
            page = s.floorsheet_page(0, size=50)
            n = len(page.get("floorsheets", {}).get("content", [])) if isinstance(page, dict) else 0
            total = page.get("floorsheets", {}).get("totalElements") if isinstance(page, dict) else None
            record("auth.floorsheet", "OK" if n else "ERROR",
                   f"rows={n} upstream_total={total}", (time.perf_counter() - t0) * 1000)
        except Exception as exc:  # noqa: BLE001
            record("auth.floorsheet", classify(exc), f"{type(exc).__name__}: {exc}",
                   (time.perf_counter() - t0) * 1000)

        # Yesterday rather than today: today's board is empty before the session.
        d = (datetime.now(NPT).date() - timedelta(days=1)).isoformat()
        t0 = time.perf_counter()
        try:
            page = s.eod_prices(d, page=0, size=50)
            n = len(page.get("content", [])) if isinstance(page, dict) else 0
            record("auth.eod-board", "OK" if n else "ERROR", f"date={d} rows={n}",
                   (time.perf_counter() - t0) * 1000)
        except Exception as exc:  # noqa: BLE001
            record("auth.eod-board", classify(exc), f"{type(exc).__name__}: {exc}",
                   (time.perf_counter() - t0) * 1000)


def main() -> int:
    print(f"NEPSE hosted-reachability probe · {datetime.now(NPT):%Y-%m-%d %H:%M:%S} NPT")
    print(f"target: {CONFIG.base_url}\n")

    step_egress()
    step_tls()
    step_no_ua()
    step_browser_ua()
    step_authenticated()

    critical = {r["step"]: r["outcome"] for r in results
                if r["step"] in {"auth.market-status", "auth.floorsheet"}}
    ok = critical.get("auth.market-status") == "OK" and critical.get("auth.floorsheet") == "OK"

    print("\n" + "=" * 72)
    if ok:
        print("VERDICT: HOSTABLE — NEPSE served this IP through the full auth chain.")
    else:
        hung = any(r["outcome"] == "HUNG" for r in results if r["step"].startswith("auth."))
        if hung:
            print("VERDICT: BLOCKED — connected, then silence. This is the datacenter-IP")
            print("         failure mode. Hosting on this provider will not work.")
        else:
            print("VERDICT: FAILED — see the step outcomes above; not a clean IP block.")
    print("=" * 72)

    print("\n" + json.dumps({"hostable": ok, "steps": results}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
