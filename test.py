#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║        🏗️  INFRASTRUCTURE CAPACITY PLANNER  🏗️                   ║
║   Long-Running Traffic Generator — Run for Hours, Find Limits   ║
╚══════════════════════════════════════════════════════════════════╝

Purpose:
  Run continuous traffic for 1, 2, 4, 8+ hours to determine
  your server's TRUE capacity for infrastructure planning.

How it works:
  1. Starts with low traffic, auto-scales up
  2. Maintains 95%+ success rate (real successful traffic)
  3. Detects server crashes → waits → retries automatically
  4. Records everything: peak RPS, crash times, recovery times
  5. Generates infra planning report at the end

Output:
  • Live dashboard with real-time metrics
  • Crash/recovery timeline
  • Infrastructure recommendation report
  • HTML report with charts

⚠️  USE ONLY ON DOMAINS YOU OWN OR HAVE PERMISSION TO TEST.
"""

import asyncio
import aiohttp
import time
import random
import sys
import os
import json
import signal
import argparse
import statistics
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, List

try:
    import brotli
    HAS_BROTLI = True
except ImportError:
    HAS_BROTLI = False


# ─────────────────────────────────────────────
#  COLORS
# ─────────────────────────────────────────────
class C:
    RST = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
    RED = "\033[91m"; GREEN = "\033[92m"; YELLOW = "\033[93m"
    BLUE = "\033[94m"; CYAN = "\033[96m"; WHITE = "\033[97m"
    GRAY = "\033[90m"; MAGENTA = "\033[95m"
    BG_GREEN = "\033[42m"; BG_YELLOW = "\033[43m"; BG_RED = "\033[41m"; BG_BLUE = "\033[44m"

    @staticmethod
    def c(text, color):
        return f"{color}{text}{C.RST}"


# ─────────────────────────────────────────────
#  USER SIMULATION DATA
# ─────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]

REFERRERS = [
    "https://www.google.com/", "https://www.google.co.in/",
    "https://www.bing.com/", "https://duckduckgo.com/",
    "https://www.linkedin.com/", "https://twitter.com/",
    "https://github.com/", "", "", "",  # weighted direct
]

ACCEPT_LANGS = [
    "en-US,en;q=0.9", "en-GB,en;q=0.9", "en-IN,en;q=0.9,hi;q=0.8",
    "en;q=0.8", "en-US,en;q=0.9,fr;q=0.5",
]


# ─────────────────────────────────────────────
#  DATA MODELS
# ─────────────────────────────────────────────
@dataclass
class CrashEvent:
    timestamp: float = 0.0
    detected_at: str = ""
    consecutive_failures: int = 0
    error_type: str = ""
    recovery_time: float = 0.0  # seconds to recover
    recovered: bool = False
    users_at_crash: int = 0
    rps_at_crash: float = 0.0


@dataclass
class TimeSlice:
    """5-minute aggregated metrics."""
    timestamp: float = 0.0
    time_label: str = ""
    total_requests: int = 0
    success: int = 0
    failures: int = 0
    avg_rps: float = 0.0
    avg_rt: float = 0.0
    p95_rt: float = 0.0
    error_rate: float = 0.0
    concurrency: int = 0
    bytes_received: int = 0
    server_status: str = "healthy"  # healthy, degraded, crashed


@dataclass
class InfraStats:
    # Overall
    total_success: int = 0
    total_failed: int = 0
    total_bytes: int = 0
    response_times: list = field(default_factory=list)
    status_codes: dict = field(default_factory=lambda: defaultdict(int))
    errors: dict = field(default_factory=lambda: defaultdict(int))
    start_time: float = 0.0
    end_time: float = 0.0

    # Capacity metrics
    peak_healthy_rps: float = 0
    sustained_healthy_rps: float = 0
    max_concurrency_reached: int = 0

    # Crash tracking
    crashes: list = field(default_factory=list)
    total_downtime: float = 0.0

    # Time-series (5-min slices)
    time_slices: list = field(default_factory=list)

    # Per-second tracking (for live display)
    current_rps: float = 0
    current_sr: float = 100.0
    current_concurrency: int = 0
    current_avg_rt: float = 0

    # Sustained tracking
    healthy_rps_samples: list = field(default_factory=list)

    @property
    def total(self):
        return self.total_success + self.total_failed

    @property
    def elapsed(self):
        end = self.end_time or time.time()
        return end - self.start_time if self.start_time else 0

    @property
    def success_rate(self):
        return (self.total_success / self.total * 100) if self.total > 0 else 100

    @property
    def avg_rps(self):
        return self.total / self.elapsed if self.elapsed > 0 else 0

    @property
    def uptime_pct(self):
        if self.elapsed == 0:
            return 100
        return max(0, (self.elapsed - self.total_downtime) / self.elapsed * 100)

    def percentile(self, p):
        if not self.response_times:
            return 0
        s = sorted(self.response_times)
        idx = min(int(len(s) * p / 100), len(s) - 1)
        return s[idx]


# ─────────────────────────────────────────────
#  DASHBOARD
# ─────────────────────────────────────────────
class Dashboard:

    @staticmethod
    def banner():
        print(C.c("""
 ██████╗ █████╗ ██████╗  █████╗  ██████╗██╗████████╗██╗   ██╗
██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██║╚══██╔══╝╚██╗ ██╔╝
██║     ███████║██████╔╝███████║██║     ██║   ██║    ╚████╔╝
██║     ██╔══██║██╔═══╝ ██╔══██║██║     ██║   ██║     ╚██╔╝
╚██████╗██║  ██║██║     ██║  ██║╚██████╗██║   ██║      ██║
 ╚═════╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝ ╚═════╝╚═╝   ╚═╝      ╚═╝""", C.CYAN))
        print(C.c("        Infrastructure Capacity Planner v2.0", C.GRAY))
        print()

    @staticmethod
    def config(url, duration_hrs, max_users, endpoints, strategy):
        duration_str = f"{duration_hrs}h" if duration_hrs >= 1 else f"{int(duration_hrs * 60)}m"
        print(C.c("  ┌─── Test Plan ─────────────────────────────────────────────┐", C.BLUE))
        print(C.c(f"  │  Target         : ", C.BLUE) + C.c(url, C.WHITE))
        print(C.c(f"  │  Strategy       : ", C.BLUE) + C.c(strategy.upper(), C.YELLOW))
        print(C.c(f"  │  Duration       : ", C.BLUE) + C.c(duration_str, C.WHITE + C.BOLD))
        print(C.c(f"  │  Max Users      : ", C.BLUE) + C.c(str(max_users), C.WHITE))
        print(C.c(f"  │  Endpoints      : ", C.BLUE) + C.c(str(len(endpoints)), C.WHITE))
        for ep in endpoints:
            print(C.c(f"  │    → ", C.GRAY) + C.c(ep, C.WHITE))
        print(C.c(f"  │  Brotli         : ", C.BLUE) + C.c("Yes" if HAS_BROTLI else "No", C.GREEN if HAS_BROTLI else C.YELLOW))
        print(C.c(f"  │  Crash Recovery : ", C.BLUE) + C.c("Enabled (auto-retry)", C.GREEN))
        print(C.c("  └──────────────────────────────────────────────────────────┘", C.BLUE))

    @staticmethod
    def live(stats: InfraStats, server_state: str):
        sr = stats.current_sr
        elapsed = stats.elapsed

        hrs = int(elapsed // 3600)
        mins = int((elapsed % 3600) // 60)
        secs = int(elapsed % 60)

        if server_state == "crashed":
            badge = C.c(" CRASHED  ", C.BG_RED + C.BOLD)
        elif server_state == "recovering":
            badge = C.c(" RECOVER  ", C.BG_YELLOW)
        elif sr >= 95:
            badge = C.c(" HEALTHY  ", C.BG_GREEN)
        elif sr >= 80:
            badge = C.c(" DEGRADED ", C.BG_YELLOW)
        else:
            badge = C.c(" FAILING  ", C.BG_RED)

        sys.stdout.write(
            f"\r  {badge} "
            f"{C.c(f'{hrs:02d}:{mins:02d}:{secs:02d}', C.WHITE)} │ "
            f"👥 {C.c(str(stats.current_concurrency), C.CYAN)} │ "
            f"RPS: {C.c(f'{stats.current_rps:.0f}', C.YELLOW)} │ "
            f"✓{C.c(f'{stats.total_success:,}', C.GREEN)} "
            f"✗{C.c(f'{stats.total_failed:,}', C.RED)} │ "
            f"RT: {C.c(f'{stats.current_avg_rt:.0f}ms', C.WHITE)} │ "
            f"SR: {C.c(f'{sr:.1f}%', C.GREEN if sr >= 95 else C.RED)} │ "
            f"Peak: {C.c(f'{stats.peak_healthy_rps:.0f}', C.GREEN)} │ "
            f"Data: {C.c(format_bytes(stats.total_bytes), C.WHITE)} │ "
            f"Crashes: {C.c(str(len(stats.crashes)), C.RED if stats.crashes else C.GREEN)}"
            f"    "
        )
        sys.stdout.flush()

    @staticmethod
    def crash_detected(crash: CrashEvent):
        print(f"\n\n  {C.c('╔═══ SERVER CRASH DETECTED ═══════════════════════════════╗', C.RED + C.BOLD)}")
        print(f"  {C.c('║', C.RED)}  Time      : {crash.detected_at}")
        print(f"  {C.c('║', C.RED)}  Users     : {crash.users_at_crash}")
        print(f"  {C.c('║', C.RED)}  RPS       : {crash.rps_at_crash:.0f}")
        print(f"  {C.c('║', C.RED)}  Error     : {crash.error_type[:60]}")
        print(f"  {C.c('║', C.RED)}  Action    : Waiting 30s then retrying...")
        print(f"  {C.c('╚════════════════════════════════════════════════════════╝', C.RED)}\n")

    @staticmethod
    def recovery(crash: CrashEvent):
        print(f"\n  {C.c('✅ SERVER RECOVERED', C.GREEN + C.BOLD)} — Downtime: {crash.recovery_time:.0f}s — Resuming traffic...\n")

    @staticmethod
    def recovery_failed():
        print(f"\n  {C.c('❌ SERVER STILL DOWN', C.RED)} — Retrying in 60s...\n")

    @staticmethod
    def scaling(action: str, count: int, reason: str):
        arrow = "⬆" if "UP" in action else "⬇" if "DOWN" in action else "•"
        print(f"\n  {C.c(arrow, C.CYAN)} {action} → {C.c(str(count), C.YELLOW)} users ({reason})")

    @staticmethod
    def final_report(stats: InfraStats, url: str):
        print("\n\n")
        print(C.c("  ╔══════════════════════════════════════════════════════════════╗", C.CYAN + C.BOLD))
        print(C.c("  ║          INFRASTRUCTURE CAPACITY REPORT                     ║", C.CYAN + C.BOLD))
        print(C.c("  ╚══════════════════════════════════════════════════════════════╝", C.CYAN + C.BOLD))

        elapsed = stats.elapsed
        hrs = int(elapsed // 3600)
        mins = int((elapsed % 3600) // 60)

        # Test Summary
        print(C.c("\n  ── Test Summary ───────────────────────────────────────────", C.BLUE))
        print(f"  Target              : {C.c(url, C.WHITE)}")
        print(f"  Total Runtime       : {C.c(f'{hrs}h {mins}m', C.WHITE + C.BOLD)}")
        print(f"  Total Requests      : {C.c(f'{stats.total:,}', C.WHITE)}")
        print(f"  Successful          : {C.c(f'{stats.total_success:,}', C.GREEN)}")
        print(f"  Failed              : {C.c(f'{stats.total_failed:,}', C.RED)}")
        print(f"  Success Rate        : {C.c(f'{stats.success_rate:.2f}%', C.GREEN if stats.success_rate > 95 else C.YELLOW)}")
        print(f"  Data Transferred    : {C.c(format_bytes(stats.total_bytes), C.WHITE)}")

        # Server Performance
        print(C.c("\n  ── Server Performance ─────────────────────────────────────", C.BLUE))
        print(f"  Average RPS         : {C.c(f'{stats.avg_rps:.1f}', C.CYAN)}")
        print(f"  Peak Healthy RPS    : {C.c(f'{stats.peak_healthy_rps:.0f}', C.GREEN + C.BOLD)}")
        print(f"  Sustained RPS (95%) : {C.c(f'{stats.sustained_healthy_rps:.0f}', C.GREEN)}")
        print(f"  Max Concurrency     : {C.c(str(stats.max_concurrency_reached), C.WHITE)}")

        # Response Times
        print(C.c("\n  ── Response Times ─────────────────────────────────────────", C.BLUE))
        if stats.response_times:
            print(f"  Min                 : {C.c(f'{min(stats.response_times):.0f} ms', C.GREEN)}")
            print(f"  Average             : {C.c(f'{statistics.mean(stats.response_times):.0f} ms', C.YELLOW)}")
            print(f"  P50                 : {C.c(f'{stats.percentile(50):.0f} ms', C.WHITE)}")
            print(f"  P90                 : {C.c(f'{stats.percentile(90):.0f} ms', C.WHITE)}")
            print(f"  P95                 : {C.c(f'{stats.percentile(95):.0f} ms', C.WHITE)}")
            print(f"  P99                 : {C.c(f'{stats.percentile(99):.0f} ms', C.WHITE)}")
            print(f"  Max                 : {C.c(f'{max(stats.response_times):.0f} ms', C.RED)}")

        # Status Codes
        if stats.status_codes:
            print(C.c("\n  ── Status Codes ───────────────────────────────────────────", C.BLUE))
            total = sum(stats.status_codes.values())
            for code in sorted(stats.status_codes.keys()):
                count = stats.status_codes[code]
                pct = count / total * 100
                color = C.GREEN if 200 <= code < 300 else C.YELLOW if 300 <= code < 400 else C.RED
                bar = "█" * min(int(pct / 2), 50)
                print(f"  {C.c(str(code), color)}  {C.c(bar, color)} {count:,} ({pct:.1f}%)")

        # Crash Timeline
        print(C.c("\n  ── Stability & Crashes ────────────────────────────────────", C.BLUE))
        print(f"  Total Crashes       : {C.c(str(len(stats.crashes)), C.RED if stats.crashes else C.GREEN)}")
        print(f"  Total Downtime      : {C.c(f'{stats.total_downtime:.0f}s', C.RED if stats.total_downtime > 0 else C.GREEN)}")
        print(f"  Uptime              : {C.c(f'{stats.uptime_pct:.2f}%', C.GREEN if stats.uptime_pct > 99 else C.YELLOW)}")

        if stats.crashes:
            print(C.c("\n  ── Crash Timeline ─────────────────────────────────────────", C.BLUE))
            for i, crash in enumerate(stats.crashes, 1):
                status = C.c("Recovered", C.GREEN) if crash.recovered else C.c("Not recovered", C.RED)
                print(f"  Crash #{i}: {crash.detected_at} | "
                      f"{crash.users_at_crash} users | "
                      f"{crash.rps_at_crash:.0f} RPS | "
                      f"Down {crash.recovery_time:.0f}s | "
                      f"{status}")

        # Errors
        if stats.errors:
            print(C.c("\n  ── Top Errors ─────────────────────────────────────────────", C.BLUE))
            for err, count in sorted(stats.errors.items(), key=lambda x: -x[1])[:7]:
                print(f"  {C.c(f'[{count:,}x]', C.RED)} {err}")

        # INFRASTRUCTURE RECOMMENDATION
        print(C.c("\n  ╔════════════════════════════════════════════════════════════╗", C.MAGENTA + C.BOLD))
        print(C.c("  ║       INFRASTRUCTURE RECOMMENDATION                       ║", C.MAGENTA + C.BOLD))
        print(C.c("  ╚════════════════════════════════════════════════════════════╝", C.MAGENTA + C.BOLD))

        safe_rps = stats.sustained_healthy_rps * 0.7
        visitors_hour = safe_rps * 3600 / 10
        visitors_day = visitors_hour * 24
        visitors_month = visitors_day * 30

        print(f"\n  Your server's proven capacity:")
        print(f"  ├─ Sustained RPS    : {C.c(f'{stats.sustained_healthy_rps:.0f}', C.GREEN)} requests/second")
        print(f"  ├─ Safe RPS (70%)   : {C.c(f'{safe_rps:.0f}', C.CYAN)} requests/second")
        print(f"  ├─ Visitors/Hour    : {C.c(f'{visitors_hour:,.0f}', C.WHITE)}")
        print(f"  ├─ Visitors/Day     : {C.c(f'{visitors_day:,.0f}', C.WHITE)}")
        print(f"  └─ Visitors/Month   : {C.c(f'{visitors_month:,.0f}', C.WHITE)}")

        if stats.crashes:
            crash_rps = min(c.rps_at_crash for c in stats.crashes)
            crash_users = min(c.users_at_crash for c in stats.crashes)
            print(f"\n  {C.c('Server crashes at:', C.RED)}")
            print(f"  ├─ {C.c(f'{crash_rps:.0f}', C.RED)} RPS")
            print(f"  └─ {C.c(str(crash_users), C.RED)} concurrent users")

        # Scaling recommendations
        print(f"\n  {C.c('Recommendations:', C.YELLOW + C.BOLD)}")
        if stats.success_rate > 99 and not stats.crashes:
            print(f"  ✅ Current infra handles your test load perfectly")
            print(f"  ✅ No crashes detected over {hrs}h {mins}m runtime")
            print(f"  💡 You may be OVER-provisioned — consider downgrading to save cost")
        elif stats.success_rate > 95 and len(stats.crashes) <= 1:
            print(f"  ✅ Current infra is adequate for {visitors_day:,.0f} daily visitors")
            if stats.crashes:
                print(f"  ⚠️  1 crash detected — add headroom with next tier plan")
            print(f"  💡 For 2x growth, upgrade to next plan tier")
        else:
            print(f"  ❌ Current infra is insufficient for target load")
            print(f"  🔧 Upgrade plan OR add these optimizations:")
            print(f"     • Enable server-side caching (Redis/Memcached)")
            print(f"     • Add Cloudflare CDN (free tier helps significantly)")
            print(f"     • Optimize database queries")
            print(f"     • Increase PHP/Node worker processes")
            if len(stats.crashes) > 2:
                print(f"  🚨 {len(stats.crashes)} crashes in {hrs}h — seriously consider VPS/Cloud upgrade")

        # Hostinger-specific advice
        print(f"\n  {C.c('Hostinger Plan Guide:', C.CYAN)}")
        if safe_rps < 100:
            print(f"  Current capacity fits: {C.c('Single/Premium Plan', C.WHITE)}")
            print(f"  For more: Upgrade to {C.c('Business Plan', C.YELLOW)}")
        elif safe_rps < 500:
            print(f"  Current capacity fits: {C.c('Business Plan', C.WHITE)}")
            print(f"  For more: Upgrade to {C.c('Cloud Hosting or VPS', C.YELLOW)}")
        elif safe_rps < 2000:
            print(f"  Current capacity fits: {C.c('Cloud Hosting / VPS 2', C.WHITE)}")
            print(f"  For more: Upgrade to {C.c('VPS 4 or Dedicated', C.YELLOW)}")
        else:
            print(f"  Current capacity fits: {C.c('VPS 4+ / Dedicated Server', C.WHITE)}")
            print(f"  🎉 Excellent capacity!")

        print(C.c("\n  ══════════════════════════════════════════════════════════════\n", C.CYAN))


def format_bytes(b: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def format_duration(secs: float) -> str:
    hrs = int(secs // 3600)
    mins = int((secs % 3600) // 60)
    s = int(secs % 60)
    if hrs > 0:
        return f"{hrs}h {mins}m {s}s"
    elif mins > 0:
        return f"{mins}m {s}s"
    return f"{s}s"


# ─────────────────────────────────────────────
#  MAIN ENGINE
# ─────────────────────────────────────────────
class InfraPlanner:
    def __init__(
        self,
        base_url: str,
        endpoints: list,
        duration_hours: float = 1.0,
        max_users: int = 200,
        start_users: int = 10,
        timeout: int = 20,
        strategy: str = "auto",
        target_sr: float = 95.0,
        report_path: str = "infra_report.html",
    ):
        self.base_url = base_url.rstrip("/")
        self.endpoints = endpoints
        self.duration_secs = int(duration_hours * 3600)
        self.duration_hours = duration_hours
        self.max_users = max_users
        self.start_users = start_users
        self.timeout = timeout
        self.strategy = strategy
        self.target_sr = target_sr
        self.report_path = report_path

        self.stats = InfraStats()
        self._stop = asyncio.Event()
        self._workers: set = set()
        self._current_users = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._server_state = "healthy"  # healthy, degraded, crashed, recovering

        # Per-second window
        self._w_success = 0
        self._w_fail = 0
        self._w_rts: list = []
        self._w_lock = asyncio.Lock()

        # 5-minute slice accumulator
        self._slice_success = 0
        self._slice_fail = 0
        self._slice_rts: list = []
        self._slice_bytes = 0
        self._slice_start = 0.0

        # Crash detection
        self._consecutive_fail_secs = 0
        self._current_crash: Optional[CrashEvent] = None

    # ── Request Logic ──────────────────────────

    def _headers(self) -> dict:
        enc = "gzip, deflate, br" if HAS_BROTLI else "gzip, deflate"
        ref = random.choice(REFERRERS)
        h = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": random.choice(ACCEPT_LANGS),
            "Accept-Encoding": enc,
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }
        if ref:
            h["Referer"] = ref
        return h

    def _url(self) -> str:
        ep = random.choice(self.endpoints)
        return ep if ep.startswith("http") else f"{self.base_url}{ep}"

    async def _request(self, retry: int = 0) -> bool:
        if self._stop.is_set() or self._server_state == "crashed":
            return False

        start = time.time()
        try:
            async with self._session.get(self._url(), headers=self._headers(), allow_redirects=True) as resp:
                body = await resp.read()
                ms = (time.time() - start) * 1000

                async with self._w_lock:
                    self.stats.response_times.append(ms)
                    self.stats.status_codes[resp.status] += 1
                    self._w_rts.append(ms)
                    self._slice_rts.append(ms)

                    if 200 <= resp.status < 400:
                        self.stats.total_success += 1
                        self.stats.total_bytes += len(body)
                        self._w_success += 1
                        self._slice_success += 1
                        self._slice_bytes += len(body)
                        return True
                    else:
                        self.stats.total_failed += 1
                        self._w_fail += 1
                        self._slice_fail += 1
                        if resp.status >= 500 and retry < 2:
                            await asyncio.sleep(0.5 * (retry + 1))
                            return await self._request(retry + 1)
                        return False

        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            ms = (time.time() - start) * 1000
            err = f"{type(e).__name__}: {str(e)[:50]}"
            async with self._w_lock:
                self.stats.total_failed += 1
                self.stats.errors[err] += 1
                self.stats.response_times.append(ms)
                self._w_fail += 1
                self._slice_fail += 1

            if retry < 1 and not self._stop.is_set():
                await asyncio.sleep(0.3 * (retry + 1))
                return await self._request(retry + 1)
            return False

        except Exception as e:
            async with self._w_lock:
                self.stats.total_failed += 1
                self.stats.errors[f"{type(e).__name__}: {str(e)[:50]}"] += 1
                self._w_fail += 1
                self._slice_fail += 1
            return False

    async def _worker_loop(self):
        while not self._stop.is_set() and self._server_state != "crashed":
            await self._request()
            await asyncio.sleep(random.uniform(0.01, 0.12))

    # ── Worker Management ──────────────────────

    async def _add_workers(self, count: int):
        for _ in range(count):
            if self._stop.is_set():
                break
            task = asyncio.create_task(self._worker_loop())
            self._workers.add(task)
            task.add_done_callback(self._workers.discard)
            self._current_users += 1
            await asyncio.sleep(0.03)  # stagger to avoid flood

    async def _remove_workers(self, count: int):
        removed = 0
        for task in list(self._workers):
            if removed >= count:
                break
            task.cancel()
            self._workers.discard(task)
            removed += 1
            self._current_users = max(0, self._current_users - 1)

    async def _kill_all_workers(self):
        for task in list(self._workers):
            task.cancel()
        self._workers.clear()
        self._current_users = 0

    # ── Crash Detection & Recovery ─────────────

    async def _check_crash(self):
        """Detect server crash and handle recovery."""
        if self._server_state == "crashed":
            return

        if self.stats.current_sr < 20 and self.stats.current_rps > 0:
            self._consecutive_fail_secs += 1
        else:
            self._consecutive_fail_secs = max(0, self._consecutive_fail_secs - 1)

        if self._consecutive_fail_secs >= 5:
            # CRASH DETECTED
            self._server_state = "crashed"
            crash = CrashEvent(
                timestamp=time.time(),
                detected_at=datetime.now().strftime("%H:%M:%S"),
                consecutive_failures=self._consecutive_fail_secs,
                error_type=next(iter(self.stats.errors), "Unknown"),
                users_at_crash=self._current_users,
                rps_at_crash=self.stats.current_rps,
            )
            self._current_crash = crash
            Dashboard.crash_detected(crash)

            # Kill all workers
            await self._kill_all_workers()

            # Recovery loop
            recovery_start = time.time()
            recovered = False

            for attempt in range(10):  # Try 10 times over ~5+ minutes
                wait_time = 30 if attempt < 3 else 60
                await asyncio.sleep(wait_time)

                if self._stop.is_set():
                    break

                # Try a single health check
                self._server_state = "recovering"
                try:
                    async with self._session.get(
                        self._url(), headers=self._headers(),
                        allow_redirects=True
                    ) as resp:
                        await resp.read()
                        if 200 <= resp.status < 400:
                            recovered = True
                            break
                except Exception:
                    Dashboard.recovery_failed()
                    continue

            crash.recovery_time = time.time() - recovery_start
            crash.recovered = recovered
            self.stats.crashes.append(crash)
            self.stats.total_downtime += crash.recovery_time

            if recovered and not self._stop.is_set():
                self._server_state = "healthy"
                self._consecutive_fail_secs = 0
                Dashboard.recovery(crash)

                # Restart with fewer users (50% of crash point)
                restart_users = max(5, crash.users_at_crash // 2)
                Dashboard.scaling("Restarting at", restart_users, "post-recovery")
                await self._add_workers(restart_users)
            else:
                if not self._stop.is_set():
                    print(f"\n  {C.c('Server did not recover after 10 attempts. Ending test.', C.RED + C.BOLD)}")
                    self._stop.set()

    # ── Metrics Loop ───────────────────────────

    async def _metrics_loop(self):
        self._slice_start = time.time()

        while not self._stop.is_set():
            await asyncio.sleep(1.0)

            async with self._w_lock:
                w_total = self._w_success + self._w_fail
                w_sr = (self._w_success / w_total * 100) if w_total > 0 else 100

                self.stats.current_rps = w_total
                self.stats.current_sr = w_sr
                self.stats.current_concurrency = self._current_users
                self.stats.current_avg_rt = (
                    statistics.mean(self._w_rts) if self._w_rts else 0
                )

                if self._current_users > self.stats.max_concurrency_reached:
                    self.stats.max_concurrency_reached = self._current_users

                # Track peak healthy RPS
                if w_sr >= 95 and w_total > self.stats.peak_healthy_rps:
                    self.stats.peak_healthy_rps = w_total

                # Track sustained healthy RPS
                if w_sr >= 95 and w_total > 0:
                    self.stats.healthy_rps_samples.append(w_total)
                    if len(self.stats.healthy_rps_samples) >= 10:
                        recent = self.stats.healthy_rps_samples[-30:]
                        sustained = statistics.mean(recent)
                        if sustained > self.stats.sustained_healthy_rps:
                            self.stats.sustained_healthy_rps = sustained

                # Reset per-second window
                self._w_success = 0
                self._w_fail = 0
                self._w_rts = []

                # 5-minute slice check
                if time.time() - self._slice_start >= 300:
                    s_total = self._slice_success + self._slice_fail
                    ts = TimeSlice(
                        timestamp=time.time(),
                        time_label=datetime.now().strftime("%H:%M"),
                        total_requests=s_total,
                        success=self._slice_success,
                        failures=self._slice_fail,
                        avg_rps=s_total / 300,
                        avg_rt=statistics.mean(self._slice_rts) if self._slice_rts else 0,
                        p95_rt=sorted(self._slice_rts)[int(len(self._slice_rts) * 0.95)] if len(self._slice_rts) > 1 else 0,
                        error_rate=(self._slice_fail / s_total * 100) if s_total > 0 else 0,
                        concurrency=self._current_users,
                        bytes_received=self._slice_bytes,
                        server_status=self._server_state,
                    )
                    self.stats.time_slices.append(ts)
                    self._slice_success = 0
                    self._slice_fail = 0
                    self._slice_rts = []
                    self._slice_bytes = 0
                    self._slice_start = time.time()

            # Crash check
            await self._check_crash()

            # Dashboard
            Dashboard.live(self.stats, self._server_state)

    # ── Auto Scaler ────────────────────────────

    async def _auto_scaler(self):
        Dashboard.scaling("Starting with", self.start_users, "initial users")
        await self._add_workers(self.start_users)
        await asyncio.sleep(5)  # stabilize

        while not self._stop.is_set():
            await asyncio.sleep(3)

            if self._server_state != "healthy":
                continue

            sr = self.stats.current_sr

            if self.strategy == "auto":
                if sr >= 98 and self._current_users < self.max_users:
                    add = min(
                        max(3, int(self._current_users * 0.15)),
                        self.max_users - self._current_users,
                    )
                    if add > 0:
                        Dashboard.scaling("Scale UP", self._current_users + add, f"SR={sr:.1f}%")
                        await self._add_workers(add)

                elif sr < 85 and self._current_users > 5:
                    remove = max(2, int(self._current_users * 0.2))
                    Dashboard.scaling("Scale DOWN", self._current_users - remove, f"SR={sr:.1f}%")
                    await self._remove_workers(remove)

                elif sr < self.target_sr and self._current_users > 5:
                    remove = max(1, int(self._current_users * 0.1))
                    Dashboard.scaling("Scale DOWN", self._current_users - remove, f"SR={sr:.1f}%")
                    await self._remove_workers(remove)

            elif self.strategy == "fixed":
                if self._current_users < self.start_users:
                    add = self.start_users - self._current_users
                    await self._add_workers(add)

            elif self.strategy == "aggressive":
                if sr >= 95 and self._current_users < self.max_users:
                    add = min(
                        max(5, int(self._current_users * 0.25)),
                        self.max_users - self._current_users,
                    )
                    if add > 0:
                        Dashboard.scaling("PUSH UP", self._current_users + add, f"aggressive, SR={sr:.1f}%")
                        await self._add_workers(add)
                elif sr < 80 and self._current_users > 5:
                    remove = max(3, int(self._current_users * 0.3))
                    Dashboard.scaling("Scale DOWN", self._current_users - remove, f"SR={sr:.1f}%")
                    await self._remove_workers(remove)

    # ── Main Run ───────────────────────────────

    async def run(self):
        Dashboard.banner()
        Dashboard.config(
            self.base_url, self.duration_hours, self.max_users,
            self.endpoints, self.strategy,
        )

        duration_str = format_duration(self.duration_secs)
        print(C.c(f"\n  🚀 Starting {duration_str} infrastructure test...\n", C.GREEN + C.BOLD))

        self.stats.start_time = time.time()

        connector = aiohttp.TCPConnector(
            limit=self.max_users + 100,
            limit_per_host=self.max_users + 50,
            ttl_dns_cache=600,
            enable_cleanup_closed=True,
            keepalive_timeout=30,
            force_close=False,
            ssl=False,
        )
        timeout = aiohttp.ClientTimeout(
            total=self.timeout,
            connect=10,
            sock_connect=10,
            sock_read=self.timeout,
        )

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self._session = session

            metrics_task = asyncio.create_task(self._metrics_loop())
            scaler_task = asyncio.create_task(self._auto_scaler())

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.duration_secs)
            except asyncio.TimeoutError:
                pass

            self._stop.set()
            await asyncio.sleep(1)

            for task in self._workers:
                task.cancel()
            metrics_task.cancel()
            scaler_task.cancel()
            await asyncio.sleep(0.5)

        self.stats.end_time = time.time()

        # Final report
        Dashboard.final_report(self.stats, self.base_url)

        if self.report_path:
            self._save_report()
            print(f"  {C.c('📄 HTML Report:', C.GREEN)} {C.c(self.report_path, C.WHITE)}\n")

    def _save_report(self):
        """Generate HTML report."""
        slices = self.stats.time_slices
        labels = [s.time_label for s in slices]
        rps = [round(s.avg_rps, 1) for s in slices]
        rt = [round(s.avg_rt, 1) for s in slices]
        err = [round(s.error_rate, 1) for s in slices]
        conc = [s.concurrency for s in slices]
        status_data = [{"code": k, "count": v} for k, v in sorted(self.stats.status_codes.items())]
        crash_data = [{"time": c.detected_at, "users": c.users_at_crash,
                       "rps": round(c.rps_at_crash), "downtime": round(c.recovery_time),
                       "recovered": c.recovered} for c in self.stats.crashes]

        sr = self.stats.success_rate
        safe = self.stats.sustained_healthy_rps * 0.7
        avg_rt = statistics.mean(self.stats.response_times) if self.stats.response_times else 0

        html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>Infra Report - {self.base_url}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#0f172a;color:#e2e8f0;padding:2rem}}
.c{{max-width:1200px;margin:0 auto}}
h1{{text-align:center;font-size:2rem;color:#38bdf8;margin-bottom:.3rem}}
.sub{{text-align:center;color:#64748b;margin-bottom:2rem}}
.g{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1rem;margin-bottom:2rem}}
.cd{{background:#1e293b;border-radius:12px;padding:1.2rem;border:1px solid #334155}}
.cl{{color:#94a3b8;font-size:.75rem;text-transform:uppercase}}
.cv{{font-size:1.5rem;font-weight:700;margin-top:.3rem}}
.grn{{color:#4ade80}}.red{{color:#f87171}}.blu{{color:#38bdf8}}.ylw{{color:#facc15}}.wht{{color:#f1f5f9}}
.ch{{background:#1e293b;border-radius:12px;padding:1.5rem;border:1px solid #334155;margin-bottom:1.5rem}}
.ct{{color:#94a3b8;margin-bottom:1rem;font-weight:600}}
canvas{{max-height:280px}}
table{{width:100%;border-collapse:collapse;margin-top:.5rem}}th,td{{padding:.5rem .8rem;text-align:left;border-bottom:1px solid #334155;font-size:.9rem}}th{{color:#94a3b8}}
.ft{{text-align:center;color:#475569;margin-top:2rem;font-size:.8rem}}
</style></head><body><div class="c">
<h1>Infrastructure Capacity Report</h1>
<p class="sub">{self.base_url} | {format_duration(self.stats.elapsed)} | {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
<div class="g">
<div class="cd"><div class="cl">Successful</div><div class="cv grn">{self.stats.total_success:,}</div></div>
<div class="cd"><div class="cl">Failed</div><div class="cv red">{self.stats.total_failed:,}</div></div>
<div class="cd"><div class="cl">Success Rate</div><div class="cv {'grn' if sr>95 else 'ylw'}">{sr:.1f}%</div></div>
<div class="cd"><div class="cl">Peak RPS</div><div class="cv grn">{self.stats.peak_healthy_rps:.0f}</div></div>
<div class="cd"><div class="cl">Sustained RPS</div><div class="cv blu">{self.stats.sustained_healthy_rps:.0f}</div></div>
<div class="cd"><div class="cl">Avg RT</div><div class="cv wht">{avg_rt:.0f}ms</div></div>
<div class="cd"><div class="cl">Crashes</div><div class="cv {'red' if self.stats.crashes else 'grn'}">{len(self.stats.crashes)}</div></div>
<div class="cd"><div class="cl">Uptime</div><div class="cv {'grn' if self.stats.uptime_pct>99 else 'ylw'}">{self.stats.uptime_pct:.1f}%</div></div>
<div class="cd"><div class="cl">Safe RPS</div><div class="cv blu">{safe:.0f}</div></div>
<div class="cd"><div class="cl">Est. Visitors/Day</div><div class="cv wht">{safe*3600/10*24:,.0f}</div></div>
</div>
<div class="ch"><div class="ct">RPS & Concurrency Over Time (5-min intervals)</div><canvas id="c1"></canvas></div>
<div class="ch"><div class="ct">Response Time & Error Rate</div><canvas id="c2"></canvas></div>
{'<div class="ch"><div class="ct">Crash Timeline</div><table><tr><th>Time</th><th>Users</th><th>RPS</th><th>Downtime</th><th>Recovered</th></tr>' + ''.join(f'<tr><td>{c["time"]}</td><td>{c["users"]}</td><td>{c["rps"]}</td><td>{c["downtime"]}s</td><td style="color:{"#4ade80" if c["recovered"] else "#f87171"}">{"Yes" if c["recovered"] else "No"}</td></tr>' for c in crash_data) + '</table></div>' if crash_data else ''}
<p class="ft">Infrastructure Capacity Planner | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
</div><script>
const L={json.dumps(labels)},R={json.dumps(rps)},RT={json.dumps(rt)},ER={json.dumps(err)},CO={json.dumps(conc)};
const o={{responsive:true,interaction:{{mode:'index',intersect:false}},scales:{{x:{{ticks:{{color:'#64748b'}}}},y:{{ticks:{{color:'#64748b'}},grid:{{color:'#334155'}}}}}},plugins:{{legend:{{labels:{{color:'#94a3b8'}}}}}}}};
new Chart(document.getElementById('c1'),{{type:'line',data:{{labels:L,datasets:[{{label:'Avg RPS',data:R,borderColor:'#38bdf8',fill:true,backgroundColor:'rgba(56,189,248,0.1)',tension:.3}},{{label:'Users',data:CO,borderColor:'#a78bfa',tension:.3,yAxisID:'y1'}}]}},options:{{...o,scales:{{...o.scales,y1:{{position:'right',ticks:{{color:'#a78bfa'}},grid:{{display:false}}}}}}}}}});
new Chart(document.getElementById('c2'),{{type:'line',data:{{labels:L,datasets:[{{label:'Avg RT (ms)',data:RT,borderColor:'#facc15',tension:.3}},{{label:'Error %',data:ER,borderColor:'#f87171',tension:.3,yAxisID:'y1'}}]}},options:{{...o,scales:{{...o.scales,y1:{{position:'right',ticks:{{color:'#f87171'}},grid:{{display:false}}}}}}}}}});
</script></body></html>"""

        with open(self.report_path, "w", encoding="utf-8") as f:
            f.write(html)

    def stop(self):
        self._stop.set()


# ─────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Infrastructure Capacity Planner — Long-running traffic generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
STRATEGIES:
  auto       — Auto-scale to find max capacity while keeping 95%+ success (recommended)
  fixed      — Maintain exact user count for duration
  aggressive — Push hard and fast to find breaking point quickly

DURATION EXAMPLES:
  -d 0.5   → 30 minutes
  -d 1     → 1 hour
  -d 2     → 2 hours
  -d 4     → 4 hours
  -d 8     → 8 hours (overnight test)

USAGE EXAMPLES:

  # 1 hour auto-scale test (recommended first run)
  python infra_planner.py -u https://yourdomain.com -d 1

  # 2 hour test with 300 max users
  python infra_planner.py -u https://yourdomain.com -d 2 --max 300

  # 4 hour soak test with fixed 100 users
  python infra_planner.py -u https://yourdomain.com -d 4 --start 100 --strategy fixed

  # Quick 30 min aggressive test
  python infra_planner.py -u https://yourdomain.com -d 0.5 --strategy aggressive --max 500

  # Test multiple endpoints
  python infra_planner.py -u https://yourdomain.com -d 1 -e "/" "/about" "/api/health"

  # Overnight 8 hour stability test
  python infra_planner.py -u https://yourdomain.com -d 8 --max 200

USE ONLY ON DOMAINS YOU OWN OR HAVE PERMISSION TO TEST.
        """,
    )

    parser.add_argument("-u", "--url", required=True, help="Your domain URL")
    parser.add_argument("-d", "--duration", type=float, default=1.0,
                        help="Duration in HOURS (default: 1.0, use 0.5 for 30min)")
    parser.add_argument("--max", type=int, default=200,
                        help="Max concurrent users (default: 200)")
    parser.add_argument("--start", type=int, default=10,
                        help="Starting users (default: 10)")
    parser.add_argument("--strategy", choices=["auto", "fixed", "aggressive"], default="auto",
                        help="Scaling strategy (default: auto)")
    parser.add_argument("-e", "--endpoints", nargs="+", default=["/"],
                        help="Endpoints to test (default: /)")
    parser.add_argument("--timeout", type=int, default=20,
                        help="Request timeout seconds (default: 20)")
    parser.add_argument("--target-sr", type=float, default=95.0,
                        help="Target success rate (default: 95)")
    parser.add_argument("-o", "--output", default="infra_report.html",
                        help="HTML report path")
    parser.add_argument("--no-report", action="store_true",
                        help="Skip HTML report")

    args = parser.parse_args()

    engine = InfraPlanner(
        base_url=args.url,
        endpoints=args.endpoints,
        duration_hours=args.duration,
        max_users=args.max,
        start_users=args.start,
        timeout=args.timeout,
        strategy=args.strategy,
        target_sr=args.target_sr,
        report_path=None if args.no_report else args.output,
    )

    def handle_signal(sig, frame):
        print(C.c("\n\n  Stopping early... generating report...\n", C.YELLOW))
        engine.stop()

    signal.signal(signal.SIGINT, handle_signal)

    print(C.c(f"\n  Press Ctrl+C anytime to stop and get report\n", C.GRAY))

    asyncio.run(engine.run())


if __name__ == "__main__":
    main()
