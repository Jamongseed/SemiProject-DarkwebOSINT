#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run concurrently:
- crawler_dragonforce.py
- crawler_ransomware_live.py
- crawler_coinbase_cartel.py

--verbose로 전체 콘솔 출력 가능, --add-pattern으로 허용 패턴 추가 가능.
"""

import asyncio
import os
import sys
import re
import argparse
from pathlib import Path
from datetime import datetime

# ── OPTIONS ──
CRAWLERS = [
    ("dragonforce",       ["python3", "-u", "crawler_dragonforce.py"]),
    ("ransomware_live",   ["python3", "-u", "crawler_ransomware_live.py"]),
    ("coinbase_cartel",   ["python3", "-u", "crawler_coinbase_cartel.py"]),
]

LOG_DIR = Path("outputs/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

RESET = "\033[0m"
COLORS = {
    "dragonforce":     "\033[1;32m",  # bold green
    "ransomware_live": "\033[1;34m",  # bold blue
    "coinbase_cartel": "\033[1;35m",  # bold magenta
    "ERR":             "\033[1;31m",  # bold red
    "OUT":             "\033[0;37m",  # gray
    "HDR":             "\033[1;36m",  # cyan
}
COLORS_ENABLED = sys.stdout.isatty()

DEFAULT_ALLOW_PATTERNS = [
    r"실행 시작", r"로그 파일", r"종료\s*\(rc=",
    r"데이터 크롤링을 시작", r"크롤링 성공", r"파싱.*시작", r"파싱.*완료",
    r"CSV .*완료", r"통합\(덮어쓰기\)", r"저장 완료", r"통계\(append\)",
    r"총\s+\d+개", r"총\s+\d+개의 페이지", r"페이지\s*\d+.*데이터 요청 시도", r"데이터 로드 성공",
    r"접속 시도", r"접속 성공",
    r"(ERROR|Error|Exception|Traceback|오류|에러|실패|failed|timeout|타임아웃)"
]

def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def colorize(s: str, color_key: str | None) -> str:
    if not COLORS_ENABLED or not color_key:
        return s
    return f"{COLORS.get(color_key,'')}{s}{RESET}"

def log_path(name: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{name}_{stamp}.log"

async def _read_stream(
    stream: asyncio.StreamReader,
    name: str,
    kind: str,
    f,
    allow_re: re.Pattern,
    verbose: bool,
    max_len: int,
):
    prefix = f"[{name}][{kind}]"
    suppressed = 0

    while True:
        line = await stream.readline()
        if not line:
            break
        try:
            text = line.decode(errors="replace").rstrip("\n")
        except Exception:
            text = str(line).rstrip("\n")

        file_line = f"{ts()} {prefix} {text}\n"
        f.write(file_line)
        f.flush()

        to_print = verbose or (allow_re.search(text) and (len(text) <= max_len))
        if to_print:
            console_line = f"{ts()} {prefix} {text}"
            color_key = "ERR" if kind == "ERR" else name
            print(colorize(console_line, color_key))
            if suppressed:
                summary = f"{ts()} [{name}] … {suppressed} line(s) filtered"
                print(colorize(summary, "OUT"))
                suppressed = 0
        else:
            suppressed += 1

    if suppressed:
        summary = f"{ts()} [{name}] … {suppressed} line(s) filtered (final)"
        print(colorize(summary, "OUT"))

async def run_one(
    name: str,
    cmd: list[str],
    allow_re: re.Pattern,
    verbose: bool,
    max_len: int,
    env: dict | None = None,
) -> tuple[str, int, Path]:
    script = cmd[-1]
    if not Path(script).exists():
        err = f"{ts()} [{name}][ERR] 스크립트 없음: {script}"
        print(colorize(err, "ERR"))
        return name, 127, Path("/dev/null")

    lp = log_path(name)
    print(colorize(f"{ts()} [{name}] 실행 시작 → {' '.join(cmd)}", name))
    print(colorize(f"{ts()} [{name}] 로그 파일: {lp.resolve()}", "HDR"))

    env2 = os.environ.copy()
    env2["PYTHONUNBUFFERED"] = "1"
    if env:
        env2.update(env)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env2,
    )

    with lp.open("a", encoding="utf-8") as f:
        t_out = asyncio.create_task(_read_stream(proc.stdout, name, "OUT", f, allow_re, verbose, max_len))
        t_err = asyncio.create_task(_read_stream(proc.stderr, name, "ERR", f, allow_re, verbose, max_len))

        try:
            rc = await proc.wait()
            await asyncio.gather(t_out, t_err)
        except asyncio.CancelledError:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            raise

    print(colorize(f"{ts()} [{name}] 종료 (rc={rc})", name))
    return name, rc, lp

async def main_async(args):
    patterns = list(DEFAULT_ALLOW_PATTERNS)
    if args.add_pattern:
        patterns.extend(args.add_pattern)
    allow_re = re.compile("|".join(patterns))

    global COLORS_ENABLED
    if args.no_color:
        COLORS_ENABLED = False

    tasks = [
        asyncio.create_task(
            run_one(name, cmd, allow_re, args.verbose, args.max_len, env=None)
        )
        for name, cmd in CRAWLERS
    ]

    results = []
    try:
        results = await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print(colorize(f"{ts()} [runner][ERR] KeyboardInterrupt 감지, 자식 프로세스 종료 시도...", "ERR"))
        for t in tasks:
            t.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass
        sys.exit(130)

    print("\n" + "=" * 80)
    print(colorize("종료 요약:", "HDR"))
    for name, rc, lp in results:
        status = "OK" if rc == 0 else f"FAIL({rc})"
        line = f"- {name:<17} rc={rc:<3} status={status:<10} log={lp.resolve()}"
        print(colorize(line, name if rc == 0 else "ERR"))
    print("=" * 80 + "\n")

def parse_args():
    p = argparse.ArgumentParser(description="Run crawlers concurrently with console log filtering.")
    p.add_argument("--verbose", action="store_true", help="콘솔에 전체 로그 출력(필터 비활성화)")
    p.add_argument("--add-pattern", action="append", default=[], help="콘솔 허용 추가 패턴(정규식). 여러 번 지정 가능")
    p.add_argument("--max-len", type=int, default=220, help="콘솔에 출력할 최대 라인 길이(기본 220)")
    p.add_argument("--no-color", action="store_true", help="콘솔 색상 비활성화")
    return p.parse_args()

def main():
    if sys.version_info < (3, 8):
        print("PY !>= Python 3.8")
        sys.exit(2)
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print(colorize(f"{ts()} [runner][ERR] 강제 종료", "ERR"))
        sys.exit(130)

if __name__ == "__main__":
    main()

