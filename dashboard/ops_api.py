#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, time, json, uuid, threading, subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from elasticsearch import Elasticsearch
import redis

APP_NAME = "ops_api"

ES_URL = os.getenv("ES_URL", "http://127.0.0.1:9200")
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
LOCK_TTL_SEC = int(os.getenv("LOCK_TTL_SEC", "900"))
INDEX_EVENTS = "ops-events"
INDEX_LOGS = "ops-logs"

CRAWL_ROOT = (Path(__file__).resolve().parent.parent / "crawling").resolve()
CRAWLERS: Dict[str, List[str]] = {
    "dragonforce":       ["python3", "-u", "crawler_dragonforce.py"],
    "ransomware_live":   ["python3", "-u", "crawler_ransomware_live.py"],
    "coinbase_cartel":   ["python3", "-u", "crawler_coinbase_cartel.py"],
}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---------- Elasticsearch ----------
es = Elasticsearch(ES_URL, request_timeout=10)

def ensure_indices():
    try:
        if not es.indices.exists(index=INDEX_EVENTS):
            es.indices.create(index=INDEX_EVENTS, mappings={
                "properties": {
                    "@timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                    "phase": {"type": "keyword"},
                    "crawler": {"type": "keyword"},
                    "account": {"type": "keyword"},
                    "job_id": {"type": "keyword"},
                    "message": {"type": "text"},
                }
            })
        if not es.indices.exists(index=INDEX_LOGS):
            es.indices.create(index=INDEX_LOGS, mappings={
                "properties": {
                    "@timestamp": {"type": "date"},
                    "crawler": {"type": "keyword"},
                    "account": {"type": "keyword"},
                    "job_id": {"type": "keyword"},
                    "stream": {"type": "keyword"},
                    "message": {"type": "text"},
                }
            })
    except Exception as e:
        print(f"[{APP_NAME}] ensure_indices error: {e}", file=sys.stderr)

def es_index_safe(index: str, doc: Dict[str, Any]):
    try:
        es.index(index=index, document=doc)
    except Exception as e:
        print(f"[{APP_NAME}] ES index error: {e}", file=sys.stderr)

def log_event(level: str, phase: str, crawler: str, account: str, message: str, job_id: str):
    es_index_safe(INDEX_EVENTS, {
        "@timestamp": utc_now_iso(),
        "level": level, "phase": phase, "crawler": crawler,
        "account": account, "message": message, "job_id": job_id,
    })

def log_line(crawler: str, account: str, stream: str, message: str, job_id: str):
    es_index_safe(INDEX_LOGS, {
        "@timestamp": utc_now_iso(),
        "crawler": crawler, "account": account, "stream": stream,
        "message": message, "job_id": job_id,
    })

# ---------- Redis (auto-reconnect) ----------
_redis_cli: Optional[redis.Redis] = None

def _new_redis() -> Optional[redis.Redis]:
    try:
        cli = redis.from_url(
            REDIS_URL,
            socket_connect_timeout=3,
            socket_timeout=3,
            health_check_interval=10,
            retry_on_timeout=True,
            decode_responses=True,
        )
        cli.ping()
        return cli
    except Exception as e:
        print(f"[{APP_NAME}] Redis connect error: {e}", file=sys.stderr)
        return None

def redis_client() -> redis.Redis:
    global _redis_cli
    if _redis_cli is not None:
        try:
            _redis_cli.ping()
            return _redis_cli
        except Exception:
            _redis_cli = None
    _redis_cli = _new_redis()
    if _redis_cli is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return _redis_cli

def acquire_lock(crawler: str, account: str, ttl_sec: int = LOCK_TTL_SEC) -> str:
    r = redis_client()
    key = f"lock:crawler:{crawler}:{account}"
    ok = r.set(name=key, value=str(int(time.time())), nx=True, ex=ttl_sec)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Lock exists: {key}")
    return key

def release_lock(lock_key: str):
    try:
        redis_client().delete(lock_key)
    except Exception:
        pass

def list_current_locks() -> List[Dict[str, Any]]:
    try:
        r = redis_client()
    except HTTPException:
        return []
    out = []
    try:
        for key in r.scan_iter("lock:crawler:*"):
            ttl = r.ttl(key)
            out.append({"key": key, "ttl_sec": ttl})
    except Exception as e:
        print(f"[{APP_NAME}] Redis scan error: {e}", file=sys.stderr)
    return out

# ---------- Runner ----------
def run_crawler_thread(crawler: str, account: str, params: Optional[Dict[str, Any]], lock_key: str, job_id: str):
    cmd = CRAWLERS[crawler][:]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    if params:
        env["CRAWLER_PARAMS_JSON"] = json.dumps(params, ensure_ascii=False)

    try:
        log_event("INFO", "start", crawler, account, f"spawn {cmd}", job_id)
        proc = subprocess.Popen(
            cmd, cwd=str(CRAWL_ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            log_line(crawler, account, "OUT", line.rstrip("\n"), job_id)
        rc = proc.wait()
        log_event("INFO", "end", crawler, account, f"exit rc={rc}", job_id)
    except Exception as e:
        log_event("ERROR", "exception", crawler, account, str(e), job_id)
    finally:
        release_lock(lock_key)

# ---------- FastAPI ----------
app = FastAPI(title="Ops API", version="1.0.1")

class TriggerReq(BaseModel):
    crawler: str
    account: str
    params: Optional[Dict[str, Any]] = None

@app.on_event("startup")
def _startup():
    ensure_indices()

@app.get("/healthz")
def healthz():
    ok_es = False
    ok_redis = False
    try:
        es.info(); ok_es = True
    except Exception:
        ok_es = False
    try:
        redis_client().ping(); ok_redis = True
    except Exception:
        ok_redis = False
    return {"status": "ok", "es": ok_es, "redis": ok_redis, "time": utc_now_iso()}

@app.get("/locks")
def locks():
    return {"locks": list_current_locks()}

@app.post("/trigger")
def trigger(req: TriggerReq):
    if req.crawler not in CRAWLERS:
        raise HTTPException(status_code=400, detail=f"unknown crawler: {req.crawler}")
    lock_key = acquire_lock(req.crawler, req.account, LOCK_TTL_SEC)
    job_id = f"{req.crawler}-{int(time.time())}-{uuid.uuid4().hex[:6]}"

    th = threading.Thread(
        target=run_crawler_thread,
        args=(req.crawler, req.account, req.params, lock_key, job_id),
        daemon=True,
    )
    th.start()

    log_event("INFO", "queued", req.crawler, req.account, f"job_id={job_id}", job_id)
    return {"job_id": job_id, "lock_key": lock_key}

