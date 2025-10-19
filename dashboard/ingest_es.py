#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, glob, hashlib
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from elasticsearch import Elasticsearch, helpers

OUTPUT_DIR = str(Path(__file__).resolve().parent.parent / "crawling" / "outputs")
INDEX_NAME = "ransomware-unified"
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")

UNIFIED_HEADERS = [
    "source","record_type","id","company","website","country","address",
    "size_bytes","size_gib","is_published","time_until_publication",
    "posted_at_utc","crawled_at_utc","crawled_at_kst",
    "ransomware_group","discovery_date","estimated_attack_date",
    "details_url","description","files_api_present"
]

DATE_FIELDS = ["posted_at_utc","crawled_at_utc","crawled_at_kst","discovery_date","estimated_attack_date"]

def to_bool(v):
    if v is None: return None
    s = str(v).strip().lower()
    if s in ("true","1","yes","y"): return True
    if s in ("false","0","no","n",""): return False
    return None

def to_int(v):
    try:
        if v in ("", None): return None
        return int(float(v))
    except Exception:
        return None

def to_float(v):
    try:
        if v in ("", None): return None
        return float(v)
    except Exception:
        return None

def to_iso(v):
    if not v or str(v).strip()=="":
        return None
    try:
        # pandas to_datetime handles most ISO/partial
        dt = pd.to_datetime(v, utc=True, errors="coerce")
        if pd.isna(dt): return None
        return dt.isoformat()
    except Exception:
        return None

def doc_id(source, rid):
    base = f"{source}|{rid}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def ensure_index(es: Elasticsearch):
    if es.indices.exists(index=INDEX_NAME):
        return
    mapping = {
        "mappings": {
            "properties": {
                "source": {"type":"keyword"},
                "record_type": {"type":"keyword"},
                "id": {"type":"keyword"},
                "company": {"type":"text","fields":{"keyword":{"type":"keyword"}}},
                "website": {"type":"keyword"},
                "country": {"type":"keyword"},
                "address": {"type":"text"},
                "size_bytes": {"type":"long"},
                "size_gib": {"type":"float"},
                "is_published": {"type":"boolean"},
                "files_api_present": {"type":"boolean"},
                "time_until_publication": {"type":"keyword"},
                "posted_at_utc": {"type":"date"},
                "crawled_at_utc": {"type":"date"},
                "crawled_at_kst": {"type":"date"},
                "ransomware_group": {"type":"keyword"},
                "discovery_date": {"type":"date"},
                "estimated_attack_date": {"type":"date"},
                "details_url": {"type":"keyword"},
                "description": {"type":"text"},
                "@timestamp": {"type":"date"},
                "source_file": {"type":"keyword"}
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=mapping)

def gen_actions(csv_path):
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    # 누락 컬럼 보강
    for col in UNIFIED_HEADERS:
        if col not in df.columns:
            df[col] = ""

    # 타입 변환
    df["is_published"] = df["is_published"].apply(to_bool)
    df["files_api_present"] = df["files_api_present"].apply(to_bool)
    df["size_bytes"] = df["size_bytes"].apply(to_int)
    df["size_gib"] = df["size_gib"].apply(to_float)
    for col in DATE_FIELDS:
        df[col] = df[col].map(to_iso)

    # @timestamp: 기본 crawled_at_utc, 없으면 posted/discovery/now
    def choose_ts(row):
        for c in ["crawled_at_utc","posted_at_utc","discovery_date","estimated_attack_date"]:
            if row.get(c):
                return row[c]
        return datetime.now(timezone.utc).isoformat()

    src_file = os.path.basename(csv_path)
    for _, row in df.iterrows():
        body = {k: (row[k] if k in row else None) for k in UNIFIED_HEADERS}
        body["@timestamp"] = choose_ts(row)
        body["source_file"] = src_file
        # 빈 문자열 → None
        for k, v in list(body.items()):
            if isinstance(v, str) and v.strip()=="":
                body[k] = None
        _id = doc_id(body.get("source",""), body.get("id",""))
        yield {
            "_index": INDEX_NAME,
            "_id": _id,
            "_op_type": "index",
            "_source": body
        }

def main():
    es = Elasticsearch(ES_URL, request_timeout=60)
    es.info()  # 연결 확인
    ensure_index(es)

    csvs = sorted(glob.glob(str(Path(OUTPUT_DIR) / "*_unified.csv")))
    if not csvs:
        print(f"[ingest] No CSV files under: {OUTPUT_DIR}")
        return

    total = 0
    for p in csvs:
        print(f"[ingest] indexing: {p}")
        ok, fail = helpers.bulk(es, gen_actions(p), chunk_size=2000, request_timeout=120)
        total += ok
        print(f"[ingest]   -> ok={ok} fail={fail}")
    print(f"[ingest] done. total indexed: {total}")

if __name__ == "__main__":
    main()

