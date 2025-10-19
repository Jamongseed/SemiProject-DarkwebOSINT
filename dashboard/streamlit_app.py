import os
import time
import requests
import streamlit as st
from elasticsearch import Elasticsearch

# ---- settings ----
OPS_API = os.getenv("OPS_API", "http://localhost:8000")
ES_URL  = os.getenv("ES_URL",  "http://localhost:9200")
INDEX_EVENTS = "ops-events"
INDEX_LOGS   = "ops-logs"

es = Elasticsearch(ES_URL, request_timeout=10)

st.set_page_config(page_title="Ops Control", layout="wide")
st.title("Crawler Control (Account Locks)")

# ---- sidebar: control ----
with st.sidebar:
    st.header("Run crawler")
    crawler = st.selectbox("crawler", ["dragonforce","ransomware_live","coinbase_cartel"])
    account = st.text_input("account", value="demo-user")

    with st.expander("params (optional)"):
        k = st.text_input("key", "")
        v = st.text_input("value", "")
        params = {k: v} if k and v else None

    if st.button("Run now", use_container_width=True):
        try:
            payload = {"crawler": crawler, "account": account, "params": params}
            r = requests.post(f"{OPS_API}/trigger", json=payload, timeout=10)
            if r.ok:
                st.success(f"Triggered: job_id={r.json().get('job_id')}")
            else:
                st.error(f"{r.status_code}: {r.text}")
        except Exception as e:
            st.error(f"trigger error: {e}")

    st.markdown("---")
    st.header("Locks")
    try:
        resp = requests.get(f"{OPS_API}/locks", timeout=5)
        data = resp.json()
        for row in data.get("locks", []):
            st.write(f"🔒 {row['key']} (ttl={row['ttl_sec']})")
        if not data.get("locks"):
            st.caption("No locks.")
    except Exception as e:
        st.warning(f"locks error: {e}")

    st.markdown("---")
    auto = st.toggle("Auto refresh", value=True)
    interval = st.slider("Refresh every (sec)", 2, 30, 5)

# ---- main: events & logs ----
col1, col2 = st.columns(2, gap="large")

with col1:
    st.subheader("Recent jobs (ops-events)")
    try:
        if es.indices.exists(index=INDEX_EVENTS):
            query = {"sort": [{"@timestamp": {"order":"desc"}}], "size": 50, "query": {"match_all": {}}}
            res = es.search(index=INDEX_EVENTS, body=query)
            rows = [
                {
                    "time": h["_source"].get("@timestamp"),
                    "level": h["_source"].get("level"),
                    "phase": h["_source"].get("phase"),
                    "crawler": h["_source"].get("crawler"),
                    "account": h["_source"].get("account"),
                    "message": h["_source"].get("message"),
                    "job_id": h["_source"].get("job_id"),
                }
                for h in res["hits"]["hits"]
            ]
            st.dataframe(rows, use_container_width=True, height=420)
        else:
            st.info(f"Index '{INDEX_EVENTS}' not found. Trigger a job to create it.")
    except Exception as e:
        st.warning(f"events error: {e}")

with col2:
    st.subheader("Live logs (ops-logs)")
    try:
        if es.indices.exists(index=INDEX_LOGS):
            query = {"sort": [{"@timestamp": {"order":"desc"}}], "size": 200, "query": {"match_all": {}}}
            res = es.search(index=INDEX_LOGS, body=query)
            for h in res["hits"]["hits"]:
                s = h["_source"]
                st.text(
                    f"[{s.get('@timestamp')}] "
                    f"[{s.get('crawler')}][{s.get('account')}][{s.get('stream')}] "
                    f"{s.get('message')}"
                )
        else:
            st.info(f"Index '{INDEX_LOGS}' not found. Trigger a job to create it.")
    except Exception as e:
        st.warning(f"logs error: {e}")

# ---- auto refresh ----
if auto:
    time.sleep(interval)
    st.rerun()

