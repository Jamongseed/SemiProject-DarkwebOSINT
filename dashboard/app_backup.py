# app.py — Darkweb Leak Dashboard (CSV Only, FINAL — more-aggressive country inference)
import streamlit as st
import pandas as pd
import numpy as np
import re
import io
import hashlib
import altair as alt
import json
import logging
from urllib.parse import urlparse
from functools import lru_cache

# ---------------- Optional deps ----------------
try:
    import plotly.express as px
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

try:
    import requests
    HAS_REQUESTS = True
except Exception:
    HAS_REQUESTS = False

# ---- Silence Plotly optional import spam (e.g., sage.all trace) ----
optlog = logging.getLogger("_plotly_utils.optional_imports")
optlog.propagate = False
optlog.disabled = True
optlog.setLevel(logging.CRITICAL)

st.set_page_config(page_title="🛡️ Darkweb Leak Dashboard (CSV Only)", layout="wide")
st.title("🛡️ Darkweb Leak Dashboard (CSV Only)")

# ------------------------------------------------
# Compat wrappers (Streamlit width API 차이 흡수)
# ------------------------------------------------
def altair_chart(container, chart):
    try:
        return container.altair_chart(chart, width="stretch")
    except TypeError:
        return container.altair_chart(chart, use_container_width=True)

def plotly_chart(container, fig):
    cfg = {"responsive": True, "displayModeBar": False}  # avoid deprecated width args
    return container.plotly_chart(fig, config=cfg)

def dataframe_(container, df):
    try:
        return container.dataframe(df, width="stretch")
    except TypeError:
        return container.dataframe(df, use_container_width=True)

# ==========================
# 업로드  (세션 캐시로 복원 지원)
# ==========================
uploaded = st.file_uploader(
    "분석할 CSV 파일을 업로드하세요",
    type=["csv"], accept_multiple_files=True
)

@st.cache_data(show_spinner=False)
def _read_csv_from_bytes(name: str, b: bytes) -> pd.DataFrame:
    # 인코딩 추정 읽기
    for enc in ["utf-8", "utf-8-sig", "cp949", "latin-1"]:
        try:
            return pd.read_csv(io.StringIO(b.decode(enc)))
        except Exception:
            continue
    # 최후의 보루
    return pd.read_csv(io.BytesIO(b))

csv_items = []

if uploaded:
    # 새 업로드 → 세션 캐시에 저장
    tmp = []
    for f in uploaded:
        try:
            data = f.getvalue()
        except Exception:
            f.seek(0)
            data = f.read()
        tmp.append({"name": f.name, "bytes": data})
    st.session_state["csv_cache"] = tmp
    csv_items = tmp
elif "csv_cache" in st.session_state:
    # 업로드가 없어도, 이전에 올렸던 CSV가 있으면 복원
    csv_items = st.session_state["csv_cache"]
    st.info("이전에 업로드하신 CSV를 복원했습니다. 필요하시면 위에서 새 파일을 업로드하여 교체하실 수 있습니다.")

if not csv_items:
    st.info("CSV들을 업로드하시면 대시보드가 생성됩니다.")
    st.stop()

# 읽기 + 전처리
dfs = []
for item in csv_items:
    df_ = _read_csv_from_bytes(item["name"], item["bytes"])
    df_.columns = [c.strip().lower().replace(" ", "_") for c in df_.columns]
    name = item["name"].split(".")[0].lower()
    df_["__dataset"] = name
    dfs.append(df_)

df = pd.concat(dfs, ignore_index=True, sort=False)


# ==========================
# Country util: ISO2/aliases → ISO3
# ==========================
ISO2_TO_ISO3 = {
    "US":"USA","GB":"GBR","UK":"GBR","KR":"KOR","RU":"RUS","CN":"CHN","JP":"JPN","DE":"DEU",
    "FR":"FRA","IT":"ITA","ES":"ESP","NL":"NLD","AE":"ARE","SA":"SAU","KSA":"SAU","BR":"BRA",
    "IN":"IND","ID":"IDN","VN":"VNM","CA":"CAN","AU":"AUS","TR":"TUR","MX":"MEX","SE":"SWE",
    "CH":"CHE","PL":"POL","TW":"TWN","HK":"HKG","SG":"SGP","TH":"THA","PH":"PHL","IR":"IRN",
    "IQ":"IRQ","IL":"ISR","UA":"UKR","AR":"ARG","CO":"COL","ZA":"ZAF","DK":"DNK","NO":"NOR",
    "FI":"FIN","BE":"BEL","PT":"PRT","IE":"IRL","RO":"ROU","HU":"HUN","CZ":"CZE","SK":"SVK",
}
ALIASES = {
    "united states":"USA","united states of america":"USA","america":"USA",
    "united kingdom":"GBR","great britain":"GBR","england":"GBR",
    "south korea":"KOR","korea":"KOR","republic of korea":"KOR","uae":"ARE",
    "russia":"RUS","russian federation":"RUS","saudi arabia":"SAU","ksa":"SAU",
    "hong kong":"HKG","hongkong":"HKG"
}
def iso3_from_any(val: str) -> str|None:
    if not val: return None
    v = str(val).strip()
    if not v: return None
    up = v.upper()
    if up in ISO2_TO_ISO3: return ISO2_TO_ISO3[up]
    if len(up) == 3 and up.isalpha(): return up
    return ALIASES.get(v.lower())

# ==========================
# 피해국가 추정 유틸(도메인/URL 기반)
# ==========================
EXCLUDE_DOMAINS = {
    # file hosting / paste
    "mega.nz","anonfiles.com","gofile.io","krakenfiles.com","mediafire.com","transfer.sh","file.io",
    "drive.google.com","docs.google.com","dropbox.com","onedrive.live.com","googleusercontent.com",
    "pastebin.com","ghostbin.com","rentry.co","hastebin.com",
    # social / chat / code
    "t.me","telegram.me","telegram.org","x.com","twitter.com","facebook.com","linkedin.com","github.com","gitlab.com",
    # forums / sources
    "breachforums.st","breachforums.is","breachforums.vc","breachforums.cx","xss.is","raidforums.com",
    "dragonforce.io","dragonforceasia.org","dragonforce.my","dragonforce.pk",
    "ransomware.live","ransomwatch.telemetry.ltd",
}

try:
    import tldextract
    def extract_suffix(host: str) -> str:
        ext = tldextract.extract(host)
        return (ext.suffix or "").lower()
except Exception:
    def extract_suffix(host: str) -> str:
        host = host.lower()
        parts = host.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return ""

def host_from_url(u: str) -> str:
    if not u: return ""
    u = u.strip()
    if not re.match(r"^https?://", u, re.I):
        u = "http://" + u
    try:
        netloc = urlparse(u).netloc
        return netloc.split(":")[0].lower()
    except Exception:
        return ""

def cc_from_suffix(suffix: str) -> str|None:
    if not suffix: return None
    last = suffix.split(".")[-1]
    if len(last) == 2 and last.isalpha():
        return last.upper()
    return None

DOMAIN_RX = re.compile(r"\b((?:[a-z0-9-]+\.)+[a-z]{2,})\b", re.I)
EMAIL_RX  = re.compile(r"\b[a-z0-9._%+-]+@((?:[a-z0-9-]+\.)+[a-z]{2,})\b", re.I)

def harvest_domains_from_text(text: str) -> set[str]:
    if not text: return set()
    s = str(text)
    doms = set(m.group(1).lower() for m in DOMAIN_RX.finditer(s))
    doms |= set(m.group(1).lower() for m in EMAIL_RX.finditer(s))
    return doms

def split_candidates(s: str) -> list[str]:
    if not s or not isinstance(s, str): return []
    s2 = s.strip()
    if s2.startswith("[") and s2.endswith("]"):
        try:
            arr = json.loads(s2)
            return [str(x) for x in arr if isinstance(x, (str, int))]
        except Exception:
            pass
    return re.split(r"[,\s\[\]\{\}\|;]+", s)

# ==========================
# 위키 & 구글 지도(Places 다수결) 온라인 보강 유틸
# ==========================
WIKI_SEARCH = "https://en.wikipedia.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
PLACES_TEXTSEARCH = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GMAPS_GEOCODE     = "https://maps.googleapis.com/maps/api/geocode/json"

@lru_cache(maxsize=4096)
def http_get_json(url, frozen_params_json: str, timeout=8):
    if not HAS_REQUESTS:
        return None
    try:
        params = json.loads(frozen_params_json) if frozen_params_json else None
        r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent":"dwosint-dashboard/1.0"})
        if r.status_code == 200:
            return r.json()
    except Exception:
        return None
    return None

@lru_cache(maxsize=2048)
def wikidata_entity(qid: str):
    data = http_get_json(WIKIDATA_ENTITY.format(qid=qid), None)
    if not data: return None
    return data.get("entities", {}).get(qid)

def first_claim_value_id(entity: dict, prop: str) -> str|None:
    try:
        for snak in entity["claims"][prop]:
            dv = snak["mainsnak"]["datavalue"]
            if dv["type"] == "wikibase-entityid":
                return "Q" + str(dv["value"]["numeric-id"])
    except Exception:
        return None
    return None

def first_claim_value_string(entity: dict, prop: str) -> str|None:
    try:
        for snak in entity["claims"][prop]:
            dv = snak["mainsnak"]["datavalue"]
            if dv["type"] == "string":
                return dv["value"]
    except Exception:
        return None
    return None

@lru_cache(maxsize=2048)
def wikidata_country_iso3_for_company(company_name: str) -> str|None:
    params = {"action":"query","format":"json","list":"search","srlimit":1,"srsearch":company_name}
    sr = http_get_json(WIKI_SEARCH, json.dumps(params, sort_keys=True))
    if not sr or not sr.get("query", {}).get("search"):
        return None
    page_title = sr["query"]["search"][0]["title"]
    params2 = {"action":"query","format":"json","prop":"pageprops","titles":page_title,"ppprop":"wikibase_item"}
    props = http_get_json(WIKI_SEARCH, json.dumps(params2, sort_keys=True))
    if not props or "query" not in props: return None
    pages = props["query"].get("pages", {})
    if not pages: return None
    wikibase_item = None
    for _, page in pages.items():
        wikibase_item = page.get("pageprops", {}).get("wikibase_item")
        if wikibase_item: break
    if not wikibase_item: return None

    ent = wikidata_entity(wikibase_item)
    if not ent: return None

    country_qid = first_claim_value_id(ent, "P17")
    if not country_qid:
        hq_qid = first_claim_value_id(ent, "P159")
        if hq_qid:
            hq_ent = wikidata_entity(hq_qid)
            if hq_ent:
                country_qid = first_claim_value_id(hq_ent, "P17")
    if not country_qid:
        country_qid = first_claim_value_id(ent, "P495")
    if not country_qid:
        return None

    c_ent = wikidata_entity(country_qid)
    if not c_ent:
        return None
    iso3 = first_claim_value_string(c_ent, "P298")
    if iso3:
        return iso3.upper()
    iso2 = first_claim_value_string(c_ent, "P297")
    if iso2:
        return iso3_from_any(iso2)
    return None

def _token_set(s: str) -> set:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))

def _name_sim(q: str, name: str) -> float:
    qs, ns = _token_set(q), _token_set(name)
    if not qs or not ns: return 0.0
    inter = len(qs & ns); union = len(qs | ns)
    return inter / union

@lru_cache(maxsize=4096)
def google_places_votes(query: str, api_key: str, max_candidates: int = 7) -> dict:
    """한 쿼리로 얻은 ISO3별 가중치 dict 반환"""
    votes = {}
    if not (HAS_REQUESTS and api_key and query):
        return votes
    ts_params = {"query": query, "key": api_key, "language": "en"}
    ts_json = http_get_json(PLACES_TEXTSEARCH, json.dumps(ts_params, sort_keys=True))
    if not ts_json or ts_json.get("status") not in {"OK", "ZERO_RESULTS"}:
        return votes
    results = ts_json.get("results", [])[:max_candidates]
    for rank, r in enumerate(results, start=1):
        place_id = r.get("place_id")
        if not place_id: 
            continue
        gc_params = {"place_id": place_id, "key": api_key, "language": "en"}
        gc_json = http_get_json(GMAPS_GEOCODE, json.dumps(gc_params, sort_keys=True))
        if not gc_json or gc_json.get("status") != "OK":
            continue
        iso2 = None
        try:
            comps = gc_json["results"][0]["address_components"]
            for c in comps:
                if "country" in c.get("types", []):
                    iso2 = c.get("short_name")
                    break
        except Exception:
            pass
        if not iso2:
            continue
        w = 0.6 * _name_sim(query, r.get("name", "")) + 0.4 * (1.0 / rank)
        types = set(r.get("types", []))
        if types & {"local_government_office", "city_hall", "police", "embassy", "courthouse"}:
            w += 0.1
        iso3 = iso3_from_any(iso2)
        if not iso3:
            continue
        votes[iso3] = votes.get(iso3, 0.0) + w
    return votes

@lru_cache(maxsize=4096)
def google_country_iso3_by_places_multi(company: str, api_key: str) -> str|None:
    """회사명으로 여러 변형 쿼리를 던져 가중투표."""
    if not company:
        return None
    variants = [
        company,
        f"{company} headquarters",
        f"{company} company",
        f"{company} government",
        f"{company} official",
    ]
    total = {}
    for q in variants:
        v = google_places_votes(q, api_key, max_candidates=7)
        for k, w in v.items():
            total[k] = total.get(k, 0.0) + w
    if not total:
        return None
    return max(total.items(), key=lambda kv: kv[1])[0]

# ==========================
# 전처리 & 특징 추출
# ==========================
def build_text(row):
    parts = []
    for c in ["record_type","description","content","title","hashtags","notes","source",
              "actor","ransomware_group","company","details_url","website","attachment_urls"]:
        if c in row and pd.notna(row[c]):
            parts.append(str(row[c]))
    return " | ".join(parts).lower()

def parse_any_dt(row):
    for c in ["posted_at_utc","post_date_utc","post_datetime_utc","published_at_utc",
              "crawled_at_utc","crawled_at_kst","discovery_date","reported_at_utc"]:
        if c in row and pd.notna(row[c]):
            v = pd.to_datetime(row[c], utc=True, errors="coerce")
            if pd.notna(v):
                return v
    return pd.NaT

def get_size_gib(row):
    if "size_gib" in row and pd.notna(row["size_gib"]):
        try:
            return float(row["size_gib"])
        except Exception:
            pass
    if "size_bytes" in row and pd.notna(row["size_bytes"]):
        try:
            return float(row["size_bytes"]) / (1024**3)
        except Exception:
            pass
    return np.nan

def norm_company(x):
    if pd.isna(x):
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(x).lower())

df["__text"] = df.apply(build_text, axis=1)
df["__ts"] = df.apply(parse_any_dt, axis=1)           # tz-aware(UTC)
df["__size_gib"] = df.apply(get_size_gib, axis=1)
df["__company_norm"] = df["company"].map(norm_company) if "company" in df.columns else ""

# ---------------------------
# 오프라인 Heuristic: 회사 법인표기/패턴 기반 투표
# ---------------------------
HEUR_PATTERNS = [
    (r"\bS\.p\.A\.?\b", "ITA", 1.0),
    (r"\bs\.r\.l\.?\b", "ITA", 0.8),
    (r"\bGmbH\b", "DEU", 1.0),
    (r"\bAG\b", "CHE", 0.6),     # (DEU/CH 공통, CH에 소폭 가중)
    (r"\bOy\b", "FIN", 1.0),
    (r"\bAB\b", "SWE", 0.8),
    (r"\bSp\. z o\.o\.\b", "POL", 1.0),
    (r"\bs\.r\.o\.\b", "CZE", 0.9),
    (r"\bd\.o\.o\.\b", "SVN", 0.7),  # (HRV/SRB/BIH/ SVN 공통 → SVN 쪽 가중)
    (r"\bK\.K\.?\b", "JPN", 1.0),
    (r"\bS\.A\. de C\.V\.\b", "MEX", 1.0),
    (r"\bA/S\b", "DNK", 1.0),
    (r"\bAS\b", "NOR", 0.6),
    (r"\bPvt\.?\s+Ltd\.?\b", "IND", 1.0),
    (r"\bLtd\.?\b", "GBR", 0.6),
    (r"\bLLC\b", "USA", 0.5),
    (r"\bInc\.?\b", "USA", 0.5),
    (r"\bSAS\b", "FRA", 0.6),  # (COL 등도 있으나 우선 FRA)
    (r"\bS\.A\.?\b", "ESP", 0.4),  # (남미 다수 국가 공통, 스페인 소가중)
]

def heuristic_country_iso3(company: str, text: str, hosts: set[str]) -> str|None:
    votes = {}

    # 1) 회사명/본문에 법인표기 패턴
    blob = " ".join([company or "", text or ""])
    for pat, iso3, w in HEUR_PATTERNS:
        if re.search(pat, blob, re.IGNORECASE):
            votes[iso3] = votes.get(iso3, 0.0) + w

    # 2) 이메일/도메인에서 2단계 ccSLD 힌트(.com.au, .co.kr 등) → 마지막 cc는 이미 처리되지만 보강
    SLD_TO_ISO2 = {
        "com.au":"AU","net.au":"AU","org.au":"AU",
        "co.kr":"KR","or.kr":"KR","go.kr":"KR","ac.kr":"KR",
        "com.br":"BR","com.tr":"TR","com.mx":"MX","com.ar":"AR","com.pe":"PE","com.co":"CO",
        "co.jp":"JP","ne.jp":"JP","or.jp":"JP","ac.jp":"JP","go.jp":"JP",
        "co.uk":"GB","ac.uk":"GB","gov.uk":"GB","org.uk":"GB","ltd.uk":"GB",
        "com.sg":"SG","com.my":"MY","com.hk":"HK","com.tw":"TW","com.sa":"SA","com.pl":"PL",
        "com.ru":"RU","com.ua":"UA","com.pt":"PT","com.ro":"RO","com.cz":"CZ",
        "com.vn":"VN","com.ph":"PH","com.id":"ID",
        "co.za":"ZA","com.ng":"NG","com.eg":"EG",
    }
    for h in hosts:
        suf = extract_suffix(h)  # e.g., "co.kr"
        if suf in SLD_TO_ISO2:
            iso3 = iso3_from_any(SLD_TO_ISO2[suf])
            votes[iso3] = votes.get(iso3, 0.0) + 0.75

    if votes:
        return max(votes.items(), key=lambda kv: kv[1])[0]
    return None

# 피해국가(1차): URL/도메인 → 컬럼 → 오프라인 heuristic
def infer_victim_country_iso3_offline(row: dict) -> str|None:
    # 1) URL/도메인 기반 후보 수집
    cand = set()
    for c in ["details_url","website","attachment_urls","extracted_domains"]:
        if c in row and pd.notna(row[c]):
            cand.update(split_candidates(str(row[c])))
    comp = str(row.get("company","") or "")
    if "." in comp and " " not in comp:
        cand.add(comp)
    text = row.get("__text","") or ""
    cand |= harvest_domains_from_text(text)

    hosts = set()
    for token in cand:
        h = host_from_url(token) if "://" in token or "/" in token else token.lower()
        if not h or "." not in h:
            continue
        if h.endswith(".onion") or h in EXCLUDE_DOMAINS:
            continue
        hosts.add(h)

    # 2) ccTLD 투표
    votes = []
    for h in hosts:
        suf = extract_suffix(h)
        cc = cc_from_suffix(suf)
        if cc:
            votes.append(cc)
    if votes:
        from collections import Counter
        cc = Counter(votes).most_common(1)[0][0]
        return iso3_from_any(cc)

    # 3) 컬럼 country/codes
    for key in ["victim_country_iso3","victim_country","country","country_code"]:
        if key in row and pd.notna(row[key]):
            iso3 = iso3_from_any(str(row[key]))
            if iso3:
                return iso3

    # 4) 오프라인 heuristic
    iso3_h = heuristic_country_iso3(comp, text, hosts)
    if iso3_h:
        return iso3_h

    return None

# 최초 오프라인 추정
df["victim_country_iso3"] = [infer_victim_country_iso3_offline(rec) for rec in df.to_dict(orient="records")]

# ==========================
# 스코어링 컴포넌트
# ==========================
SENS_PATS = [
    ("wallet_keys", r"\b(private key|seed phrase|mnemonic|wallet\.dat|api key|jwt|ssh key)\b", 50),
    ("pii",         r"\b(personal data|pii|ssn|passport|national id|주민등록|여권|운전면허)\b", 40),
    ("financial",   r"\b(credit card|iban|bank|송금|계좌|financial)\b", 40),
    ("credentials", r"\b(credentials?|passwords?|hash(?:es)?|combo(?:list)?|stealer logs?|cookies?)\b", 35),
    ("db_dump",     r"\b(database|db dump|sql dump|backup|mongodb|postgres|mysql)\b", 35),
    ("source_code", r"\b(source code|git leak|repository)\b", 25),
    ("access_infra",r"\b(vpn|rdp|citrix|okta|admin panel|zimbra|o365)\b", 25),
    ("lists",       r"\b(email lists?|phone lists?|dox|fullz)\b", 20),
]
RANSOM = {"lockbit","blackcat","alphv","play","cl0p","medusa","black basta","akira",
          "8base","bianlian","cactus","ragroup","cuba","royal","conti","ransomh0use","anubis"}
HACKT = {"dragonforce","dragonforce malaysia","killnet","anonymous","thunderspy"}
RESELL= {"coinbase cartel","xss","breachforums","raid"}

def sens_score(t):
    best = 0
    for _, pat, w in SENS_PATS:
        if re.search(pat, t):
            best = max(best, w)
    return best

def vol_score(g):
    if pd.isna(g): return 0
    if g >= 100: return 30
    if g >= 10:  return 22
    if g >= 1:   return 15
    if g >= 0.1: return 8
    return 4

def exp_stage(t, row):
    isp = False
    if "is_published" in row and pd.notna(row["is_published"]):
        isp = str(row["is_published"]).strip().lower() in {"1","true","yes"}
    if isp or re.search(r"\b(leaked|published|dumped|released)\b", t): return "published"
    if re.search(r"\b(for sale|selling|price|btc|xmr|monero|bitcoin)\b", t): return "for_sale"
    if re.search(r"\b(countdown|leak in|time until)\b", t) or ("time_until_publication" in row and pd.notna(row["time_until_publication"])): 
        return "announced"
    return "listed"

def exp_score(stage): return {"published":30, "for_sale":20, "announced":15, "listed":10}.get(stage, 10)

def actor_score(t):
    s = 0
    if any(a in t for a in RANSOM): s = max(s, 20)
    if any(a in t for a in HACKT):  s = max(s, 12)
    if any(a in t for a in RESELL): s = max(s, 8)
    return s

def rec_score(ts):
    if pd.isna(ts): return 5
    now = pd.Timestamp.now(tz="UTC")
    ts = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(ts): return 5
    days = (now - ts).days
    if days <= 7:  return 15
    if days <= 30: return 10
    if days <= 90: return 7
    return 5

def evid_score(row, t):
    fields = ["attachment_urls","media","files_api_present","extracted_emails","extracted_domains"]
    ok = any((c in row and pd.notna(row[c]) and str(row[c]).strip() not in {"[]","{}",""}) for c in fields)
    if ok: return 10
    return 8 if re.search(r"\b(screenshot|sample|proof|poc)\b", t) else 0

def mentions_score(t):
    s = 0
    if re.search(r"\b(admin|root|privileged|domain admin|global admin)\b", t): s += 10
    if re.search(r"\b(okta|adfs|azure ad|o365|exchange)\b", t):              s += 5
    if re.search(r"\b(ransom|btc|xmr|monero|bitcoin|demand)\b", t):          s += 8
    return min(s, 23)

# 교차게시 키
dup = df.groupby(df["__company_norm"])["__dataset"].nunique()
cross_keys = set(dup[dup > 1].index)
def cross_score(norm): return 10 if norm and norm in cross_keys else 0

# raw(가중치 계산용)
raw = pd.DataFrame({
    "__dataset": df["__dataset"],
    "company": df["company"] if "company" in df.columns else "",
    "country": df["country"] if "country" in df.columns else "",
    "victim_country_iso3": df["victim_country_iso3"],
    "__ts": df["__ts"],
    "__size_gib": df["__size_gib"],
})
raw["sensitivity"] = df["__text"].apply(sens_score)
raw["volume"]      = df["__size_gib"].apply(vol_score)
raw["exposure_stage"] = [exp_stage(t, r) for t, r in zip(df["__text"], df.to_dict(orient="records"))]
raw["exposure"]    = raw["exposure_stage"].apply(exp_score)
raw["actor"]       = df["__text"].apply(actor_score)
raw["recency"]     = df["__ts"].apply(rec_score)
raw["evidence"]    = [evid_score(r, t) for r, t in zip(df.to_dict(orient="records"), df["__text"])]
raw["mentions"]    = df["__text"].apply(mentions_score)
raw["cross"]       = df["__company_norm"].apply(cross_score)

# ==========================
# 가중치 (읽기 전용 표시; 설정은 별도 페이지)
# ==========================
st.sidebar.header("⚖️ 현재 가중치 (읽기 전용)")

DEFAULT_WEIGHTS = {"sensitivity":25,"volume":10,"actor":15,"exposure":20,"recency":10,"evidence":10,"mentions":5,"cross":5}

def _normalize(ws: dict) -> dict:
    t = sum(ws.values())
    if t <= 0:
        ws = DEFAULT_WEIGHTS.copy()
        t = sum(ws.values())
    return {k: (v / t) * 100 for k, v in ws.items()}

# 설정 페이지에서 저장한 세션값을 읽음(없으면 기본값)
weights_raw = st.session_state.get("weights_raw", DEFAULT_WEIGHTS.copy())
norm_w = st.session_state.get("weights_norm", _normalize(weights_raw))

st.sidebar.caption(f"합계(원시 슬라이더): {sum(weights_raw.values())} → 내부 환산 100 기준")

# 가중치 도넛(읽기 전용)
w_df = pd.DataFrame({"component": list(norm_w.keys()), "weight": list(norm_w.values())})
pie = alt.Chart(w_df).mark_arc(innerRadius=60).encode(
    theta="weight:Q", color="component:N", tooltip=["component","weight"]
)
altair_chart(st.sidebar, pie)

# 설정 페이지로 바로 가는 링크(있으면)
try:
    st.sidebar.page_link("pages/01_Weights_Settings.py", label="가중치 설정 페이지 열기")
    st.sidebar.page_link("pages/02_Weights_Guide.py",   label="가중치 유스케이스 가이드")
except Exception:
    st.sidebar.info("좌측 Pages 메뉴에서 가중치 관련 페이지로 이동하세요.")


# ==========================
# 점수 계산 & 등급
# ==========================
def to_norm(val, maxv): return max(0.0, min(1.0, float(val)/maxv if maxv>0 else 0.0))
NMAX = {"sensitivity":50,"volume":30,"actor":20,"exposure":30,"recency":15,"evidence":10,"mentions":23,"cross":10}
for k in NMAX: raw[f"n_{k}"] = raw[k].apply(lambda v: to_norm(v, NMAX[k]))
raw["severity_score"] = sum(raw[f"n_{k}"] * norm_w[k] for k in NMAX)
def sev_level(s): return "CRITICAL" if s>=80 else "HIGH" if s>=60 else "MEDIUM" if s>=40 else "LOW" if s>=20 else "INFO"
raw["severity_level"] = raw["severity_score"].apply(sev_level)

# ==========================
# 필터 (날짜 포함)
# ==========================
st.sidebar.header("🔎 필터")
srcs = sorted(raw["__dataset"].dropna().unique().tolist())
pick_src = st.sidebar.multiselect("Source", srcs, default=srcs)

include_na_ts = st.sidebar.checkbox("날짜 없는 항목 포함", True)
if raw["__ts"].notna().any():
    min_dt = raw["__ts"].min().date()
    max_dt = raw["__ts"].max().date()
    st_date = st.sidebar.date_input("시작일", value=min_dt)
    en_date = st.sidebar.date_input("종료일", value=max_dt)
else:
    st_date = en_date = None

# ==========================
# 🌐 회사 → 국가 온라인 보강 (사이드바 하단, URL/오프라인 실패분만 대상)
# ==========================
st.sidebar.header("🌐 회사 → 국가 온라인 보강")
enrich_method = st.sidebar.radio("보강 방식", ["Google Maps (추천)", "Wikipedia/Wikidata"], index=0)
enable_online = st.sidebar.checkbox("보강 사용", value=True)
max_lookups = st.sidebar.slider("최대 조회 수(고유 회사)", 0, 400, 120, 10)

gmaps_api_key = ""
if enrich_method.startswith("Google"):
    gmaps_api_key = st.sidebar.text_input("Google Maps API Key", type="password",
                                          help="Places Text Search + 역지오코딩 다수결로 국가 추정")

if enable_online and "company" in df.columns:
    missing_mask = df["victim_country_iso3"].isna() & df["company"].notna()
    missing_companies = df.loc[missing_mask, "company"].dropna().unique().tolist()

    seen = set(); targets = []
    for c in missing_companies:
        key = norm_company(c)
        if key and key not in seen:
            seen.add(key); targets.append(c)
        if len(targets) >= max_lookups: break

    enrich_map = {}
    if enrich_method.startswith("Google") and HAS_REQUESTS and gmaps_api_key:
        for comp in targets:
            try:
                iso3 = google_country_iso3_by_places_multi(comp, gmaps_api_key)
            except Exception:
                iso3 = None
            if not iso3:  # 구글 실패 시 위키 보조
                try:
                    iso3 = wikidata_country_iso3_for_company(comp)
                except Exception:
                    iso3 = None
            if not iso3:  # 완전 실패 시 Heuristic 재시도(회사명만)
                iso3 = heuristic_country_iso3(comp, comp, set())
            if iso3:
                enrich_map[comp] = iso3
    elif enrich_method.endswith("Wikidata") and HAS_REQUESTS:
        for comp in targets:
            try:
                iso3 = wikidata_country_iso3_for_company(comp)
            except Exception:
                iso3 = None
            if not iso3:
                iso3 = heuristic_country_iso3(comp, comp, set())
            if iso3:
                enrich_map[comp] = iso3

    if enrich_map:
        mask = df["victim_country_iso3"].isna() & df["company"].isin(enrich_map.keys())
        df.loc[mask, "victim_country_iso3"] = df.loc[mask, "company"].map(enrich_map)
        raw.loc[mask, "victim_country_iso3"] = df.loc[mask, "victim_country_iso3"]

# === 필터 적용 ===
v = raw.copy()
if pick_src:
    v = v[v["__dataset"].isin(pick_src)]
if v["__ts"].notna().any() and st_date and en_date:
    mask = v["__ts"].notna() & (v["__ts"].dt.date >= st_date) & (v["__ts"].dt.date <= en_date)
    if include_na_ts:
        mask = mask | v["__ts"].isna()
    v = v[mask]

# ==========================
# 커버리지 KPI
# ==========================
assigned_rate = (v["victim_country_iso3"].notna().mean() * 100) if len(v) else 0.0
st.caption(f"🌍 Victim country coverage: {assigned_rate:.1f}% (rows with ISO3)")

# ==========================
# 1) 소스별 심각도 분포 (가로형 누적 막대)
# ==========================
st.subheader("소스별 심각도 분포")
order_levels = ["INFO","LOW","MEDIUM","HIGH","CRITICAL"]
by_src = (v.assign(sev=v["severity_level"].astype("category").cat.set_categories(order_levels, ordered=True))
            .groupby(["__dataset","sev"], observed=False)
            .size()
            .reset_index(name="count"))
chart1 = (alt.Chart(by_src)
            .mark_bar()
            .encode(
                y=alt.Y("__dataset:N", title="Source", sort='-x', axis=alt.Axis(labelAngle=0)),
                x=alt.X("count:Q", title="Count", stack="zero"),
                color=alt.Color("sev:N", title="Severity", scale=alt.Scale(domain=order_levels)),
                tooltip=["__dataset","sev","count"]
            ).properties(height=320))
altair_chart(st, chart1)

# ==========================
# 2) 점수대 분포 (가로형 누적 막대)
# ==========================
st.subheader("심각도 점수대 분포 (누적)")
bins = pd.IntervalIndex.from_tuples([(0,19.99),(20,39.99),(40,59.99),(60,79.99),(80,100)], closed="both")
labels = ["0–19","20–39","40–59","60–79","80–100"]
v["score_bin"] = pd.cut(v["severity_score"], bins=bins, labels=labels, include_lowest=True)
by_bin = (v.groupby(["__dataset","score_bin"], observed=False)
            .size()
            .reset_index(name="count"))
chart2 = (alt.Chart(by_bin)
            .mark_bar()
            .encode(
                y=alt.Y("__dataset:N", title="Source", sort='-x', axis=alt.Axis(labelAngle=0)),
                x=alt.X("count:Q", title="Count", stack="zero"),
                color=alt.Color("score_bin:N", title="Score Bucket", sort=labels),
                tooltip=["__dataset","score_bin","count"]
            ).properties(height=320))
altair_chart(st, chart2)

# ==========================
# 3) 세계 지도 — 피해국가 기준(Choropleth)
# ==========================
# ==========================
# 피해 국가 현황 — 지도
# ==========================
st.subheader("피해 국가 현황")
if HAS_PLOTLY and v["victim_country_iso3"].notna().any():
    # 1) 국가별 집계
    geo = (v.dropna(subset=["victim_country_iso3"])
             .groupby("victim_country_iso3")
             .size()
             .reset_index(name="count")
             .sort_values("count", ascending=False))

    # 2) ISO3 → 한국어 국가명 매핑
    ISO3_TO_KO = {
        "KOR":"대한민국","USA":"미국","GBR":"영국","FRA":"프랑스","DEU":"독일","JPN":"일본","CHN":"중국",
        "RUS":"러시아","CAN":"캐나다","AUS":"호주","ITA":"이탈리아","ESP":"스페인","NLD":"네덜란드",
        "SWE":"스웨덴","NOR":"노르웨이","DNK":"덴마크","FIN":"핀란드","CHE":"스위스","POL":"폴란드",
        "PRT":"포르투갈","IRL":"아일랜드","BEL":"벨기에","CZE":"체코","SVK":"슬로바키아","HUN":"헝가리",
        "ROU":"루마니아","AUT":"오스트리아","GRC":"그리스","TUR":"튀르키예","UKR":"우크라이나",
        "ISR":"이스라엘","SAU":"사우디아라비아","ARE":"아랍에미리트","IND":"인도","IDN":"인도네시아",
        "VNM":"베트남","THA":"태국","PHL":"필리핀","SGP":"싱가포르","MYS":"말레이시아","HKG":"홍콩",
        "TWN":"대만","MEX":"멕시코","ARG":"아르헨티나","BRA":"브라질","COL":"콜롬비아","CHL":"칠레",
        "PER":"페루","ZAF":"남아프리카공화국","EGY":"이집트","NGA":"나이지리아",
        # 필요 시 계속 추가하세요.
    }
    geo["Country"] = geo["victim_country_iso3"].map(ISO3_TO_KO).fillna(geo["victim_country_iso3"])

    # 3) 지도 생성 (타이틀/라벨 변경)
    fig_choro = px.choropleth(
        geo,
        locations="victim_country_iso3",
        locationmode="ISO-3",
        color="count",
        color_continuous_scale="Reds",
        title="유출 대상 회사의 위치 기준",
        labels={"count":"유출 건수"}  # 컬러바 라벨
    )

    # 4) Hover 템플릿 커스터마이즈: Country와 건수만 노출
    import numpy as np
    fig_choro.update_traces(
        customdata=np.stack([geo["Country"]], axis=-1),
        hovertemplate="Country=%{customdata[0]}<br>건수=%{z}<extra></extra>"
    )

    plotly_chart(st, fig_choro)
else:
    if not HAS_PLOTLY:
        st.info("Plotly가 설치되어 있지 않아 지도를 표시하지 못했습니다. `pip install plotly`를 설치해 주십시오.")
    else:
        st.info("피해 국가를 추정할 데이터가 부족합니다. (URL/도메인/회사명 보강 옵션을 확인해 주십시오.)")

# ==========================
# 상세 & 내보내기
# ==========================
st.subheader("상세")
show_cols = ["__dataset","company","country","victim_country_iso3","__ts","__size_gib",
             "exposure_stage","severity_level","severity_score"]
show_cols = [c for c in show_cols if c in v.columns]
dataframe_(st, v.sort_values("severity_score", ascending=False)[show_cols])

st.download_button(
    "💾 현재 뷰 CSV 다운로드",
    v.to_csv(index=False).encode("utf-8"),
    "dashboard_export.csv",
    "text/csv"
)

