import requests
import platform
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import csv
from pathlib import Path
import re

# DragonForce (Ubuntu 기본 Tor 포트 9050)
URL = "http://z3wqggtxft7id3ibr7srivv5gjof5fwg76slewnzwwakjuf3nlhukdid.onion"
PORT = "9150" if platform.system() == "Windows" else "9050"
PROXIES = {
    "http": f"socks5h://127.0.0.1:{PORT}",
    "https": f"socks5h://127.0.0.1:{PORT}"
}

# --- 통합 스키마 헤더 ---
UNIFIED_HEADERS = [
    "source", "record_type", "id", "company", "website", "country", "address",
    "size_bytes", "size_gib", "is_published", "time_until_publication",
    "posted_at_utc", "crawled_at_utc", "crawled_at_kst",
    "ransomware_group", "discovery_date", "estimated_attack_date",
    "details_url", "description", "files_api_present"
]

def get_tor_session() -> requests.Session:
    s = requests.session()
    s.proxies = PROXIES
    return s

def fetch_page_data(session: requests.Session, page: int, base_url: str) -> dict | None:
    api_url = f"{base_url}/api/guest/blog/posts?page={page}"
    print(f"⏳ Page {page} 데이터 요청 시도 (URL: {api_url})")
    try:
        response = session.get(api_url, timeout=60)
        response.raise_for_status()
        print(f"Page {page}: 데이터 로드 성공")
        return response.json()
    except Exception as e:
        print(f"Page {page}: 데이터 로드 실패 - {e}")
        return None

def format_bytes(size_bytes: int) -> str:
    if size_bytes == 0: return "0 B"
    power = 1024
    n = 0
    labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size_bytes >= power and n < len(labels) - 1:
        size_bytes /= power
        n += 1
    return f"{size_bytes:.2f} {labels[n]}"

def parse_iso8601(s: str) -> datetime | None:
    """
    느슨한 ISO8601 파서:
    - Z → +00:00 치환
    - 타임존 없으면 +00:00 가정
    - 소수점(초) 1~6 자리로 정규화
    """
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if not re.search(r"[+\-]\d{2}:\d{2}$", s):
        s += "+00:00"
    m = re.match(r"^(.*T\d{2}:\d{2}:\d{2})(\.(\d+))?([+\-]\d{2}:\d{2})$", s)
    if m:
        pre = m.group(1)
        frac = m.group(3) or ""
        tz = m.group(4)
        if frac:
            if len(frac) > 6:
                frac = frac[:6]
            else:
                frac = frac.ljust(6, "0")
            s = f"{pre}.{frac}{tz}"
        else:
            s = f"{pre}{tz}"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def parse_victim_data(publication: dict, page: int) -> dict:
    """(콘솔 샘플 출력용)"""
    now_utc = datetime.now(timezone.utc)
    now_kst = now_utc.astimezone(ZoneInfo("Asia/Seoul"))

    created_at_str = publication.get('created_at', '')
    posted_dt = parse_iso8601(created_at_str)

    timer_str = publication.get('timer_publication', '')
    is_published = False
    time_until_publication = ""

    pub_dt = parse_iso8601(timer_str)
    if pub_dt:
        if now_utc >= pub_dt:
            is_published = True
            time_until_publication = ""
        else:
            time_left = pub_dt - now_utc
            d = time_left.days
            h, rem = divmod(time_left.seconds, 3600)
            m, _ = divmod(rem, 60)
            time_until_publication = f"{d}d {h}h {m}m left"

    details_url = f"{URL}/api/guest/blog/post?post_uuid={publication.get('uuid', '')}"
    publication_url = f"{URL}/api/guest/blog/post/files?post_uuid={publication.get('uuid', '')}"

    return {
        "Current Time (KST)": now_kst.strftime('%Y-%m-%d %H:%M:%S'),
        "Current Time (UTC)": now_utc.strftime('%Y-%m-%d %H:%M:%S'),
        "Page Number": page,
        "Company Name": (publication.get('name') or '').strip(),
        "Website": (publication.get('website') or '').strip(),
        "Address": (publication.get('address') or '').strip(),
        "Data Size": format_bytes(publication.get('weight', 0) or 0),
        "Description": (publication.get('description') or '').strip(),
        "Is Published": 'Yes' if is_published else 'No',
        "Time Until Publication": time_until_publication,
        "Publication URL": publication_url if is_published else "",
        "Posted Time (UTC)": posted_dt.strftime('%Y-%m-%d %H:%M:%S') if posted_dt else '',
        "Details URL": details_url
    }

def print_victim_details(victim_dict: dict):
    print("--------------------------------------------------")
    for key, value in victim_dict.items():
        if key == "Description" and len(str(value)) > 100:
            value = f"{str(value)[:100]}..."
        print(f"  [+] {key}: {value}")
    print("--------------------------------------------------\n")

def _to_gib(n):
    try:
        return f"{(float(n)/(1024**3)):.2f}" if n not in (None, "", 0) else ""
    except Exception:
        return ""

def to_unified_row(publication: dict, now_utc: datetime, now_kst: datetime) -> dict:
    """DragonForce 게시물 -> 통합 스키마 1행"""
    created_at_str = publication.get('created_at', '')
    posted_dt = parse_iso8601(created_at_str)
    posted_at_utc_iso = posted_dt.isoformat() if posted_dt else ""

    # 공개 여부/남은 시간 (결측은 빈칸)
    is_published_bool = False
    time_until_publication = ""
    pub_dt = parse_iso8601(publication.get('timer_publication', ''))
    if pub_dt:
        if now_utc >= pub_dt:
            is_published_bool = True
            time_until_publication = ""
        else:
            left = pub_dt - now_utc
            d = left.days
            h, r = divmod(left.seconds, 3600)
            m, _ = divmod(r, 60)
            time_until_publication = f"{d}d {h}h {m}m left"

    uuid = publication.get("uuid", "") or ""
    details_url = f"{URL}/api/guest/blog/post?post_uuid={uuid}"
    weight = publication.get('weight')
    size_bytes = weight if isinstance(weight, int) else ""

    # files_api_present: 공개 시 true, 미공개면 false
    files_api_present = "true" if is_published_bool else "false"

    return {
        "source": "dragonforce",
        "record_type": "leak_post",
        "id": uuid,
        "company": (publication.get('name') or "").strip(),
        "website": (publication.get('website') or "").strip(),
        "country": "",
        "address": (publication.get('address') or "").strip(),
        "size_bytes": size_bytes,
        "size_gib": _to_gib(weight),
        "is_published": "true" if is_published_bool else "false",
        "time_until_publication": time_until_publication,
        "posted_at_utc": posted_at_utc_iso,
        "crawled_at_utc": now_utc.isoformat(),
        "crawled_at_kst": now_kst.isoformat(),
        "ransomware_group": "",
        "discovery_date": "",
        "estimated_attack_date": "",
        "details_url": details_url,
        "description": (publication.get('description') or "").strip(),
        "files_api_present": files_api_present
    }

def save_unified_csv_dragonforce(rows: list[dict],
                                 out_dir: str = "outputs",
                                 filename: str = "dragonforce_unified.csv"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / filename
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNIFIED_HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f" - 통합(덮어쓰기): {path.resolve()}")

def main():
    print("### TimeZone 에러 발생시 pip install tzdata 실행 (Ubuntu 일반적으로 기본 제공)")
    all_victims = []
    unified_rows = []

    session = get_tor_session()
    initial_data = fetch_page_data(session=session, page=1, base_url=URL)
    if not initial_data:
        print("### 프로그램을 종료합니다. 첫 페이지를 가져올 수 없습니다.")
        return

    total_pages = initial_data.get('data', {}).get('pages', 1)
    print(f"총 {total_pages}개의 페이지를 발견했습니다. 순차적으로 크롤링을 시작합니다.\n")

    for page_num in range(1, total_pages + 1):
        page_data = initial_data if page_num == 1 else fetch_page_data(session=session, page=page_num, base_url=URL)
        if not page_data:
            print(f"페이지 {page_num} 처리를 건너뜁니다.")
            continue

        publications = page_data.get('data', {}).get('publications', [])
        for item in publications:
            if not item.get('is_transfering', True):
                parsed_info = parse_victim_data(item, page_num)  # 콘솔 확인용
                all_victims.append(parsed_info)

                now_utc = datetime.now(timezone.utc)
                now_kst = now_utc.astimezone(ZoneInfo("Asia/Seoul"))
                unified_rows.append(to_unified_row(item, now_utc, now_kst))

    print(f"### 데이터 처리 완료! 총 **{len(all_victims)}** 개의 피해 기업 정보를 리스트에 저장했습니다.")
    print("### 수집된 데이터 샘플 (최신 5개)")
    for victim_data in all_victims[:5]:
        print_victim_details(victim_data)

    save_unified_csv_dragonforce(unified_rows, out_dir="outputs", filename="dragonforce_unified.csv")

if __name__ == "__main__":
    main()

