# crawler_coinbase_cartel.py
import platform
import requests
from pprint import pprint
from bs4 import BeautifulSoup
from typing import List, Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urljoin
from pathlib import Path
import csv

# Tor 프록시 & 타깃 URL (Ubuntu 9050 기본)
PORT = "9150" if platform.system() == "Windows" else "9050"
PROXIES = {
    "http": f"socks5h://127.0.0.1:{PORT}",
    "https": f"socks5h://127.0.0.1:{PORT}",
}
BASE_URL = "http://fjg4zi4opkxkvdz7mvwp7h6goe4tcby3hhkrz43pht4j3vakhy75znyd.onion"

# --- 통합 스키마 헤더 ---
UNIFIED_HEADERS = [
    "source", "record_type", "id", "company", "website", "country", "address",
    "size_bytes", "size_gib", "is_published", "time_until_publication",
    "posted_at_utc", "crawled_at_utc", "crawled_at_kst",
    "ransomware_group", "discovery_date", "estimated_attack_date",
    "details_url", "description", "files_api_present"
]

def get_tor_response(url: str, timeout: int = 30) -> Optional[requests.Response]:
    print(f"Tor 프록시(포트: {PORT})로 접속 시도 → {url}")
    try:
        res = requests.get(url, proxies=PROXIES, timeout=timeout)
        res.raise_for_status()
        print("--- 접속 성공 ---")
        return res
    except Exception as e:
        print(f"Error: {e}")
        return None

class CC_Victim:
    def __init__(self, name: str, industry: str = None, revenue: str = None,
                 website: str = None, details_link: str = None):
        self.name = name
        self.industry = industry
        self.revenue = revenue
        self.website = website
        self.details_link = details_link

    def to_dict(self):
        return {
            "name": self.name,
            "industry": self.industry or "",
            "revenue": self.revenue or "",
            "website": self.website or "",
            "details_link": self.details_link or "",
        }

    def __repr__(self):
        return f"[Name: {self.name}, industry: {self.industry}, revenue: {self.revenue}, website: {self.website}, details_link: {self.details_link}]"

def parse_victims_from_html(html_text: str) -> List[CC_Victim]:
    soup = BeautifulSoup(html_text, "html.parser")
    articles = soup.select("div.companies-grid > article")
    victims: List[CC_Victim] = []

    for article in articles:
        try:
            name_tag = article.select_one("h3.card-name")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            industry = None
            revenue = None
            meta_tag = article.select_one("div.card-meta")
            if meta_tag:
                for span in meta_tag.select("span"):
                    span_text = span.get_text(" ", strip=True)
                    if "Industry" in span_text:
                        industry = span_text.replace("Industry:", "").strip()
                    elif "Revenue" in span_text:
                        revenue = span_text.replace("Revenue:", "").strip()

            website = None
            if meta_tag:
                a_tag = meta_tag.select_one("a")
                if a_tag and a_tag.get("href"):
                    website = a_tag["href"].strip()

            details_link = None
            details_link_tag = article.select_one("a.view-detail")
            if details_link_tag and details_link_tag.get("href"):
                details_link = urljoin(BASE_URL, details_link_tag["href"].strip())

            victims.append(CC_Victim(
                name=name, industry=industry, revenue=revenue,
                website=website, details_link=details_link
            ))
        except Exception as e:
            print(f"개별 article 파싱 중 오류: {e}")
            continue
    return victims

def to_unified_row(v: CC_Victim, crawled_at_utc: str, crawled_at_kst: str) -> dict:
    vdict = v.to_dict()
    description_parts = []
    if vdict["industry"]:
        description_parts.append(f"Industry: {vdict['industry']}")
    if vdict["revenue"]:
        description_parts.append(f"Revenue: {vdict['revenue']}")
    description = "; ".join(description_parts)

    _id = vdict["details_link"] or vdict["website"] or vdict["name"]

    return {
        "source": "coinbase_cartel",
        "record_type": "victim",
        "id": _id,
        "company": vdict["name"],
        "website": vdict["website"],
        "country": "",
        "address": "",
        "size_bytes": "",
        "size_gib": "",
        "is_published": "",
        "time_until_publication": "",
        "posted_at_utc": "",
        "crawled_at_utc": crawled_at_utc,
        "crawled_at_kst": crawled_at_kst,
        "ransomware_group": "Coinbase Cartel",
        "discovery_date": "",
        "estimated_attack_date": "",
        "details_url": vdict["details_link"],
        "description": description,
        "files_api_present": "",
    }

def save_unified_csv_coinbase(victims: List[CC_Victim], out_dir: str = "outputs", filename: str = "coinbase_cartel_unified.csv"):
    kst = ZoneInfo("Asia/Seoul")
    now_utc = datetime.now(timezone.utc).isoformat()
    now_kst = datetime.now(kst).isoformat()

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    csv_path = out_path / filename

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=UNIFIED_HEADERS)
        writer.writeheader()
        for v in victims:
            row = to_unified_row(v, crawled_at_utc=now_utc, crawled_at_kst=now_kst)
            writer.writerow(row)

    print(f"\nCSV 저장 완료 (덮어쓰기): {csv_path.resolve()}")

def run_coinbase_cartel_crawler():
    print("--- Coinbase Cartel Crawler ---")
    res = get_tor_response(BASE_URL)
    if not res or not res.text:
        print("URL 데이터를 찾지 못함.")
        return

    print(f"--- Response Preview ---\n{res.text[:300]}")
    victims = parse_victims_from_html(res.text)
    print(f"\n총 {len(victims)}개 항목 파싱")
    if victims:
        pprint(victims[:5])  # 샘플 출력
    save_unified_csv_coinbase(victims)

if __name__ == "__main__":
    run_coinbase_cartel_crawler()

