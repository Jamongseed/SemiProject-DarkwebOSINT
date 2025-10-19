# crawler_ransomware_live.py
from bs4 import BeautifulSoup
from pprint import pprint
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import re
from pathlib import Path
import csv

# --- 통합 스키마 헤더 ---
UNIFIED_HEADERS = [
    "source", "record_type", "id", "company", "website", "country", "address",
    "size_bytes", "size_gib", "is_published", "time_until_publication",
    "posted_at_utc", "crawled_at_utc", "crawled_at_kst",
    "ransomware_group", "discovery_date", "estimated_attack_date",
    "details_url", "description", "files_api_present"
]

def get_html_sync(url: str, timeout: int = 10) -> str | None:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        print(f"✅ [{url}] - HTML 콘텐츠 로드 성공")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ [{url}] - 오류 발생: {e}")
        return None

def parse_ransomware_live_data(html_content):
    kst_timezone = ZoneInfo("Asia/Seoul")
    crawl_time_utc = datetime.now(timezone.utc).isoformat()
    crawl_time_kst = datetime.now(kst_timezone).isoformat()

    soup = BeautifulSoup(html_content, 'html.parser')

    def get_victim_details(item):
        try:
            name = item.select_one('strong').get_text(strip=True)

            group_el = item.select_one('small a span.badge')
            group = group_el.get_text(strip=True) if group_el else ""

            date_container = item.select_one('div.text-body-secondary')
            date_text = date_container.get_text(" ", strip=True) if date_container else ""
            discovery_date_match = re.search(r"Discovery Date: ([\d-]+)", date_text)
            discovery_date = discovery_date_match.group(1) if discovery_date_match else ""
            attack_date_match = re.search(r"Estimated Attack Date: ([\d-]+)", date_text)
            estimated_attack_date = attack_date_match.group(1) if attack_date_match else ""

            description_tag = item.select_one('div.bg-body-secondary')
            description = description_tag.get_text(strip=True) if description_tag else ""

            country_tag = item.select_one('img[style*="width: 32px"]')
            country = country_tag['alt'].strip() if country_tag and country_tag.has_attr('alt') else ""
            country = country.upper() if len(country) == 2 else ""  # ISO-2만 유지

            website_tag = item.select_one('a:has(i.fa-globe-americas)')
            website = website_tag['href'].strip() if website_tag and website_tag.has_attr('href') else ""

            details_link_tag = item.select_one('a[href*="/id/"]')
            details_url = ("https://www.ransomware.live" + details_link_tag['href'].strip()) if (details_link_tag and details_link_tag.has_attr('href')) else ""
            
            return {
                "company_name": name,
                "ransomware_group": group,
                "discovery_date": discovery_date,
                "estimated_attack_date": estimated_attack_date,
                "description": description,
                "country": country,
                "website": website,
                "details_url": details_url
            }
        except Exception:
            return None

    # (선택) 상단 카운터 파싱은 기존대로 유지
    statistics = {}
    try:
        scripts = soup.find_all('script')
        script_text = ""
        for script in scripts:
            if 'animateCounter' in script.text:
                script_text = script.text
                break

        groups_match = re.search(r"animateCounter\('groupsCounter',\s*\d+,\s*([\d,]+)", script_text)
        victims_match = re.search(r"animateCounter\('victimsCounter',\s*\d+,\s*([\d,]+)", script_text)
        year_match = re.search(r"animateCounter\('victimsThisYearCounter',\s*\d+,\s*([\d,]+)", script_text)
        month_match = re.search(r"animateCounter\('victimsThisMonthCounter',\s*\d+,\s*([\d,]+)", script_text)

        statistics = {
            "Total Groups": int(groups_match.group(1).replace(',', '')) if groups_match else 0,
            "Total Victims": int(victims_match.group(1).replace(',', '')) if victims_match else 0,
            "Victims This Year": int(year_match.group(1).replace(',', '')) if year_match else 0,
            "Victims This Month": int(month_match.group(1).replace(',', '')) if month_match else 0
        }
    except Exception as e:
        print(f"통계 데이터 추출 중 오류 발생: {e}")
        statistics = {}

    victim_items = soup.select('#victim-list .victim-item')
    victims_list = [data for item in victim_items if (data := get_victim_details(item)) is not None]

    return {
        "crawled_at_utc": crawl_time_utc,
        "crawled_at_kst": crawl_time_kst,
        "statistics": statistics,
        "victims": victims_list
    }

def save_csvs(results: dict, out_dir: str = "outputs", prefix: str = "ransomware_live"):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 통계는 append
    stats_file = out_path / f"{prefix}_stats.csv"
    stats_headers = [
        "crawled_at_utc", "crawled_at_kst",
        "Total Groups", "Total Victims", "Victims This Year", "Victims This Month"
    ]
    write_header = not stats_file.exists()
    with stats_file.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=stats_headers)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "crawled_at_utc": results.get("crawled_at_utc"),
            "crawled_at_kst": results.get("crawled_at_kst"),
            "Total Groups": results.get("statistics", {}).get("Total Groups", 0),
            "Total Victims": results.get("statistics", {}).get("Total Victims", 0),
            "Victims This Year": results.get("statistics", {}).get("Victims This Year", 0),
            "Victims This Month": results.get("statistics", {}).get("Victims This Month", 0),
        })

    # 원본 victims.csv (덮어쓰기) - 기존 포맷 유지용
    victims_file = out_path / f"{prefix}_victims.csv"
    victim_headers = [
        "crawled_at_utc", "crawled_at_kst",
        "company_name", "ransomware_group",
        "discovery_date", "estimated_attack_date",
        "description", "country", "website", "details_url"
    ]
    with victims_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=victim_headers)
        writer.writeheader()
        for v in results.get("victims", []):
            writer.writerow({
                "crawled_at_utc": results.get("crawled_at_utc"),
                "crawled_at_kst": results.get("crawled_at_kst"),
                **v
            })
    print(f" - 원본 피해자: {victims_file.resolve()}")

def save_unified_csv_ransomware(results: dict, out_dir: str = "outputs",
                                filename: str = "ransomware_live_unified.csv"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / filename

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNIFIED_HEADERS)
        w.writeheader()

        crawled_at_utc = results.get("crawled_at_utc", "")
        crawled_at_kst = results.get("crawled_at_kst", "")
        for v in results.get("victims", []):
            rid = v.get("details_url") or f'{v.get("company_name","")}|{v.get("ransomware_group","")}|{v.get("discovery_date","")}'
            # 국가/웹사이트 등 결측은 이미 parse 단계에서 빈칸 처리됨
            w.writerow({
                "source": "ransomware.live",
                "record_type": "victim",
                "id": rid,
                "company": v.get("company_name", ""),
                "website": v.get("website", ""),
                "country": v.get("country", ""),
                "address": "",
                "size_bytes": "",
                "size_gib": "",
                "is_published": "",
                "time_until_publication": "",
                "posted_at_utc": "",
                "crawled_at_utc": crawled_at_utc,
                "crawled_at_kst": crawled_at_kst,
                "ransomware_group": v.get("ransomware_group", ""),
                "discovery_date": v.get("discovery_date", ""),
                "estimated_attack_date": v.get("estimated_attack_date", ""),
                "details_url": v.get("details_url", ""),
                "description": v.get("description", ""),
                "files_api_present": ""
            })
    print(f" - 통합(덮어쓰기): {path.resolve()}")

def main():
    URL = "https://www.ransomware.live/"
    print(f"'{URL}'에서 데이터 크롤링을 시작합니다...")
    html_content = get_html_sync(URL)

    if html_content:
        print("\n크롤링 성공! 데이터 파싱을 시작합니다...")
        ransomware_data = parse_ransomware_live_data(html_content)

        print("\n--- 파싱 완료된 데이터 ---")
        pprint(ransomware_data)

        save_csvs(ransomware_data, out_dir="outputs", prefix="ransomware_live")
        save_unified_csv_ransomware(ransomware_data, out_dir="outputs",
                                    filename="ransomware_live_unified.csv")
        print("\n🎉 프로그램이 성공적으로 실행되었습니다.")
    else:
        print("\n❗️ HTML 콘텐츠를 가져오지 못해 파싱을 진행할 수 없습니다.")
        print("프로그램 실행에 실패했습니다.")

if __name__ == "__main__":
    main()

