from bs4 import BeautifulSoup
from pprint import pprint
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import re

def get_html_sync(url: str, timeout: int = 10) -> str | None:
    """
    주어진 URL의 HTML 콘텐츠를 동기적으로 가져옵니다.
    """
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
    """
    ransomware.live HTML 콘텐츠를 파싱하여 통계, 피해자 목록, 크롤링 시간을 반환합니다.
    """
    kst_timezone = ZoneInfo("Asia/Seoul")
    crawl_time_utc = datetime.now(timezone.utc).isoformat()
    crawl_time_kst = datetime.now(kst_timezone).isoformat()

    soup = BeautifulSoup(html_content, 'html.parser')

    def get_victim_details(item):
        try:
            name = item.select_one('strong').get_text(strip=True)
            group = item.select_one('small a span.badge').get_text(strip=True)
            date_container = item.select_one('div.text-body-secondary')
            date_text = date_container.get_text(" ", strip=True) if date_container else ""
            discovery_date_match = re.search(r"Discovery Date: ([\d-]+)", date_text)
            discovery_date = discovery_date_match.group(1) if discovery_date_match else 'N/A'
            attack_date_match = re.search(r"Estimated Attack Date: ([\d-]+)", date_text)
            estimated_attack_date = attack_date_match.group(1) if attack_date_match else 'Not available'
            description_tag = item.select_one('div.bg-body-secondary')
            description = description_tag.get_text(strip=True) if description_tag else 'No description available.'
            country_tag = item.select_one('img[style*="width: 32px"]')
            country = country_tag['alt'] if country_tag and country_tag.has_attr('alt') else 'N/A'
            website_tag = item.select_one('a:has(i.fa-globe-americas)')
            website = website_tag['href'] if website_tag and website_tag.has_attr('href') else 'Not available'
            details_link_tag = item.select_one('a[href*="/id/"]')
            details_url = "https://www.ransomware.live" + details_link_tag['href'] if details_link_tag and details_link_tag.has_attr('href') else 'Not available'
            
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

def main():
    """
    크롤링 및 파싱 프로세스를 실행하는 메인 함수
    """
    URL = "https://www.ransomware.live/"
    print(f"'{URL}'에서 데이터 크롤링을 시작합니다...")
    
    # 1. 웹사이트에서 HTML 콘텐츠를 가져옵니다.
    html_content = get_html_sync(URL)

    # 2. HTML 콘텐츠를 성공적으로 가져왔는지 확인합니다.
    if html_content:
        print("\n크롤링 성공! 데이터 파싱을 시작합니다...")
        
        # 3. HTML을 파싱하여 데이터를 추출합니다.
        ransomware_data = parse_ransomware_live_data(html_content)
        
        # 4. 최종 결과를 보기 좋게 출력합니다.
        print("\n--- 파싱 완료된 데이터 ---")
        pprint(ransomware_data)
        print("\n🎉 프로그램이 성공적으로 실행되었습니다.")
    else:
        print("\n❗️ HTML 콘텐츠를 가져오지 못해 파싱을 진행할 수 없습니다.")
        print("프로그램 실행에 실패했습니다.")


if __name__ == "__main__":
    main()