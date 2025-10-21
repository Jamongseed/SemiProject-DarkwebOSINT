import requests
import platform
from bs4 import BeautifulSoup, Tag
from typing import Optional, List
from pprint import pprint
import csv
import time

# --- 1. 기본 설정 ---
PORT = "9150" if platform.system() == "Windows" else "9050"
PROXIES = {
    "http": f"socks5h://127.0.0.1:{PORT}",
    "https": f"socks5h://127.0.0.1:{PORT}"
}
BASE_URL = "http://ijzn3sicrcy7guixkzjkib4ukbiilwc3xhnmby4mcbccnsd7j2rekvqd.onion/"

# --- 2. 데이터 보고서 양식 ---
class QilinVictim:
    def __init__(self, name: str, leak_date: str, data_size: str, file_count: str, details_link: Optional[str] = None):
        self.name = name
        self.leak_date = leak_date
        self.data_size = data_size
        self.file_count = file_count
        self.details_link = details_link

    def to_dict(self):
        return {
            "Name": self.name,
            "Leak_Date": self.leak_date,
            "Data_Size": self.data_size,
            "File_Count": self.file_count,
            "Details_Link": self.details_link
        }

    def __repr__(self):
        return (f"[Name: {self.name}, Date: {self.leak_date}, Size: {self.data_size}]")

# --- 3. 핵심 기능 함수 ---
def get_tor_response(url: str, timeout: int = 60) -> Optional[requests.Response]:
    print(f"[*] '{url}' 페이지에 접속 시도 중...")
    try:
        res = requests.get(url, proxies=PROXIES, timeout=timeout)
        res.raise_for_status()
        print("[+] 접속 성공!")
        return res
    except requests.exceptions.RequestException as e:
        print(f"[!] 접속 오류: {e}")
        return None

def get_qilin_company_info(div_tag: Tag) -> Optional[QilinVictim]:
    try:
        name_tag = div_tag.select_one("a.item_box-title")
        name = name_tag.get_text(strip=True) if name_tag else "이름 없음"
        
        meta_items = div_tag.select("div.item_box-info__item")
        leak_date, data_size, file_count = "날짜 없음", "용량 없음", "파일 수 없음"
        
        for item in meta_items:
            text = item.get_text(strip=True).upper()
            if any(month in text for month in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']):
                leak_date = item.get_text(strip=True)
            elif any(unit in text for unit in ['GB', 'MB', 'KB', 'TB']):
                data_size = item.get_text(strip=True)
            elif 'FILES' in text:
                file_count = item.get_text(strip=True).replace("files", "").strip()

        details_link_tag = div_tag.select_one('a.learn-more-button')
        details_url = BASE_URL.rstrip('/') + details_link_tag['href'] if details_link_tag and details_link_tag.has_attr('href') else None
        
        return QilinVictim(name, leak_date, data_size, file_count, details_url)
    except Exception as e:
        print(f"[!] 정보 추출 중 오류 발생: {e}")
        return None

# --- 4. 메인 실행 로직 (새로운 페이지네이션 방식 적용) ---
def main():
    all_victims: List[QilinVictim] = []
    page_number = 1

    while True: # 무한 반복 시작
        # [새로운 페이지네이션]
        # 페이지 번호를 이용해 URL을 직접 만듭니다.
        target_url = f"{BASE_URL}?page={page_number}"
        
        response = get_tor_response(target_url)
        if not response:
            print(f"[!] {page_number} 페이지 접속에 실패하여 크롤링을 중단합니다.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        div_list = soup.select("div.list-view div.item_box")
        
        # [종료 조건] 
        # 만약 현재 페이지에 게시물이 하나도 없다면, 마지막 페이지를 넘긴 것이므로 반복을 멈춥니다.
        if not div_list:
            print(f"[*] {page_number} 페이지에 더 이상 게시물이 없습니다. 크롤링을 종료합니다.")
            break

        page_victims = [info for div in div_list if (info := get_qilin_company_info(div))]
        all_victims.extend(page_victims)
        print(f"[*] {page_number} 페이지에서 {len(page_victims)}개의 데이터를 수집했습니다. (총 {len(all_victims)}개)")
        
        page_number += 1 # 다음 페이지로 넘어가기 위해 페이지 번호를 1 증가시킵니다.
        time.sleep(2)    # 사이트 보호를 위해 2초 대기

    # --- CSV 저장 기능 ---
    if all_victims:
        csv_filename = "qilin_final_data.csv"
        try:
            with open(csv_filename, "w", newline="", encoding="utf-8-sig") as csvfile:
                headers = all_victims[0].to_dict().keys()
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows([victim.to_dict() for victim in all_victims])
            print(f"\n[+] 총 {len(all_victims)}개의 데이터를 '{csv_filename}' 파일에 성공적으로 저장했습니다.")
        except Exception as e:
            print(f"\n[!] CSV 파일 저장 중 오류 발생: {e}")
    else:
        print("\n[!] 수집된 데이터가 없어 CSV 파일을 생성하지 않았습니다.")

if __name__ == "__main__":
    main()
