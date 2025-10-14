import platform
import requests
from pprint import pprint
from bs4 import BeautifulSoup
from typing import List, Optional
from config import URL_COINBASE_CARTEL, PORT, PROXIES


PORT = "9150" if platform.system() == "Windows" else "9050"

PROXIES = {
    "http": f"socks5h://127.0.0.1:{PORT}",
    "https": f"socks5h://127.0.0.1:{PORT}"
}

URL_COINBASE_CARTEL = "http://fjg4zi4opkxkvdz7mvwp7h6goe4tcby3hhkrz43pht4j3vakhy75znyd.onion"


def get_tor_response(url: str, timeout: int = 30) -> Optional[requests.Response]:
    """
    주어진 URL에 대해 토르 네트워크를 통해 HTTP GET 요청.

    :param url: 요청을 보낼 URL 주소
    :param timeout: 요청 대기 시간 (초)
    :return: 성공 시 requests.Response 객체, 실패 시 None
    """
    print(f"Tor 프록시(포트: {PORT})를 통해 {url}에 접속을 시도")

    try:
        res = requests.get(url, proxies=PROXIES, timeout=timeout)
        res.raise_for_status()
        print("--- 접속 성공 ---")
        return res
    except Exception as e:
        print(f"Error: {e}")
        return None
    

class CC_Victim: 
    def __init__(self, name:str, industry:str=None, revenue:str=None, website:str=None, details_link:str=None):
        """
        Coinbase_Cartel_Victim 데이터 타입 클래스
        
        :param name: 회사 이름 (필수)
        :param industry: 산업 분야
        :param revenue: 매출
        :param website: 웹사이트 주소
        :param details_link: 상세 정보 링크, 링크가 없을 시엔 None
        """
        self.name = name
        self.industry = industry
        self.revenue = revenue
        self.website = website
        self.details_link = details_link

    def to_str(self):
        return f"[Name: {self.name}, industry: {self.industry}, revenue: {self.revenue}, website: {self.website}, details_link: {self.details_link}]"

    def __str__(self):
        return self.to_str()
    
    def __repr__(self):
        return self.to_str()


    def to_dict(self):
        return {
            'name': self.name,
            'industry': self.industry,
            'revenue': self.revenue,
            'website': self.website,
            'details_link': self.details_link
        }


def parse_victims_from_html(html_text: str) -> List[CC_Victim]:
    """
    HTML 전체 텍스트에서 모든 회사 정보를 추출하여 CC_Victim 객체 리스트로 반환.

    :param html_text: 파싱할 웹페이지의 전체 HTML 문자열
    :return: CC_Victim 객체들이 담긴 리스트
    """
    soup = BeautifulSoup(html_text, 'html.parser')

    articles = soup.select("div.companies-grid > article")

    victims = []

    for article in articles:
        try:
            name_tag = article.select_one("h3.card-name")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            industry = None
            revenue = None

            if meta_tag := article.select_one("div.card-meta"):
                for span in meta_tag.select("span"): # 명시적으로 span을 선택
                    span_text = span.get_text()
                    if "Industry" in span_text:
                        industry = span_text.replace("Industry:", "").strip()
                    elif "Revenue" in span_text:
                        revenue = span_text.replace("Revenue:", "").strip()

            website = None
            if website_tag := article.select_one("div.card-meta a"):
                website = website_tag.get("href")

            details_link = None
            if details_link_tag := article.select_one('a.view-detail'):
                link_path = details_link_tag.get("href")
                if link_path:
                    details_link = URL_COINBASE_CARTEL + link_path
                    
            victims.append(CC_Victim(
                name=name, industry=industry, revenue=revenue,
                website=website, details_link=details_link
            ))

        except Exception as e:
            print(f"개별 article 파싱 중 오류 발생: {e}")
            continue
    
    return victims


def run_coinbase_cartel_crawler():
    print("--- Coinbase Cartel Crawler Test ---")

    res = get_tor_response(URL_COINBASE_CARTEL)
    print(f"--- Coinbase Cartel Response ---\n{res.text[:300]}")

    if res and res.text:
        victims = parse_victims_from_html(res.text)
        print(f"\n 총 {victims.__len__()}개의 회사 정보 발견")
        pprint(victims)
    else:
        print("URL 데이터를 찾지 못함.")


if __name__ == "__main__":
    run_coinbase_cartel_crawler()