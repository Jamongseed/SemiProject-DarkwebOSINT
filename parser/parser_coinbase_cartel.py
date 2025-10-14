from bs4 import BeautifulSoup
from typing import List
from config import URL_COINBASE_CARTEL


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
    