import csv
import re
import platform
import asyncio
import httpx
import sys
import logging
import json
import dataclasses
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Any, Tuple, Optional
from abc import ABC, abstractmethod


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BASE_URL = "http://qeei4m7a2tve6ityewnezvcnf647onsqbmdbmlcw4y5pr6uwwfwa35yd.onion/"
OUTPUT_DIR = "outputs"
OUTPUT_FILENAME = "dark_forums_unified.csv"
STATE_FILENAME = "crawl_state.json"

# Tor 프록시 설정 (Windows의 Tor Browser 기본 포트: 9150, macOS/Linux: 9050)
PORT = "9150" if platform.system() == "Windows" else "9050"
TOR_PROXY = f"socks5h://127.0.0.1:{PORT}"
HTTPX_TRANSPORT = httpx.AsyncHTTPTransport(retries=3, proxy=TOR_PROXY, verify=False)

TARGET_FORUMS = {
    # Home
    "Announcements": "Forum-Announcements",
    "Introductions": "Forum-Introductions",
    "Support & Suggestions": "Forum-Support-Suggestions",
    "World News": "Forum-World-News",
    "The Lounge": "Forum-The-Lounge",
    "Freebies & Courses": "Forum-Freebies-Courses",
    "Tutorials": "Forum-Tutorials",

    # Leaks
    "Databases": "Forum-Databases",
    "Stealer Logs": "Forum-Stealer-Logs",
    "Games": "Forum-Games",
    "Source Codes": "Forum-Source-Codes",
    "Other Leaks": "Forum-Other-Leaks",
    "Database Discussion": "Forum-Database-Discussion",
    "Combolists": "Forum-Combolists",
    "Doxes": "Forum-Doxes",
    "HackTheBox & TryHackMe": "Forum-HackTheBox-TryHackMe",
    "Cracked Accounts": "Forum-Cracked-Accounts",

    # Marketplace
    "Sellers Place": "Forum-Sellers-Place",
    "Services": "Forum-Services",
    "Premium Marketplace": "Forum-Premium-Marketplace",
    "Currency Exchange": "Forum-Currency-Exchange",
    "Buyers Place": "Forum-Buyers-Place",
    "Scam Reports": "Forum-Scam-Reports",

    # Gaming
    "PUBG": "Forum-PUBG",
    "GTA V": "Forum-GTA-V",
    "Valorant": "Forum-Valorant",
    "Other Games": "Forum-Other-Games",

    # Hacking
    "Web application vulnerabilities": "Forum-Web-application-vulnerabilities",
    "Software Vulnerabilities / Exploitation": "Forum-Software-Vulnerabilities-Exploitation",
    "Malware": "Forum-Malware",
    "Hacking Tutorials": "Forum-Hacking-Tutorials",
    "Hacking Tools": "Forum-Hacking-Tools",
    "Exploit & POCs": "Forum-Exploit-POCs",

    # Cracking
    "Cracking Tutorials": "Forum-Cracking-Tutorials",
    "Configs": "Forum-Configs",
    "Cracking Discussion": "Forum-Cracking-Discussion",
    "Cracking Tools": "Forum-Cracking-Tools",

    # Tech (Coding)
    "Programming": "Forum-Programming",
    "All things C": "Forum-All-things-C",
    "java": "Forum-java",
    "Other languages": "Forum-Other-languages",

    # Tech (Nulled_Scripts)
    "PHP Script": "Forum-PHP-Script",
    "HTML Template": "Forum-HTML-Template",
    "Mobile Apps": "Forum-Mobile-Apps",
    "Others": "Forum-Others",

    # Tech (Security)
    "General": "Forum-General",
    "OSINT": "Forum-OSINT",
    "Operational Security": "Forum-Operational-Security",

    # Premium
    "Upgraded Lounge": "Forum-Upgraded-Lounge",
    "Premium Databases": "Forum-Premium-Databases",
    "GOD users Area": "Forum-GOD-users-Area",
    "Premium Other Leaks": "Forum-Premium-Other-Leaks",
    "Premium Cracked Accounts": "Forum-Premium-Cracked-Accounts",
    "Premium Combolist": "Forum-Premium-Combolist",
    "Premium Source Codes": "Forum-Premium-Source-Codes",

    # Staff
    "Staff Applications": "Forum-Staff-Applications"
}

# # 테스트용
# TARGET_FORUMS = {
#     # 페이지 태그 선택 테스트 예시 : 페이지 넘버 없음
#      # 1페이지
#     "Cracking Tutorials": "Forum-Cracking-Tutorials",
#     # 페이지 생략 없는 다수 페이지 예시 : 1 2 3 4
#      # 404 테스트
#     "Tutorials": "Forum-Tutorials",
#     # 페이지 생략 있는 다수 페이지 예시 : 1 2 ... 7
#     "Databases": "Forum-Databases",
# }

# --- 크롤링 범위 제한 설정 ---
# 각 게시판에서 크롤링할 최대 게시물 수 (None으로 설정 시 모두 크롤링)
MAX_POSTS_PER_FORUM = None
# 각 게시판에서 크롤링할 최대 페이지 수 (None으로 설정 시 모두 크롤링)
MAX_PAGES_PER_FORUM = None # 예: 3 페이지로 제한
# True : 중복을 발견해도 다른 게시판으로 넘어가지 말고 다음 페이지 계속 진행
DUPLICATION_KEEP_SEARCH = False
# ---

UNIFIED_HEADERS = [
    "source", "record_type", "id", "company", "website", "country", "address",
    "size_bytes", "size_gib", "is_published", "time_until_publication",
    "posted_at_utc", "crawled_at_utc", "crawled_at_kst",
    "ransomware_group", "discovery_date", "estimated_attack_date",
    "details_url", "description", "files_api_present", "forum", "title", "author", 
    "last_edited_info", "author_rank", "reputation",
    "posts_count", "threads_count", "join_date", "main_content"
]

# --- 치명적 오류 발생 시 사용할 사용자 정의 예외 ---
class CriticalCrawlStop(Exception):
    """크롤링을 즉시 중지하고 상태를 저장해야 할 때 발생하는 예외"""
    pass


@dataclasses.dataclass
class PageCrawlResult:
    """_crawl_page 메서드의 크롤링 결과(상태)를 담는 데이터 클래스"""
    page_data: List[Dict[str, Any]]
    new_urls_found: bool
    processed_count: int
    errors: int
    http_errors: int
    is_critical_failure: bool = False


def setup_csv_limit():
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt / 10)
    logging.info(f"CSV field size limit이 {maxInt}로 설정되었습니다.")


def load_existing_urls_from_csv(csv_path: Path) -> Set[str]:
    if not csv_path.is_file():
        logging.warning(f"기존 CSV 파일({csv_path})을 찾을 수 없습니다. 새 파일로 시작합니다.")
        return set()
    existing_urls = set()
    try:
        with csv_path.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'id' in row and row['id']:
                    existing_urls.add(row['id'])
        logging.info(f"CSV에서 {len(existing_urls)}개의 기존 게시물 ID를 로드했습니다.")
        return existing_urls
    except Exception as e:
        logging.error(f"CSV 파일 읽기 중 오류 발생: {e}. 빈 set으로 시작합니다.")
        return set()

def save_to_csv(data_list: List[Dict[str, Any]], out_dir: str = OUTPUT_DIR, filename: str = OUTPUT_FILENAME):
    if not data_list:
        return
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    csv_path = out_path / filename
    file_exists = csv_path.is_file()
    try:
        with csv_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=UNIFIED_HEADERS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(data_list)
        logging.info(f"[+] {len(data_list)}개 게시물 정보를 '{csv_path}'에 추가했습니다.")
    except Exception as e:
        logging.error(f"CSV 파일 저장 중 오류 발생: {e}")

    
def convert_to_iso_utc(input_str: str) -> str:
    now = datetime.now(timezone.utc)

    clean_str = input_str.strip()

    try:
        # "7 hours ago" (UTC 기준 7시간 전)
        match_hours = re.search(r'(\d+)\s+hour(s?)\s+ago', clean_str, re.IGNORECASE)
        if match_hours:
            hours_ago = int(match_hours.group(1))
            utc_dt = now - timedelta(hours=hours_ago)
            return utc_dt.isoformat()

        # "Yesterday, 02:50 PM" (UTC 기준 어제)
        if clean_str.startswith("Yesterday") or clean_str.startswith("Today"):
            day_part, time_part = clean_str.split(',', 1)
            time_part = time_part.strip()
            
            parsed_time = datetime.strptime(time_part, "%I:%M %p").time()
            
            if day_part == "Yesterday":
                target_date = (now - timedelta(days=1)).date()
            else:
                target_date = now.date()
                
            naive_dt = datetime.combine(target_date, parsed_time)

            utc_dt = naive_dt.replace(tzinfo=timezone.utc)
            return utc_dt.isoformat()

        # "16-05-25, 02:12 PM" (UTC 시간)
        match_absolute = re.search(r'(\d{2}-\d{2}-\d{2},\s+\d{2}:\d{2}\s+[AP]M)', clean_str)
        if match_absolute:
            date_str = match_absolute.group(1)
            
            naive_dt = datetime.strptime(date_str, "%d-%m-%y, %I:%M %p")
            
            utc_dt = naive_dt.replace(tzinfo=timezone.utc)
            return utc_dt.isoformat()

        logging.warning(f"날짜 형식 변환 실패: '{date_str}'")
        return ""

    except Exception as e:
        logging.warning(f"날짜 형식 변환 실패: '{date_str}' {e}")
        return ""

# --- 이어하기 (Resume) 기능 함수 (Receiver인 Crawler가 사용) ---

def save_crawl_state(forum_uri: str, next_page_num: int):
    """현재 크롤링 상태(다음 포럼, 다음 페이지)를 JSON 파일에 저장합니다."""
    state = {
        "current_forum_uri": forum_uri,
        "next_page_to_crawl": next_page_num
    }
    try:
        with open(STATE_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4)
        logging.debug(f"상태 저장됨: 포럼 {forum_uri}, 다음 페이지 {next_page_num}")
    except IOError as e:
        logging.error(f"크롤링 상태 파일 저장 실패: {e}")

def load_crawl_state() -> Tuple[Optional[str], int]:
    """크롤링 상태 파일을 읽어 (포럼 URI, 시작 페이지)를 반환합니다. (Invoker가 사용)"""
    try:
        if not Path(STATE_FILENAME).is_file():
            logging.info("크롤링 상태 파일이 없습니다. 처음부터 시작합니다.")
            return None, 1
            
        with open(STATE_FILENAME, 'r', encoding='utf-8') as f:
            state = json.load(f)
            forum_uri = state.get("current_forum_uri")
            next_page = state.get("next_page_to_crawl", 1)
            logging.info(f"크롤링 상태 로드: 포럼 {forum_uri}, 페이지 {next_page}부터 시작합니다.")
            return forum_uri, next_page
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"크롤링 상태 파일 로드 실패: {e}. 처음부터 다시 시작합니다.")
        return None, 1


def clear_crawl_state():
    """모든 크롤링이 성공적으로 완료되면 상태 파일을 삭제합니다."""
    try:
        state_path = Path(STATE_FILENAME)
        if state_path.is_file():
            state_path.unlink()
            logging.info(f"모든 작업 완료. 크롤링 상태 파일('{STATE_FILENAME}') 삭제됨.")
    except IOError as e:
        logging.error(f"상태 파일 삭제 실패: {e}")

# --- 1. Receiver (수신자) ---
# 실제 크롤링 로직을 모두 캡슐화하는 클래스

class Crawler:
    """
    실제 크롤링 작업을 수행하는 Receiver 클래스.
    HTTP 클라이언트와 중복 URL 세트를 상태로 관리합니다.
    """
    def __init__(self, client: httpx.AsyncClient, crawled_post_urls: Set[str]):
        self.client = client
        self.crawled_post_urls = crawled_post_urls
        self.total_posts_saved = 0
        self.total_errors = 0
        self.total_http_errors = 0

    async def _async_get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """(private) URL에서 BeautifulSoup 객체를 비동기로 가져옵니다."""
        try:
            response = await self.client.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except httpx.HTTPStatusError as e:
            logging.warning(f"HTTP 상태 에러: {e.response.status_code} - {e.request.url}")
            raise
        except Exception as e:
            logging.error(f"페이지 요청 중 알 수 없는 에러: {e} - {url}")
            raise

    async def _crawl_post_details(self, post_url: str) -> Optional[Dict[str, str]]:
        """(private) 게시물 상세 페이지를 스크랩합니다."""
        logging.debug(f"    - 상세 페이지 크롤링 시작: {post_url}")
        
        soup = await self._async_get_soup(post_url)
        if not soup:
            logging.warning(f"    - 상세 페이지 응답값 없음: {post_url}")
            return None 

        details = {'details_url': post_url}
        first_post = soup.select_one("#posts > .post.classic:first-of-type")
        if not first_post:
            logging.warning(f"    - 상세 페이지에서 첫 번째 게시물({post_url})을 찾을 수 없습니다.")
            return None

        def get_text(base, selector, default="N/A"):
            try:
                return base.select_one(selector).text.strip()
            except AttributeError:
                return default

        details['title'] = get_text(soup, ".thread-info__name")
        details['author'] = get_text(first_post, ".post_user-profile a")
        details['posted_date'] = get_text(first_post, ".post_date").split('\n')[0]
        details['last_edited_info'] = get_text(first_post, ".post_edit em", "N/A")
        details['main_content'] = get_text(first_post, ".post_body")
        details['author_rank'] = get_text(first_post, ".post_user-title")
        details['reputation'] = get_text(first_post, ".reputation_positive, .reputation_neutral, .reputation_negative", "0")
        
        author_stats = {spans[0].text.strip(): spans[1].text.strip()
                        for stat in first_post.select(".post_author-stats .post_stats-bit.group")
                        if (spans := stat.find_all("span")) and len(spans) == 2}
        
        details['posts_count'] = author_stats.get("Posts", "N/A")
        details['threads_count'] = author_stats.get("Threads", "N/A")
        details['join_date'] = author_stats.get("Joined", "N/A")

        for k, v in details.items():
            details[k] = re.sub(r'\s+', ' ', v).strip()

        return details

    def _process_page_results(self, results: List[Any], forum_name: str, crawled_at_utc: str, crawled_at_kst: str) -> Tuple[List[Dict[str, Any]], int, int]:
        """(private) asyncio.gather의 결과를 처리하여 CSV 행으로 변환합니다."""
        page_data = []
        error_count = 0
        http_error_count = 0

        for details in results:
            if isinstance(details, httpx.HTTPStatusError):
                logging.warning(f"  [Error] 웹 요청 실패: {details}")
                http_error_count += 1
                continue
            elif isinstance(details, Exception):
                logging.warning(f"  [Error] 비동기 작업 실패: {details}")
                error_count += 1
                continue
            elif not details:
                logging.warning("  [Error] 웹 응답 없음 또는 파싱 실패.")
                error_count += 1
                continue

            try:
                posted_at_utc = convert_to_iso_utc(details.pop("posted_date", ""))
                post_id = details.get("details_url", "")
                details_url = details.pop("details_url")
                row = {
                    "forum": forum_name, "source": "darkforums.st", "record_type": "leak_post",
                    "id": post_id, "posted_at_utc": posted_at_utc, "crawled_at_utc": crawled_at_utc,
                    "crawled_at_kst": crawled_at_kst, "details_url": details_url, **details 
                }
                for header in UNIFIED_HEADERS:
                    if header not in row:
                        row[header] = ""
                page_data.append(row)
            except Exception as e:
                logging.error(f"  [Error] 결과 처리 중 예외 발생: {e} - (데이터: {details})")
                error_count += 1
                
        return page_data, error_count, http_error_count
    
    def _get_last_page_number(self, soup: BeautifulSoup) -> int:
        """(private) 게시판 목록으로부터 마지막 페이지 번호를 추출합니다."""
        page_tag = soup.select_one("div.pagination")
        if not page_tag:
            logging.info("  페이지네이션 태그를 찾을 수 없음 (1페이지가 마지막).")
            return 1
        last_link_tag = page_tag.select_one("a.pagination_last")
        if last_link_tag:
            try: return int(last_link_tag.text)
            except (ValueError, TypeError): pass 
        page_links = page_tag.select("a.pagination_page")
        if page_links:
            try: return int(page_links[-1].text)
            except (ValueError, TypeError, IndexError): pass 
        logging.info("  마지막 페이지 링크를 찾을 수 없음 (1페이지가 마지막).")
        return 1

    async def _crawl_page(self, page_url: str, forum_name: str, current_total_posts: int) -> PageCrawlResult:
        """(private) 단일 페이지를 크롤링합니다."""
        logging.info(f"  - 페이지 방문 중: {page_url}")
        try:
            list_soup = await self._async_get_soup(page_url)
        except (httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadTimeout) as e:
            logging.error(f"  [치명적 네트워크 오류]: {e}. 진행 상황을 저장하고 중지를 시도합니다.")
            return PageCrawlResult([], False, 0, 1, 0, True)
        except httpx.HTTPStatusError as e:
            # 4xx, 5xx 등 일반 HTTP 오류 (건너뛰기)
            logging.warning(f"  페이지 {page_url} 요청 실패 (HTTP {e.response.status_code}): {e}. 이 페이지만 건너뜁니다.")
            return PageCrawlResult([], False, 0, 0, 1)
        except Exception as e:
            logging.error(f"  페이지 {page_url} 요청 실패: {e}. 이 페이지만 건너뜁니다.")
            return PageCrawlResult([], False, 0, 1, 1 if isinstance(e, httpx.HTTPStatusError) else 0)

        if not list_soup:
            logging.error(f"  페이지 {page_url} 내용을 가져오지 못했습니다. 이 페이지만 건너뜁니다.")
            return PageCrawlResult([], False, 0, 1, 0)

        posts_on_page_tags = list_soup.select("span[id^='tid_'] a")
        if not posts_on_page_tags:
            logging.info("  이 페이지에서 게시물을 찾을 수 없습니다.")
            return PageCrawlResult([], False, 0, 0, 0)
        
        utc_now = datetime.now(timezone.utc)
        kst_now = utc_now.astimezone(ZoneInfo('Asia/Seoul'))
        crawled_at_utc = utc_now.isoformat(timespec='microseconds')
        crawled_at_kst = kst_now.isoformat(timespec='microseconds')

        tasks, new_urls_on_page_count, temp_total_posts = [], 0, current_total_posts

        for post_link_tag in posts_on_page_tags:
            post_url = urljoin(BASE_URL, post_link_tag['href'])
            if post_url not in self.crawled_post_urls:
                new_urls_on_page_count += 1
                if MAX_POSTS_PER_FORUM is not None and temp_total_posts >= MAX_POSTS_PER_FORUM:
                    logging.info(f"    - 게시물 최대 개수({MAX_POSTS_PER_FORUM}) 도달. 작업 추가 중단.")
                    break 
                tasks.append(self._crawl_post_details(post_url)) # [*] self._crawl_post_details 호출
                self.crawled_post_urls.add(post_url) 
                temp_total_posts += 1
        
        if not tasks:
            return PageCrawlResult([], new_urls_on_page_count > 0, 0, 0, 0)

        logging.info(f"    ... {len(tasks)}개 게시물 비동기 크롤링 시작 ...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logging.info(f"    ... {len(tasks)}개 게시물 비동기 크롤링 완료 ...")

        page_data, errors, http_errors = self._process_page_results(
            results, forum_name, crawled_at_utc, crawled_at_kst
        )
        processed_count = len(page_data)
        
        if processed_count == 0 and len(tasks) > 0:
            logging.warning("  이 페이지의 모든 작업이 실패했거나 (None) 결과 처리 중 오류가 발생했습니다.")

        return PageCrawlResult(page_data,
                               new_urls_on_page_count > 0,
                               processed_count,
                               errors,
                               http_errors,
                               is_critical_failure=False)

    async def crawl_forum(self, forum_display_name: str, forum_uri: str, start_page: int = 1):
        """(public) 특정 포럼을 `start_page`부터 크롤링합니다."""
        logging.info(f"\n{'='*50}\n[+] 게시판 '{forum_display_name}' 크롤링 시작...\n{'='*50}")
        base_forum_url = urljoin(BASE_URL, forum_uri)
        total_posts_saved_in_forum = 0
        
        try:
            first_page_soup = await self._async_get_soup(base_forum_url)
        except (httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadTimeout) as e:
            # 치명적 네트워크 오류 (게시판 목록)
            logging.error(f"  [치명적 네트워크 오류] (게시판 {forum_display_name}): {e}. 진행 상황을 저장하고 중지합니다.")
            # 실패한 '현재 시작 페이지(start_page)'를 저장하여 재시도하도록 함
            save_crawl_state(forum_uri, start_page)
            # CrawlManager에게 중지 신호를 보냄
            raise CriticalCrawlStop(f"Server disconnected at {base_forum_url}")
        except httpx.HTTPStatusError as e:
            # 일반 HTTP 오류 (예: 404 - 게시판 없음)
            logging.warning(f"  게시판 '{forum_display_name}' 접근 실패 (HTTP {e.response.status_code}): {e}. 이 게시판을 건너뜁니다.")
            self.total_errors += 1
            self.total_http_errors += 1
            return # 이 커맨드(게시판)만 종료하고 다음 커맨드로 넘어감
        except Exception as e:
            # 기타 알 수 없는 오류
            logging.error(f"  게시판 '{forum_display_name}' 접근 중 알 수 없는 에러: {e}. 이 게시판을 건너뜁니다.")
            self.total_errors += 1
            return # 이 커맨드(게시판)만 종료

        if not first_page_soup:
            logging.error(f"  게시판 '{forum_display_name}' 첫 페이지 내용을 가져오지 못했습니다. 이 게시판을 건너뜁니다.")
            self.total_errors += 1
            return

        last_page = self._get_last_page_number(first_page_soup)
        logging.info(f"  게시판 '{forum_display_name}'의 총 페이지 수: {last_page}")

        if start_page > 1:
            logging.info(f"  ... '{forum_display_name}' 게시판의 {start_page} 페이지부터 크롤링을 다시 시작합니다.")

        for current_page_num in range(start_page, last_page + 1):
            if MAX_POSTS_PER_FORUM is not None and total_posts_saved_in_forum >= MAX_POSTS_PER_FORUM:
                logging.info(f"  게시물 크롤링 최대 개수({MAX_POSTS_PER_FORUM})에 도달하여 다음 게시판으로 넘어갑니다.")
                break
            if MAX_PAGES_PER_FORUM is not None and (current_page_num - start_page + 1) > MAX_PAGES_PER_FORUM:
                logging.info(f"  페이지 크롤링 최대 개수({MAX_PAGES_PER_FORUM})에 도달하여 다음 게시판으로 넘어갑니다.")
                break

            page_url = base_forum_url if current_page_num == 1 else f"{base_forum_url}?page={current_page_num}"
            
            result = await self._crawl_page(
                page_url, 
                forum_uri.replace("Forum-", "").lower(), 
                total_posts_saved_in_forum
            )
            
            # Receiver의 전역 상태 업데이트
            total_posts_saved_in_forum += result.processed_count
            self.total_posts_saved += result.processed_count
            self.total_errors += result.errors
            self.total_http_errors += result.http_errors

            if result.is_critical_failure:
                logging.warning(
                    f"  '{forum_display_name}'에서 치명적 오류 감지. "
                    f"페이지 {current_page_num}에서 중지합니다."
                )

                save_crawl_state(forum_uri, current_page_num)

                raise CriticalCrawlStop(f"Server disconnected at {page_url}")

            if result.page_data:
                save_to_csv(result.page_data)
            
            # [*] Receiver가 스스로의 상태를 저장
            save_crawl_state(forum_uri, current_page_num + 1)

            if not result.new_urls_found:
                logging.info(f"  페이지 {current_page_num}에서 새로운 게시물을 찾지 못했습니다.")
                if not DUPLICATION_KEEP_SEARCH:
                    logging.info("  이 게시판을 종료합니다.(DUPLICATION_KEEP_SEARCH=False)")
                    break
                logging.info("  다음 페이지를 계속 검색합니다.(DUPLICATION_KEEP_SEARCH=True)")
        
        logging.info(f"[+] 게시판 '{forum_display_name}' 크롤링 완료 (총 {total_posts_saved_in_forum}개 저장)")

    async def check_tor_connection(self) -> bool:
        """(public) Tor 연결을 확인합니다."""
        logging.info("Tor 네트워크 연결 확인 중...")
        ip_check_url = 'https://check.torproject.org/api/ip'
        try:
            response = await self.client.get(ip_check_url, timeout=15)
            response.raise_for_status()
            response_data = response.json()
            
            if response_data.get("IsTor"):
                logging.info(f"Tor를 통해 성공적으로 연결되었습니다. (IP: {response_data.get('IP')})")
                return True
            else:
                logging.error("경고: Tor를 통해 연결되지 않았습니다. 스크립트를 종료합니다.")
                return False
        except Exception as e:
            logging.error(f"Tor 연결 확인 실패: {e}")
            logging.error(f"Tor Browser가 실행 중인지, 프록시 설정({TOR_PROXY})이 올바른지 확인하세요.")
            return False

# --- 2. Command (커맨드) ---

class AsyncCommand(ABC):
    """(Abstract) 비동기 커맨드 인터페이스"""
    @abstractmethod
    async def execute(self):
        pass
    
    # 이어하기 로직을 위해 커맨드가 자신의 식별자(URI)를 반환하도록 함
    @abstractmethod
    def get_identifier(self) -> str:
        pass

class CrawlForumCommand(AsyncCommand):
    """(Concrete) 포럼 크롤링을 요청하는 커맨드"""
    def __init__(self, 
                 crawler: Crawler, 
                 forum_display_name: str, 
                 forum_uri: str, 
                 start_page: int = 1):
        self.crawler = crawler  # Receiver
        self.forum_display_name = forum_display_name
        self.forum_uri = forum_uri
        self.start_page = start_page # 이어하기를 위한 시작 페이지

    async def execute(self):
        await self.crawler.crawl_forum(
            self.forum_display_name,
            self.forum_uri,
            self.start_page
        )
    
    def get_identifier(self) -> str:
        return self.forum_uri
    
    # 이어하기 시 시작 페이지를 업데이트하기 위한 메서드
    def set_start_page(self, page_num: int):
        self.start_page = page_num

class CheckTorCommand(AsyncCommand):
    """(Concrete) Tor 연결을 확인하는 커맨드"""
    def __init__(self, crawler: Crawler):
        self.crawler = crawler
        self._success = False

    async def execute(self):
        self._success = await self.crawler.check_tor_connection()

    def was_successful(self) -> bool:
        return self._success
    
    def get_identifier(self) -> str:
        return "check_tor" # 고유 식별자

# --- 3. Invoker (호출자) ---

class CrawlManager:
    """커맨드 큐를 관리하고 실행하는 Invoker 클래스"""
    def __init__(self):
        self.command_queue: List[AsyncCommand] = []

    def register(self, command: AsyncCommand):
        """실행할 커맨드를 큐에 등록합니다."""
        self.command_queue.append(command)

    async def run(self):
        """
        큐에 등록된 모든 커맨드를 실행합니다.
        '이어하기' 로직을 포함합니다.
        """
        start_forum_uri, start_page_num = load_crawl_state()
        
        start_index = 0
        
        if start_forum_uri:
            try:
                # 1. 중단된 포럼의 인덱스 찾기
                start_index = [cmd.get_identifier() for cmd in self.command_queue].index(start_forum_uri)
                logging.info(f"'{start_forum_uri}' 포럼부터 다시 시작합니다.")
                
                # 2. 해당 커맨드의 시작 페이지 설정
                command_to_resume = self.command_queue[start_index]
                if isinstance(command_to_resume, CrawlForumCommand):
                    command_to_resume.set_start_page(start_page_num)
                    
            except ValueError:
                logging.warning(f"상태 파일의 포럼 '{start_forum_uri}'을(를) 찾을 수 없습니다. 처음부터 시작합니다.")

        # 3. 작업 큐 실행 (Tor 확인은 매번 실행)
        try:
            for command in self.command_queue:
                if isinstance(command, CheckTorCommand):
                    await command.execute()
                    if not command.was_successful():
                        logging.error("Tor 연결 실패. 크롤링을 중단합니다.")
                        return # Tor 연결 실패 시 즉시 중단
                else:
                    # 큐의 인덱스가 start_index보다 뒤에 있을 때만 실행
                    current_cmd_index = self.command_queue.index(command)
                    if current_cmd_index >= start_index:
                        await command.execute()
            
            # 4. 모든 작업 완료 후 상태 파일 삭제
            clear_crawl_state()

        except CriticalCrawlStop as e:
            logging.critical(f"크롤링 작업 중단 (치명적 오류): {e}")
            logging.info("현재 진행 상황이 저장되었습니다. 네트워크 안정 후 스크립트를 다시 시작하세요.")
        

# --- 4. Client (클라이언트) ---

async def main():
    setup_csv_limit()
    
    csv_path = Path(OUTPUT_DIR) / OUTPUT_FILENAME
    crawled_post_urls = load_existing_urls_from_csv(csv_path)

    async with httpx.AsyncClient(transport=HTTPX_TRANSPORT) as client:
        
        # 1. Receiver 생성
        crawler = Crawler(client, crawled_post_urls)
        
        # 2. Invoker 생성
        manager = CrawlManager()

        # 3. Commands 생성 및 등록
        
        # 3-1. Tor 연결 확인 커맨드
        manager.register(CheckTorCommand(crawler))

        # 3-2. 포럼 크롤링 커맨드
        for forum_display_name, forum_uri in TARGET_FORUMS.items():
            command = CrawlForumCommand(
                crawler=crawler,
                forum_display_name=forum_display_name,
                forum_uri=forum_uri
            )
            manager.register(command)
        
        # 4. Invoker 실행
        await manager.run()

    # --- 최종 결과는 Receiver(crawler)의 상태에서 가져옴 ---
    logging.info(f"\n{'='*50}\n모든 크롤링 작업이 완료되었습니다.\n{'='*50}")
    logging.info(f"총 저장된 새 게시물 수 : {crawler.total_posts_saved}")
    logging.info(f"총 게시물 처리/파싱 에러 횟수 : {crawler.total_errors}")
    logging.info(f"총 400, 500 HTTP 에러 횟수 : {crawler.total_http_errors}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("작업이 사용자에 의해 중단되었습니다. (현재 상태 저장됨)")
    except Exception as e:
        logging.critical(f"예상치 못한 오류로 프로그램이 종료됩니다: {e} (현재 상태 저장됨)")