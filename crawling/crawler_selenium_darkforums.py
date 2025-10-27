# pip install blinker==1.7.0 brotli psutil
import os
import platform
import subprocess
import psutil
import time
import functools
import brotli
import json
import csv
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
# from seleniumwire import webdriver
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

# 크롤링할 대상 포럼을 관리하는 딕셔너리입니다.
# 여기에 명시된 게시판만 순서대로 방문하여 크롤링합니다.
TARGET_FORUMS = {
    "Databases": "Forum-Databases",
    "Stealer Logs": "Forum-Stealer-Logs",
    "Games": "Forum-Games",
    "Source Codes": "Forum-Source-Codes",
    "Other Leaks": "Forum-Other-Leaks",
    "Combolists": "Forum-Combolists",
    "Doxes": "Forum-Doxes",
    "HackTheBox & TryHackMe": "Forum-HackTheBox-TryHackMe",
    "Cracked Accounts": "Forum-Cracked-Accounts",
    "Cracked Tools": "Forum-Cracked-Tools"
}

UNIFIED_HEADERS = [
    "source", "record_type", "id", "company", "website", "country", "address",
    "size_bytes", "size_gib", "is_published", "time_until_publication",
    "posted_at_utc", "crawled_at_utc", "crawled_at_kst",
    "ransomware_group", "discovery_date", "estimated_attack_date",
    "details_url", "description", "files_api_present"
]

# 접속할 목표 웹사이트
# TARGET_URL = "https://check.torproject.org"
TARGET_URL = "https://darkforums.st"
TARGET_TITLE = "darkforums"
# Tor 프록시 주소 (Tor Browser 기본 설정)
TOR_PROXY = f"127.0.0.1:{"9150" if platform.system() == "Windows" else "9050"}"
# TARGET_RESPONSE_URLS = [
#     "https://darkforums.st/", "https://darkforums.st/Forum-Databases"
# ]
TARGET_CONTENT_TYPE = "text/html; charset=UTF-8"
TARGET_CONTENT_ENCODING = "br"


def convert_to_iso_utc(date_str: str) -> str:
    """
    'YY-MM-DD, HH:MM AM/PM' 형식의 문자열을
    'YYYY-MM-DDTHH:MM:SS.ffffff+00:00' (ISO 8601 UTC) 형식으로 변환합니다.

    입력 시간은 UTC로 간주됩니다.
    """
    
    # 1. 입력 문자열을 파싱하기 위한 형식 지정
    # %y: 2자리 연도, %m: 월, %d: 일
    # %I: 12시간제 시간, %M: 분, %p: AM/PM
    input_format = "%y-%m-%d, %I:%M %p"
    
    # 2. 문자열을 naive datetime 객체로 파싱
    # (시간대 정보가 없는 객체)
    dt_naive = datetime.strptime(date_str, input_format)
    
    # 3. naive 객체에 UTC 시간대 정보를 할당하여 aware 객체로 만듦
    dt_aware_utc = dt_naive.replace(tzinfo=timezone.utc)
    
    # 4. 원하는 출력 형식으로 포맷팅
    # .isoformat()는 마이크로초가 0일 때 생략할 수 있으므로,
    # 예시 형식과 정확히 맞추기 위해 strftime을 사용하여 .000000을 강제로 표시합니다.
    # %Y-%m-%dT%H:%M:%S.%f+00:00
    output_format = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    output_string = dt_aware_utc.strftime(output_format)
    
    return output_string


def verification_solve():
    """인증 페이지가 감지되면 사용자 입력을 기다립니다."""
    print("\n" + "="*60)
    print("   인증 페이지가 감지되었습니다.")
    print("   브라우저에서 직접 보안 문자를 해결해주세요.")
    input("   완료되었으면, 이 창에서 Enter 키를 눌러주세요...")
    print("   크롤링을 계속합니다.")
    print("="*60 + "\n")
    # time.sleep(1) # 해결 후 페이지 로딩 대기

def check_for_verification(driver: WebDriver):
    """페이지 로드 후 인증 페이지가 나타났는지 확인하고 처리합니다."""
    try:
        # time.sleep(1)
        if "Verification Requested" in driver.find_element(By.CLASS_NAME, "accent").text:
            verification_solve()
    except NoSuchElementException:
        # 인증 없이 정상 접속
        pass
    except Exception as e:
        print(f"   인증 페이지 검사중 오류가 감지되었습니다.")
        print(e)
        pass


def handle_ddos_after_action(func):
    """
    메서드 실행 후 DDOS 페이지를 자동으로 확인하는 데코레이터입니다.
    이 데코레이터는 메서드의 첫 번째 인자가 self(클래스 인스턴스)이고,
    self가 'driver' 속성을 가지고 있다고 가정합니다.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # 원본 메서드 실행 (e.g., click, back, get)
        result = func(self, *args, **kwargs)
        # 실행 직후 DDOS 검사
        check_for_verification(self.driver)
        return result
    return wrapper


class SafeWebDriver:
    """
    기존 Selenium WebDriver를 감싸서, 페이지 이동 메서드에
    인증 자동 확인 기능을 프록시로 추가한 클래스입니다.
    
    'with' 구문을 지원하여 드라이버의 생성과 종료(quit)를 자동으로 관리합니다.
    """
    def __init__(self, service: ChromeService, options: ChromeOptions):
        print("SafeDriver: 드라이버 생성을 시작합니다...")
        self.driver = webdriver.Chrome(service=service, options=options)

        try:
            self.driver_pids = set()

            driver_pid = self.driver.service.process.pid
            print(f"SafeDriver: 크롬 드라이버 PID: {driver_pid}")
            self.driver_pids.add(driver_pid)

            parent = psutil.Process(driver_pid)
            children = parent.children(recursive=True)
            for child in children:
                self.driver_pids.add(child.pid)
                print(f"SafeDriver: 자식 크롬 브라우저 PID: {child.pid}")

            print(f"SafeDriver: 크롬 드라이버와 자식 프로세스의 PID를 저장하였습니다. 총 {len(self.driver_pids)}개")
        except AttributeError:
            print("SafeDriver: 경고 - chromedriver PID를 찾을 수 없습니다.")

        print("SafeDriver: 드라이버 초기 설정 적용 중...")
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.implicitly_wait(1)
        self.driver.set_page_load_timeout(60)
        print("SafeDriver: 드라이버 생성 및 초기화 완료.")

    def __enter__(self):
        """'with' 블록 진입 시 self(SafeWebDriver 인스턴스)를 반환합니다."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """'with' 블록 종료 시(오류 발생 여부와 무관) 드라이버를 종료합니다."""
        print("\nSafeDriver: __exit__ 호출. 브라우저를 종료합니다.")
        if not self.driver:
            return

        try:
            self.driver.quit()
            print("SafeDriver: 드라이버가 성공적으로 종료되었습니다.")
            
            if platform.system() == "Windows" and len(self.driver_pids):
                print("SafeDriver: 윈도우에서 브라우저 프로세스 강제 종료를 시도합니다.")
                self._force_kill_windows_process_tree()
            else:
                print("SafeDriver: (비-Windows) 강제 종료를 건너뜁니다.")   
        except Exception as e:
            print(f"SafeDriver: 정상 종료(quit) 실패: {e}")      


    def _force_kill_windows_process_tree(self):
        if not len(self.driver_pids):
            print("SafeDriver: PID가 저장되지 않아 강제 종료를 건너뜁니다.")
            return

        print(f"SafeDriver: Windows에서 드라이버 및 모든 자식 프로세스 강제 종료 시도...")
        for pid in self.driver_pids:
            try:
                # /T 스위치: 지정된 프로세스(chromedriver)와 모든 자식 프로세스(chrome)를 함께 종료
                # /F 스위치: 강제 종료
                # creationflags=subprocess.CREATE_NO_WINDOW: cmd 창이 깜박이지 않게 함
                subprocess.run(
                        ["taskkill", "/F", "/PID", str(pid), "/T"],
                        check=True,
                        capture_output=True,
                        creationflags=subprocess.CREATE_NO_WINDOW
                    )
                print(f"SafeDriver: PID {pid} 및 자식 프로세스가 강제 종료되었습니다.")
            except subprocess.CalledProcessError as kill_e:
                # '프로세스를 찾을 수 없습니다' (이미 종료된 경우)는 오류가 아님
                error_message = kill_e.stderr.decode('cp949', 'ignore').lower()
                print(f"SafeDriver: 강제 종료 중 오류/경고: {error_message}")
            except FileNotFoundError:
                print("SafeDriver: 'taskkill' 명령을 찾을 수 없습니다. 강제 종료 실패.")
            except Exception as e:
                print(f"SafeDriver: 알 수 없는 에러 발생 : {e} \n강제 종료 실패.")


    @handle_ddos_after_action
    def get(self, url: str):
        self.driver.get(url)

    @handle_ddos_after_action
    def back(self):
        self.driver.back()

    @handle_ddos_after_action
    def click(self, element: WebElement):
        try:
            element.click()
        except Exception as e:
            print(f"클릭 중 오류 발생: {e}")

    @handle_ddos_after_action
    def find_element(self, by, value):
        return self.driver.find_element(by, value)

    @handle_ddos_after_action
    def find_elements(self, by, value):
        return self.driver.find_elements(by, value)
        
    @handle_ddos_after_action
    def execute_script(self, script, *args):
        return self.driver.execute_script(script, *args)
    
    @property
    @handle_ddos_after_action
    def current_url(self):
        return self.driver.current_url

    @property
    @handle_ddos_after_action
    def title(self):
        return self.driver.title

    # --- selenium-wire 호환성을 위한 속성 (필요시) ---
    @property
    def requests(self):
        """selenium-wire의 'requests' 속성에 접근합니다."""
        if hasattr(self.driver, 'requests'):
            return self.driver.requests
        # selenium-wire가 아니면 빈 리스트 반환
        return []
    
    # WebDriver의 다른 모든 속성/메서드에 직접 접근할 수 있도록 위임합니다.
    def __getattr__(self, name):
        return getattr(self.driver, name)


def crawl_post_details(safe_driver: SafeWebDriver):
    """
    게시물 상세 페이지에서 동적 ID에 의존하지 않고 안정적으로 정보를 크롤링합니다.
    
    :param driver: 현재 게시물 상세 페이지를 제어 중인 SafeWebDriver 객체
    :return: 크롤링된 데이터가 담긴 딕셔너리
    """
    print("\n   [게시물 상세 정보 크롤링 시작]")
    details = {}

    url = safe_driver.current_url
    details['url'] = url

    try:
        # 항상 페이지의 첫 번째 게시물을 기준으로 삼습니다. (ID에 의존하지 않음)
        first_post = safe_driver.find_element(By.CSS_SELECTOR, "#posts > .post.classic:first-of-type")
    except NoSuchElementException:
        print("   오류: 게시물 컨테이너를 찾을 수 없습니다.")
        return None

    # 헬퍼 함수: 기준 요소(first_post) 내에서 안전하게 텍스트를 가져옵니다.
    def get_text_safely(base_element, selector):
        try:
            return base_element.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return "N/A"

    # --- 게시물 정보 ---
    details['title'] = get_text_safely(safe_driver, ".thread-info__name") # 제목은 게시물 외부에 있음
    details['author'] = get_text_safely(first_post, ".post_user-profile a")
    
    # 작성일은 수정 정보가 포함될 수 있으므로, 전체 텍스트에서 분리합니다.
    full_date_text = get_text_safely(first_post, ".post_date")
    details['posted_date'] = full_date_text.split('\n')[0] # 첫 줄이 항상 작성일

    # 수정 날짜와 ID는 'post_edit' 클래스 존재 여부로 확인
    try:
        edit_info_text = first_post.find_element(By.CSS_SELECTOR, ".post_edit em").text
        # 예: (This post was last modified: 25-08-25, 07:00 PM by VexDB.)
        # 실제 텍스트 형식에 맞춰 파싱 로직을 조정해야 할 수 있습니다.
        details['last_edited_info'] = edit_info_text
    except NoSuchElementException:
        details['last_edited_info'] = "N/A"

    # --- 글쓴이 정보 ---
    details['author_rank'] = get_text_safely(first_post, ".post_user-title")
    details['reputation'] = get_text_safely(first_post, "strong[class^='reputation_'") or "0"
    
    # post_stats-bit 그룹을 순회하며 안정적으로 데이터 추출
    author_stats = {}
    try:
        stats_elements = first_post.find_elements(By.CSS_SELECTOR, ".post_author-stats .post_stats-bit.group")
        for stat_element in stats_elements:
            spans = stat_element.find_elements(By.TAG_NAME, "span")
            if len(spans) == 2:
                key = spans[0].text.strip()
                value = spans[1].text.strip()
                if key: # 레이블이 있는 경우에만 추가
                    author_stats[key] = value
    except NoSuchElementException:
        pass # 통계 정보가 없어도 오류 없음

    details['posts_count'] = author_stats.get("Posts", "N/A")
    details['threads_count'] = author_stats.get("Threads", "N/A")
    details['join_date'] = author_stats.get("Joined", "N/A")
    
    # --- 본문 내용 ---
    details['main_content'] = get_text_safely(first_post, ".post_body")

    # --- 크롤링 결과 출력 ---
    print("   ---------------------------------")
    print(f"   - Url: {details['url']}")
    print(f"   - 제목: {details['title']}")
    print(f"   - 글쓴이: {details['author']} (등급: {details['author_rank']})")
    print(f"   - 작성일: {details['posted_date']}")
    print(f"   - 수정 정보: {details['last_edited_info']}")
    print(f"   - 평판: {details['reputation']}")
    print(f"   - 포스트/쓰레드: {details['posts_count']} / {details['threads_count']}")
    print(f"   - 가입일: {details['join_date']}")
    print(f"   - 본문 (일부): {details['main_content'][:100]}...")
    print("   ---------------------------------")
    print("   [게시물 상세 정보 크롤링 완료]\n")
    
    return details


def crawl_current_page_posts(driver: SafeWebDriver):
    """현재 페이지의 모든 게시물을 순회하며 크롤링합니다."""
    print(f"\n--- '{driver.title}' 페이지의 모든 게시물 크롤링 시작 ---")
    
    # 게시물 목록을 찾는 CSS 선택자입니다.
    post_selector = "span[id^='tid_'] a"
    post_elements = driver.find_elements(By.CSS_SELECTOR, post_selector)
    post_count = len(post_elements)

    if post_count == 0:
        print("게시물을 찾을 수 없습니다."); return

    all_crawled_data = []
    print(f"\n--- {driver.title} 페이지에서 {post_count}개의 게시물 발견 ---")
    for i in range(post_count):
        try:
            # StaleElementReferenceException을 피하기 위해 매번 요소를 다시 찾습니다.
            posts = driver.find_elements(By.CSS_SELECTOR, post_selector)
            if i >= len(posts): break
            
            post_title = posts[i].text.strip() or "[제목 없음]"
            print(f"({i + 1}/{post_count}) '{post_title}' 게시물로 이동.")
            
            # 게시물 클릭
            driver.click(posts[i])

            print(f"({i + 1}/{post_count}) '{post_title}' 게시물로 이동 완료.")
            
            crawled_data = crawl_post_details(driver)
            if crawled_data:
                all_crawled_data.append(crawled_data)

            # save_network_data(driver)
            
            # 목록으로 복귀
            driver.back()

        except StaleElementReferenceException:
            print("페이지 요소를 잃었습니다. 목록을 다시 로드하여 계속합니다.")
            break # 현재 페이지의 루프를 중단하고 다음 페이지로 넘어갑니다.

        except Exception as e:
            print(f"게시물 처리 중 예상치 못한 오류: {e}")
            try:
                driver.back() # 목록 페이지로 복귀 시도
            except Exception as back_e:
                print(f"뒤로가기 실패. 루프 중단: {back_e}")
                break # 뒤로가기도 실패하면 루프 중단
            continue
    print(f"--- 현재 페이지의 {len(all_crawled_data)}개 게시물 크롤링 완료 ---")
    return all_crawled_data


def crawl_entire_forum(driver: SafeWebDriver):
    """한 게시판의 모든 페이지를 순회하며 게시글을 크롤링합니다."""
    print(f"\n{'='*25}\n게시판 '{driver.title}' 전체 크롤링 시작\n{'='*25}")
    
    # 마지막 페이지 번호 확인
    try:
        last_page_element = driver.find_element(By.CSS_SELECTOR, "a.pagination_last")
        last_page = int(last_page_element.text.strip())
    except (NoSuchElementException, ValueError):
        last_page = 1 # 페이지네이션이 없으면 1페이지로 간주
    
    print(f"총 {last_page} 페이지의 게시판입니다.")
    current_page = 1

    all_page_crawled_data = []
    
    while current_page <= last_page:
        print(f"\n{'='*20} {current_page} / {last_page} 페이지 처리 시작 {'='*20}")
        all_page_crawled_data.extend(crawl_current_page_posts(driver))
        
        if current_page >= last_page:
            print("마지막 페이지까지 크롤링 완료.")
            break

        # 다음 페이지로 이동
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "a.pagination_next")
            print("다음 페이지로 이동합니다.")
            driver.click(next_button)
            current_page += 1
        except NoSuchElementException:
            print("다음 페이지 버튼을 찾을 수 없어 해당 게시판 크롤링을 종료합니다.")
            break

    kst_timezone = ZoneInfo("Asia/Seoul")
    crawl_time_utc = datetime.now(timezone.utc).isoformat()
    crawl_time_kst = datetime.now(kst_timezone).isoformat()
    
    return {
        "crawled_at_utc": crawl_time_utc,
        "crawled_at_kst": crawl_time_kst,
        "posts": all_page_crawled_data
    }

def navigate_and_crawl_forums(driver: SafeWebDriver, main_page_url: str):
    """메인 페이지에서 시작하여 지정된 모든 포럼을 순회하며 크롤링합니다."""
    for forum_name, forum_uri in TARGET_FORUMS.items():
        print(f"\n{'#'*60}")
        print(f"  게시판 '{forum_name}' ({forum_uri}) 크롤링을 시작합니다.")
        print(f"{'#'*60}")

        # 안정성을 위해 항상 메인 페이지에서 시작합니다.
        driver.get(main_page_url)

        try:
            # href 속성값으로 게시판 링크를 정확하게 찾습니다.
            forum_link_selector = f"a[href='{forum_uri}']"
            forum_link = driver.find_element(By.CSS_SELECTOR, forum_link_selector)
            
            print(f"'{forum_name}' 게시판 링크를 찾았습니다. 이동합니다.")
            driver.click(forum_link)
            
            # 게시판 페이지로 이동했으므로, 해당 게시판 전체를 크롤링하는 함수를 호출합니다.
            crawl_entire_forum(driver)

        except NoSuchElementException:
            print(f"오류: 메인 페이지에서 '{forum_name}' 게시판 링크를 찾을 수 없습니다. 다음 게시판으로 넘어갑니다.")
            continue # 다음 게시판으로
        except Exception as e:
            print(f"'{forum_name}' 게시판 처리 중 예상치 못한 오류 발생: {e}")
            continue


def save_unified_csv_ransomware(results: dict, forum: str, out_dir: str = "outputs",
                                filename: str = "dark_forums_"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    end = "_unified.csv"
    # dark_forums_databases_unified.csv
    path = out / (filename + forum.lower() + end)

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNIFIED_HEADERS)
        w.writeheader()

        crawled_at_utc = results.get("crawled_at_utc", "")
        crawled_at_kst = results.get("crawled_at_kst", "")
        for v in results.get("posts", []):
            # rid에 url 추가
            rid = v.get("url") or f'{v.get("author","")}|{v.get("ransomware_group","")}|{v.get("discovery_date","")}'

            # 특정 기업을 대상으로 유출을 하는게 아니라 기업, 랜섬웨어 그룹을 지정하기 힘듬
            w.writerow({
                "source": "darkforums.st",
                "record_type": "leak_post",
                "id": rid,
                "company": "",
                "website": "",
                "country": "",
                "address": "",
                "size_bytes": "",
                "size_gib": "",
                "is_published": "",
                "time_until_publication": "",
                "posted_at_utc": convert_to_iso_utc(v.get("posted_date", "")),
                "crawled_at_utc": crawled_at_utc,
                "crawled_at_kst": crawled_at_kst,
                "ransomware_group": "",
                "discovery_date": "",
                "estimated_attack_date": "",
                "details_url": "",
                "description": v.get("main_content", ""),
                "files_api_present": "",
                "title": v.get("title", ""),
                "author": v.get("author", ""),
                "last_edited_info": v.get("last_edited_info", ""),
                "author_rank": v.get("author_rank", ""),
                "reputation": v.get("reputation", ""),
            })
            
    print(f" - 통합(덮어쓰기): {path.resolve()}")


def save_network_data(driver: SafeWebDriver):
    directory = 'network_log'
    if not os.path.exists(directory):
        os.makedirs(directory) # 디렉토리가 없으면 생성

    # 최종 파일 경로
    filename = f"{TARGET_TITLE}_{datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d_%H-%M-%S')}.json"
    file_path = os.path.join(directory, filename)

    print(f"로그가 다음 파일에 저장됩니다: {file_path}")
    json_list = []
    
    # 캡처된 요청들을 확인합니다.
    print("\n--- 네트워크 트래픽 로그 (selenium-wire) ---")
    if not driver.requests:
        print("캡처된 요청이 없습니다.")
    else:
    # --- 방문 페이지 순회 ---
        for i, request in enumerate(driver.requests):
            if (request.response and 
                    # request.url in TARGET_RESPONSE_URLS and
                    request.response.headers.get('content-type', '') == 'text/html; charset=UTF-8' and
                    request.response.headers.get('content-encoding', '') == 'br'):
                print(f"[{i+1}] URL: {request.url}")
                print(f"    Request Headers: {request.headers}")
                print(f"    Status Code: {request.response.status_code}")
                print(f"    Headers: {request.response.headers}")
                # print(f"    Body: {request.response.body.decode('utf-8', 'ignore')[:100]}...") # 응답 본문 (필요 시)
                print("-" * 20)
                body = None
                try:
                    body = brotli.decompress(request.response.body).decode('utf-8')
                            
                except Exception as e:
                    print(e)
                    
                # print(f"body : {body[:100]}")

                log_entry = {
                    "index": i + 1,
                    "time_utc": datetime.now(timezone.utc).isoformat(),
                    "time_kst": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
                    "url": request.url,
                    "request_headers": dict(request.headers), # 헤더를 dict로 변환
                    "status_code": request.response.status_code,
                    "response_headers": dict(request.response.headers), # 헤더를 dict로 변환
                    "body": body if body else ""
                }
                json_list.append(log_entry)
                # with open(f'{file_path}_{i+1}_body.br', 'wb') as f:
                #     f.write(request.response.body)
                # Forum-Databases
                title = os.path.basename(urlparse(request.url).path)
                full_path_file = f"{os.path.join(directory, title)}.html"
                

                # if not os.path.exists(full_path_file):
                with open(full_path_file, 'w', encoding='utf-8') as html_file:
                    html_file.write(body)


                
    with open(f'{file_path}', 'w', encoding='utf-8') as f:
        f.write(json.dumps(json_list, ensure_ascii=False))


if __name__ == "__main__":
    """
    SafeWebDriver의 'with' 구문을 사용하여 드라이버의 생명주기를 관리합니다.
    """

    # selenium-wire 옵션 설정
    # selenium-wire가 사용할 업스트림 프록시로 Tor를 지정합니다.
    # sw_options = {
    #     'proxy': {
    #         'https': f'socks5h://{TOR_PROXY}',
    #         'http': f'socks5h://{TOR_PROXY}',
    #         'no_proxy': 'localhost,127.0.0.1'
    #     }
    # }

    try:
        print("크롤링 프로세스를 시작합니다.")

        service = Service(ChromeDriverManager().install())
        print(f"ChromeDriverManager settings: {service.__repr__()}")
        
        # Chrome 옵션
        # options.add_argument("--headless")  # 필요 시 브라우저 창을 띄우지 않음
        options = webdriver.ChromeOptions()
        # options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        # 'untrusted enterprise roots' 경고 무시
        options.add_argument('--ignore-certificate-errors-spki-list')
        options.add_argument('--ignore-ssl-errors')

        options.add_argument("disable-blink-features=AutomationControlled")  # 자동화 탐지 방지
        options.add_experimental_option("excludeSwitches", ["enable-automation"])  # 자동화 표시 제거
        options.add_experimental_option('useAutomationExtension', False)  # 자동화 확장 기능 사용 안 함
        
        # (selenium-wire 사용 시)
        # real_driver = seleniumwire.webdriver.Chrome(service=service, options=options, seleniumwire_options=sw_options)
        
        # 3. 'with' 구문으로 SafeWebDriver 생성 및 사용
        #    SafeWebDriver.__init__이 service와 options를 받아 드라이버를 생성합니다.
        with SafeWebDriver(service=service, options=options) as safe_driver:
            
            # 4. 메인 크롤링 작업을 별도의 try...except로 감쌉니다.
            try:
                navigate_and_crawl_forums(safe_driver, TARGET_URL)
            
            except KeyboardInterrupt:
                # 5. 사용자가 Ctrl+C를 누르면 여기가 실행됩니다.
                print("\n\n[중단 요청] 사용자가 (Ctrl+C)를 눌렀습니다. 크롤링을 중단합니다...")
                print("브라우저 종료를 시도합니다.")
                # 'pass'를 실행하면 'try...except' 블록을 빠져나갑니다.
                # 그러면 'with' 구문이 정상적으로 종료되면서
                # SafeWebDriver.__exit__ 메서드가 자동으로 호출됩니다.
                pass

    except Exception as e:
        print(f"\n메인 스크립트 실행 중 예상치 못한 오류 발생: {e}")
        import traceback
        traceback.print_exc() # 상세한 오류 스택 트레이스 출력
    
    print("\n모든 작업이 완료되었습니다. 프로그램이 종료됩니다.")