import requests
import platform
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# 예시 사이트 크롤링. DragonForce
URL = "http://z3wqggtxft7id3ibr7srivv5gjof5fwg76slewnzwwakjuf3nlhukdid.onion"

# OS에 따라 Tor 포트를 자동으로 설정
PORT = "9150" if platform.system() == "Windows" else "9050"

# Tor 네트워크 접속을 위한 프록시 설정
PROXIES = {
    "http": f"socks5h://127.0.0.1:{PORT}",
    "https": f"socks5h://127.0.0.1:{PORT}"
}


def get_tor_session() -> requests.Session:
    """OS에 맞게 설정된 Tor 프록시를 사용하는 requests 세션 객체를 생성합니다."""
    session = requests.session()
    session.proxies = PROXIES
    return session


def fetch_page_data(session: requests.Session, page: int, base_url: str) -> dict | None:
    """지정된 페이지의 API 데이터를 가져옵니다."""
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
    """바이트 단위의 파일 크기를 사람이 읽기 쉬운 형태로 변환합니다."""
    if size_bytes == 0: return "0 B"
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size_bytes >= power and n < len(power_labels) - 1:
        size_bytes /= power
        n += 1
    return f"{size_bytes:.2f} {power_labels[n]}"


def parse_victim_data(publication: dict, page: int) -> dict:
    """개별 유출 정보를 파싱하여 딕셔너리로 반환합니다."""
    now_utc = datetime.now(timezone.utc)
    now_kst = now_utc.astimezone(ZoneInfo("Asia/Seoul"))

    created_at_str = publication.get('created_at', '')
    posted_at_utc = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')) if created_at_str else None
    
    timer_str = publication.get('timer_publication', '')
    is_published = False
    time_until_publication = "N/A"
    
    if timer_str:
        publication_time_utc = datetime.fromisoformat(timer_str.replace('Z', '+00:00'))
        if now_utc >= publication_time_utc:
            is_published = True
        else:
            time_left = publication_time_utc - now_utc
            days = time_left.days
            hours, remainder = divmod(time_left.seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            time_until_publication = f"{days}d {hours}h {minutes}m left"

    details_url = f"{URL}/api/guest/blog/post?post_uuid={publication.get('uuid', '')}"
    publication_url = f"{URL}/api/guest/blog/post/files?post_uuid={publication.get('uuid', '')}"
    
    return {
        "Current Time (KST)": now_kst.strftime('%Y-%m-%d %H:%M:%S'),
        "Current Time (UTC)": now_utc.strftime('%Y-%m-%d %H:%M:%S'),
        "Page Number": page,
        "Company Name": publication.get('name', 'N/A').strip(),
        "Website": publication.get('website', 'N/A').strip(),
        "Address": publication.get('address', 'N/A').strip(),
        "Data Size": format_bytes(publication.get('weight', 0)),
        "Description": publication.get('description', 'N/A').strip(),
        "Is Published": 'Yes' if is_published else 'No',
        "Time Until Publication": time_until_publication,
        "Publication URL": publication_url if is_published else "Not yet published",
        "Posted Time (UTC)": posted_at_utc.strftime('%Y-%m-%d %H:%M:%S') if posted_at_utc else 'N/A',
        "Details URL": details_url
    }


def print_victim_details(victim_dict: dict):
    print("--------------------------------------------------")
    for key, value in victim_dict.items():
        if key == "Description" and len(str(value)) > 100:
            value = f"{str(value)[:100]}..."
        print(f"  [+] {key}: {value}")
    print("--------------------------------------------------\n")


def main():
    print("### TimeZone 에러 발생시 conda 환경 사용 혹은 pip install tzdata 실행")
    all_victims = []

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
                parsed_info = parse_victim_data(item, page_num)
                all_victims.append(parsed_info)
    
    print(f"### 데이터 처리 완료! 총 **{len(all_victims)}** 개의 피해 기업 정보를 리스트에 저장했습니다.")
    
    # 결과 확인 (최신 5개)
    print("### 수집된 데이터 샘플 (최신 5개)")
    for victim_data in all_victims[:5]:
        print_victim_details(victim_data)


if __name__ == "__main__":
    main()
        