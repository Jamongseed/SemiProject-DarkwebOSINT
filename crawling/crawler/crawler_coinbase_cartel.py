import requests
from typing import Optional
from config import PORT, PROXIES


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