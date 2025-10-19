from pprint import pprint
from crawler.crawler_coinbase_cartel import get_tor_response
from parser.parser_coinbase_cartel import parse_victims_from_html
from config import URL_COINBASE_CARTEL


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