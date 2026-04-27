# SemiProject-DarkwebOSINT

다크웹 및 랜섬웨어 유출 사이트에서 피해 기업 정보를 수집하고, 수집 결과를 통합 CSV 스키마로 정규화한 뒤 Streamlit 대시보드에서 분석하는 OSINT 프로젝트입니다.

본 프로젝트는 Tor 프록시 기반 `.onion` 크롤링, 공개 랜섬웨어 인텔리전스 사이트 수집, 다크웹 포럼 게시글 수집, 통합 CSV 생성, 위험도 기반 시각화 대시보드로 구성되어 있습니다.

## Overview

- Tor SOCKS5 프록시 기반 다크웹 사이트 접근
- DragonForce / Coinbase Cartel 랜섬웨어 유출 사이트 크롤링
- ransomware.live 공개 피해자 정보 수집
- DarkForums 게시판/게시글 크롤링
- 수집 결과를 통합 CSV 스키마로 저장
- Streamlit 기반 Darkweb Leak Dashboard 제공
- 심각도 점수, 국가 추정, 데이터 노출 단계, 행위자 정보 등을 기준으로 분석 가능

<img width="716" height="470" alt="image" src="https://github.com/user-attachments/assets/3252fda3-edd8-4a64-ab13-f30e254f7850" />
<img width="985" height="465" alt="image" src="https://github.com/user-attachments/assets/762947b0-3945-410b-ad0a-e9bef9807c5c" />
<img width="1052" height="374" alt="image" src="https://github.com/user-attachments/assets/70256c62-e31e-4488-8bd0-c20f4e9fa921" />


## Tech Stack

### Crawling

- Python
- requests
- PySocks
- BeautifulSoup4
- httpx
- lxml
- Selenium
- webdriver-manager
- psutil
- brotli

### Data Analysis / Dashboard

- Streamlit
- pandas
- numpy
- altair
- plotly
- tldextract

### Cloud / Utility

- boto3
- botocore
- AWS IAM Access Key rotation utility

## Quick Start

### 0) Requirements

- Python 3.10+
- Tor Browser 또는 Tor 서비스
- Chrome Browser
- ChromeDriver 또는 webdriver-manager
- pip

Tor 프록시 포트는 OS에 따라 자동으로 다르게 사용됩니다.

```bash
# Windows Tor Browser 기본 포트
127.0.0.1:9150

# Linux / macOS Tor 서비스 기본 포트
127.0.0.1:9050
```

### 1) Clone Repository

```bash
git clone https://github.com/<your-id>/SemiProject-DarkwebOSINT.git
cd SemiProject-DarkwebOSINT
```

### 2) Create Virtual Environment

```bash
cd crawling

python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux / macOS
source .venv/bin/activate
```

### 3) Install Dependencies

현재 레포에는 `.venv`가 포함되어 있을 수 있지만, GitHub 업로드 시에는 `.venv`를 제외하고 `requirements.txt` 기반으로 의존성을 관리하는 것을 권장합니다.

```bash
pip install requests pysocks beautifulsoup4 lxml httpx selenium webdriver-manager psutil brotli streamlit pandas numpy altair plotly tldextract boto3 botocore tzdata
```

권장 `requirements.txt` 예시는 아래와 같습니다.

```txt
requests
PySocks
beautifulsoup4
lxml
httpx
selenium
webdriver-manager
psutil
brotli
streamlit
pandas
numpy
altair
plotly
tldextract
boto3
botocore
tzdata
```

설치 명령어:

```bash
pip install -r requirements.txt
```

## Crawling Usage

크롤러는 `crawling/` 폴더에서 실행합니다.

```bash
cd crawling
```

### 1) DragonForce Crawler

DragonForce `.onion` API에서 게시글 데이터를 수집합니다.

```bash
python crawler_dragonforce.py
```

Output:

```txt
outputs/dragonforce_unified.csv
```

주요 수집 필드:

- company
- website
- address
- size_bytes
- size_gib
- is_published
- time_until_publication
- posted_at_utc
- details_url
- description
- files_api_present

### 2) Coinbase Cartel Crawler

Coinbase Cartel `.onion` 사이트에서 피해 기업 목록을 수집합니다.

```bash
python crawler_coinbase_cartel.py
```

Output:

```txt
outputs/coinbase_cartel_unified.csv
```

주요 수집 필드:

- company
- website
- industry
- revenue
- details_url
- ransomware_group
- crawled_at_utc
- crawled_at_kst

### 3) Ransomware.live Crawler

공개 랜섬웨어 인텔리전스 사이트인 ransomware.live에서 피해 기업 및 통계 정보를 수집합니다.

```bash
python crawler_ransomware_live.py
```

Output:

```txt
outputs/ransomware_live_unified.csv
outputs/ransomware_live_victims.csv
outputs/ransomware_live_stats.csv
```

주요 수집 필드:

- company
- ransomware_group
- discovery_date
- estimated_attack_date
- country
- website
- details_url
- description

### 4) DarkForums BeautifulSoup Crawler

DarkForums `.onion` 사이트를 대상으로 게시판별 게시글 정보를 수집합니다.

```bash
python crawler_beautifulsoup_darkforums.py
```

Output:

```txt
outputs/dark_forums_unified.csv
outputs/crawl_state.json
```

특징:

- Tor 프록시 기반 비동기 크롤링
- `httpx.AsyncHTTPTransport` 사용
- 게시판별 URL 순회
- 게시글 중복 확인
- 크롤링 상태 저장 및 재개
- 게시글 본문, 작성자, 평판, 가입일, 게시글 수 등 수집

대상 게시판 예시:

- Databases
- Stealer Logs
- Source Codes
- Combolists
- Cracked Accounts
- Malware
- Hacking Tools
- Exploit & POCs
- OSINT
- Operational Security

### 5) DarkForums Selenium Crawler

Selenium 기반으로 DarkForums를 브라우저 환경에서 크롤링합니다.

```bash
python crawler_selenium_darkforums.py
```

특징:

- Chrome WebDriver 사용
- Tor 프록시 설정
- 인증/Verification 페이지 감지 시 사용자 수동 처리
- SafeWebDriver 래퍼를 통한 브라우저 제어
- Windows 환경에서 ChromeDriver 및 자식 프로세스 정리
- Brotli 인코딩 응답 처리

Selenium 방식은 동적 페이지, 인증 페이지, 브라우저 렌더링이 필요한 경우를 보조하기 위한 크롤러입니다.

### 6) Run Multiple Crawlers Concurrently

`script.py`는 주요 크롤러 3개를 동시에 실행하는 runner입니다.

실행 대상:

- `crawler_dragonforce.py`
- `crawler_ransomware_live.py`
- `crawler_coinbase_cartel.py`

```bash
python script.py
```

전체 로그를 콘솔에 출력하려면:

```bash
python script.py --verbose
```

색상 출력을 끄려면:

```bash
python script.py --no-color
```

출력 로그:

```txt
outputs/logs/dragonforce_YYYYMMDD_HHMMSS.log
outputs/logs/ransomware_live_YYYYMMDD_HHMMSS.log
outputs/logs/coinbase_cartel_YYYYMMDD_HHMMSS.log
```

## Dashboard Usage

대시보드는 `dashboard/` 폴더에 있습니다.

```bash
cd dashboard
streamlit run app.py
```

실행 후 브라우저에서 Streamlit 대시보드가 열립니다.

대시보드에서는 크롤러가 생성한 CSV 파일을 업로드하여 분석합니다.

업로드 대상 예시:

```txt
crawling/outputs/dragonforce_unified.csv
crawling/outputs/coinbase_cartel_unified.csv
crawling/outputs/ransomware_live_unified.csv
crawling/outputs/dark_forums_unified.csv
```

## Dashboard Features

### 1) CSV Multi Upload

여러 크롤러의 CSV를 동시에 업로드할 수 있습니다.

- DragonForce
- Coinbase Cartel
- ransomware.live
- DarkForums

업로드된 CSV는 하나의 DataFrame으로 병합되어 분석됩니다.

### 2) Unified Schema Analysis

크롤러별 데이터 구조가 달라도 통합 스키마 기준으로 분석할 수 있습니다.

공통 필드:

```txt
source
record_type
id
company
website
country
address
size_bytes
size_gib
is_published
time_until_publication
posted_at_utc
crawled_at_utc
crawled_at_kst
ransomware_group
discovery_date
estimated_attack_date
details_url
description
files_api_present
```

DarkForums 확장 필드:

```txt
forum
title
author
last_edited_info
author_rank
reputation
posts_count
threads_count
join_date
main_content
```

### 3) Severity Scoring

대시보드는 여러 요소를 기반으로 심각도 점수를 계산합니다.

주요 점수 요소:

- sensitivity
- volume
- actor
- exposure
- recency
- evidence
- mentions
- cross

점수 구간:

```txt
80 이상: CRITICAL
60 이상: HIGH
40 이상: MEDIUM
20 이상: LOW
20 미만: INFO
```

### 4) Weight Presets

`dashboard/pages/01_Weights_Settings.py`에서 위험도 계산 가중치를 조정할 수 있습니다.

내장 프리셋 예시:

- 기본 균형형
- 긴급 대응 중심
- 랜섬웨어/게시 중심
- 교차게시/트렌드 강화
- 규제/공공 영역 대응

### 5) Weights Guide

`dashboard/pages/02_Weights_Guide.py`에서 각 가중치 항목의 의미와 활용 기준을 확인할 수 있습니다.

### 6) Country Enrichment

대시보드는 피해 기업의 국가 정보를 보강하기 위해 다음 정보를 활용합니다.

- CSV 내 country 필드
- website 도메인 TLD
- description / main_content 내 도메인
- Wikipedia / Wikidata 기반 기업 국가 추정
- Google Places API 기반 보강 옵션

## Output Files

### Crawling Outputs

```txt
crawling/outputs/coinbase_cartel_unified.csv
crawling/outputs/dragonforce_unified.csv
crawling/outputs/ransomware_live_unified.csv
crawling/outputs/ransomware_live_victims.csv
crawling/outputs/ransomware_live_stats.csv
crawling/outputs/dark_forums_unified.csv
```

### Log Outputs

```txt
crawling/outputs/logs/coinbase_cartel_YYYYMMDD_HHMMSS.log
crawling/outputs/logs/dragonforce_YYYYMMDD_HHMMSS.log
crawling/outputs/logs/ransomware_live_YYYYMMDD_HHMMSS.log
```

## Components

### 1) Coinbase Cartel Crawler

File:

```txt
crawling/crawler_coinbase_cartel.py
```

Role:

- Coinbase Cartel `.onion` 사이트 접속
- 피해 기업 카드 파싱
- 기업명, 산업군, 매출, 웹사이트, 상세 링크 수집
- 통합 CSV 저장

### 2) DragonForce Crawler

File:

```txt
crawling/crawler_dragonforce.py
```

Role:

- DragonForce `.onion` API 요청
- 페이지 단위 게시글 수집
- 게시글 공개 여부 및 공개까지 남은 시간 계산
- 데이터 크기 변환
- 통합 CSV 저장

### 3) Ransomware.live Crawler

File:

```txt
crawling/crawler_ransomware_live.py
```

Role:

- ransomware.live HTML 수집
- 피해자 목록 파싱
- 랜섬웨어 그룹, 발견일, 추정 공격일, 국가, 웹사이트 추출
- 전체 통계 CSV 및 통합 CSV 저장

### 4) DarkForums BeautifulSoup Crawler

File:

```txt
crawling/crawler_beautifulsoup_darkforums.py
```

Role:

- DarkForums `.onion` 게시판 크롤링
- BeautifulSoup 기반 HTML 파싱
- 게시글 상세 페이지 수집
- 게시판, 제목, 작성자, 본문, 평판, 가입일 등 수집
- 중복 URL 방지
- 상태 저장 및 재개 지원

### 5) DarkForums Selenium Crawler

File:

```txt
crawling/crawler_selenium_darkforums.py
```

Role:

- Selenium 기반 DarkForums 브라우저 크롤링
- 인증 페이지 감지
- 수동 인증 이후 크롤링 계속 진행
- 동적 페이지 대응
- ChromeDriver 프로세스 관리

### 6) Concurrent Runner

File:

```txt
crawling/script.py
```

Role:

- 주요 크롤러 병렬 실행
- 크롤러별 로그 파일 생성
- 콘솔 출력 필터링
- 실행 결과 요약 출력

### 7) Streamlit Dashboard

Files:

```txt
dashboard/app.py
dashboard/pages/01_Weights_Settings.py
dashboard/pages/02_Weights_Guide.py
dashboard/lib/weights_presets.py
```

Role:

- CSV 업로드 기반 분석
- 심각도 점수 계산
- 국가별 피해 현황 시각화
- 소스별 위험도 분포 시각화
- 가중치 프리셋 설정
- 분석 결과 CSV 다운로드

### 8) Cloud Key Utility

File:

```txt
useleakafter/actions_cloud_keys.py
```

Role:

- AWS IAM Access Key 로테이션
- 기존 키 감사
- CloudTrail 기반 Access Key 사용 이력 조회
- 새 프로필 생성
- 오래된 키 비활성화/삭제 옵션 제공

## Directory Structure

```txt
SemiProject-DarkwebOSINT-main/
  README.md

  crawling/
    README.md
    .gitignore

    crawler_coinbase_cartel.py
    crawler_coinbase_cartel.ipynb

    crawler_dragonforce.py
    crawler_dragonforce.ipynb

    crawler_ransomware_live.py
    crawler_ransomware_live.ipynb

    crawler_beautifulsoup_darkforums.py
    crawler_selenium_darkforums.py

    script.py

    outputs/
      coinbase_cartel_unified.csv
      dragonforce_unified.csv
      ransomware_live_unified.csv
      ransomware_live_victims.csv
      ransomware_live_stats.csv

      logs/
        coinbase_cartel_YYYYMMDD_HHMMSS.log
        dragonforce_YYYYMMDD_HHMMSS.log
        ransomware_live_YYYYMMDD_HHMMSS.log

  dashboard/
    app.py
    lib/
      weights_presets.py
    pages/
      01_Weights_Settings.py
      02_Weights_Guide.py

  domain/

  useleakafter/
    actions_cloud_keys.py
```

## Unified CSV Schema

본 프로젝트는 크롤러별 결과를 하나의 공통 스키마로 맞춥니다.

```txt
source
record_type
id
company
website
country
address
size_bytes
size_gib
is_published
time_until_publication
posted_at_utc
crawled_at_utc
crawled_at_kst
ransomware_group
discovery_date
estimated_attack_date
details_url
description
files_api_present
```

DarkForums 게시글 크롤러는 아래 필드를 추가로 사용합니다.

```txt
forum
title
author
last_edited_info
author_rank
reputation
posts_count
threads_count
join_date
main_content
```

## GitHub Upload Notes

현재 ZIP에는 `.venv/`, `outputs/`, `__pycache__/`가 포함되어 있을 수 있습니다.

GitHub에는 아래 항목을 제외하는 것을 권장합니다.

```gitignore
# Python
__pycache__/
*.pyc
*.pyo
*.pyd

# Virtual Environment
.venv/
venv/
env/

# Outputs
outputs/
*.csv
*.log

# Streamlit / Cache
.streamlit/
.cache/

# OS
.DS_Store
Thumbs.db

# Secrets
.env
*.pem
*.key
credentials.json
```

특히 다크웹/랜섬웨어 수집 결과 CSV는 민감 데이터가 포함될 수 있으므로 GitHub 공개 레포에는 업로드하지 않는 것이 좋습니다.

## Safety / Ethics

본 프로젝트는 보안 연구, 위협 인텔리전스 학습, OSINT 분석 목적의 프로젝트입니다.

- 실제 서비스에 대한 공격 행위를 수행하지 않습니다.
- 인증 우회, 권한 상승, 무단 침입을 목적으로 사용하지 않습니다.
- 수집된 데이터는 연구 및 방어 목적에 한해 사용합니다.
- 민감 정보가 포함된 원본 데이터는 공개 저장소에 업로드하지 않습니다.
- 다크웹 접속 및 데이터 수집은 관련 법률과 조직 정책을 준수해야 합니다.

## Project Status

현재 구현된 주요 기능:

- DragonForce `.onion` API 크롤링
- Coinbase Cartel `.onion` 피해 기업 크롤링
- ransomware.live 공개 피해자 정보 크롤링
- DarkForums BeautifulSoup 기반 크롤링
- DarkForums Selenium 기반 크롤링
- 크롤러 병렬 실행 runner
- 통합 CSV 스키마 저장
- Streamlit 기반 분석 대시보드
- 위험도 가중치 설정 페이지
- AWS IAM Access Key 관리 유틸리티

## Credits

본 프로젝트는 보안 교육 과정 내 다크웹 OSINT 및 랜섬웨어 위협 인텔리전스 분석 실습을 목적으로 제작되었습니다.
