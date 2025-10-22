# lib/weights_presets.py
from typing import Dict, List

ORDER = ["sensitivity","volume","actor","exposure","recency","evidence","mentions","cross"]

DEFAULT_WEIGHTS = {
    "sensitivity":25,"volume":10,"actor":15,"exposure":20,"recency":10,"evidence":10,"mentions":5,"cross":5
}

PRESETS: Dict[str, Dict] = {
    "balanced": {
        "label": "균형형 (기본)",
        "raw": { "sensitivity":25,"volume":10,"actor":15,"exposure":20,"recency":10,"evidence":10,"mentions":5,"cross":5 },
        "when": [
            "대시보드를 처음 설정하시거나, 전체 소스를 고르게 파악하고 싶으실 때",
            "팀 내 공통 기준으로 사건을 모니터링하시고자 할 때",
        ],
        "notes": (
            "민감도, 노출 단계, 행위자, 최근성, 증거, 교차게시 등 주요 신호를 고르게 반영하는 프리셋입니다. "
            "특정 신호에 치우치지 않고 전체 분포와 우선순위를 폭넓게 살펴보실 때 적합합니다. "
            "초기 운영·팀 합의용 기준선으로 사용하시고, 이후 목적에 맞게 세부 조정을 권장드립니다."
        )
    },
    "pii_secrets": {
        "label": "PII/시크릿 민감도 우선",
        "raw": { "sensitivity":35,"volume":8,"actor":10,"exposure":18,"recency":8,"evidence":15,"mentions":4,"cross":2 },
        "when": [
            "개인정보(PII), 인증정보, 토큰/키 등 민감 자산의 유출 가능성에 선제 대응하시고자 할 때",
            "내부 접근권한 탈취·계정 탈취 등 2차 피해가 우려될 때",
        ],
        "notes": (
            "‘민감도’와 ‘증거’를 높게 반영하여 자격증명·비밀키·API 토큰·지갑키·쿠키 등 보안 핵심자산의 유출 신호를 "
            "더 촘촘히 포착합니다. 준수(컴플라이언스)·IAM·개인정보보호 관점에서 빠른 확인과 격리 조치가 필요하실 때 권장드립니다."
        )
    },
    "breaking_now": {
        "label": "실시간/신규 사고 추적",
        "raw": { "sensitivity":18,"volume":6,"actor":12,"exposure":22,"recency":25,"evidence":10,"mentions":5,"cross":2 },
        "when": [
            "최근 7~30일 내에 발생·게시된 이슈를 우선적으로 모니터링하실 때",
            "보안관제·사고대응(IR)에서 ‘지금 중요한 것’을 상단에 띄우고 싶을 때",
        ],
        "notes": (
            "‘최근성’과 ‘노출 단계(공지→게시)’ 비중을 높여, 방금 올라온 사건이나 공개 직전·직후 이슈를 상위에 노출합니다. "
            "알림/온콜 대시보드에 적합하며, 초동 대응·현황 브리핑·승인 의사결정에 유용합니다."
        )
    },
    "large_dumps": {
        "label": "대용량 덤프 우선",
        "raw": { "sensitivity":18,"volume":25,"actor":10,"exposure":18,"recency":8,"evidence":10,"mentions":6,"cross":5 },
        "when": [
            "수십 GB~TB급 대용량 데이터 유출 가능성을 우선 선별하고 싶으실 때",
            "영향 범위와 복구 비용이 큰 사건을 먼저 보고받고 싶으실 때",
        ],
        "notes": (
            "‘용량’ 신호를 크게 반영하여 대규모 데이터 유출 가능성이 있는 항목을 상위로 끌어올립니다. "
            "침해면 분석·업무 영향도 추산·백업/복구 전략 수립 등, 리소스 집중이 필요한 상황에서 먼저 대응 대상을 추리실 때 유용합니다."
        )
    },
    "ransom_pub": {
        "label": "랜섬웨어/게시 중심",
        "raw": { "sensitivity":18,"volume":6,"actor":18,"exposure":28,"recency":12,"evidence":10,"mentions":6,"cross":2 },
        "when": [
            "랜섬웨어 블로그의 카운트다운·공개·재게시 흐름을 면밀히 추적하실 때",
            "법무·PR과의 커뮤니케이션 타이밍(공개 임박/직후)을 놓치지 않으시려 할 때",
        ],
        "notes": (
            "‘노출 단계’와 ‘행위자(랜섬그룹)’ 비중을 강화해, 공개 선언·증거 샘플·게시 완료 등 타임라인 변화를 "
            "명확히 반영합니다. 외부 커뮤니케이션·대외 리스크 관리·경영 보고에 적합합니다."
        )
    },
    "cross_trending": {
        "label": "교차게시/트렌드 강화",
        "raw": { "sensitivity":18,"volume":8,"actor":10,"exposure":16,"recency":10,"evidence":10,"mentions":8,"cross":20 },
        "when": [
            "여러 소스에 반복 노출되며 확산 중인 이슈(재판매·재게시·재가공)를 우선 확인하고 싶으실 때",
            "레퓨테이션·브랜드 훼손 리스크가 커지는 사안을 조기 포착하고자 할 때",
        ],
        "notes": (
            "‘교차게시’ 신호를 크게 반영하여 다중 출처에 반복 등장하는 항목을 상위로 배치합니다. "
            "확산 속도가 빠른 재유통 이슈를 조기에 식별해, 서비스·고객 커뮤니케이션·침해범위 공지 등 선제적 조치를 지원합니다."
        )
    },
    "regulated_gov": {
        "label": "규제/공공 영역 대응",
        "raw": { "sensitivity":28,"volume":8,"actor":10,"exposure":18,"recency":10,"evidence":16,"mentions":8,"cross":2 },
        "when": [
            "금융·의료·공공 등 규제 강도가 높은 조직의 관제·보고 체계를 운영하실 때",
            "증거 기반 확인(샘플·스크린샷)과 민감 데이터 보호를 동시에 강화하고 싶으실 때",
        ],
        "notes": (
            "개인정보·의료정보·정부 데이터 등 규제 민감 자산을 고려하여 ‘민감도’와 ‘증거’ 비중을 높였습니다. "
            "감사·보고·사후대응 체계와의 연계를 염두에 두고, 확인 가능한 근거가 있는 사건을 우선 검토하시기에 적합합니다."
        )
    },
}


def normalize(raw: Dict[str, float]) -> Dict[str, float]:
    total = sum(raw.get(k, 0) for k in ORDER)
    if total <= 0:
        raw = DEFAULT_WEIGHTS.copy()
        total = sum(raw.values())
    return {k: (raw.get(k, 0) / total) * 100 for k in ORDER}

