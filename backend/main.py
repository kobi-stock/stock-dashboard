import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import copy
import threading
from typing import Any

import requests
import websockets
import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

NAME_MAP_PATH = BASE_DIR / "korean_name_map.json"
try:
    KOREAN_NAME_MAP = json.loads(NAME_MAP_PATH.read_text(encoding="utf-8"))
except Exception:
    KOREAN_NAME_MAP = {}

KIS_MASTER_PATH = BASE_DIR / "kis_master.json"
try:
    KIS_MASTER = json.loads(KIS_MASTER_PATH.read_text(encoding="utf-8"))
except Exception:
    KIS_MASTER = {}

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "").strip()
KIS_BASE_URL = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443").strip()
KIS_WS_URL = os.getenv("KIS_WS_URL", "ws://ops.koreainvestment.com:21000").strip()
KIS_CUSTTYPE = os.getenv("KIS_CUSTTYPE", "P").strip() or "P"
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()

USER_AGENT = "Mozilla/5.0"
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"
KIS_EXECUTION_TR_ID = "H0STCNT0"
KIS_SUBSCRIBE = "1"
KIS_UNSUBSCRIBE = "2"
KIS_WS_CACHE_TTL_SECONDS = 90
LAST_GOOD_QUOTE_TTL_SECONDS = 600

app = FastAPI(title="Stock Dashboard API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_STOCKS = [
    {"name": "삼성전자", "ticker": "005930", "market": "KRX", "source": "kis"},
    {"name": "현대차", "ticker": "005380", "market": "KRX", "source": "kis"},
    {"name": "LG전자", "ticker": "066570", "market": "KRX", "source": "kis"},
    {"name": "SK하이닉스", "ticker": "000660", "market": "KRX", "source": "kis"},
    {"name": "엔비디아", "ticker": "NVDA", "market": "US", "source": "yf"},
    {"name": "테슬라", "ticker": "TSLA", "market": "US", "source": "yf"},
]

STOCK_MASTER = {
    "005930": {"name": "삼성전자", "ticker": "005930", "market": "KRX", "source": "kis"},
    "005380": {"name": "현대차", "ticker": "005380", "market": "KRX", "source": "kis"},
    "066570": {"name": "LG전자", "ticker": "066570", "market": "KRX", "source": "kis"},
    "000660": {"name": "SK하이닉스", "ticker": "000660", "market": "KRX", "source": "kis"},
    "009150": {"name": "삼성전기", "ticker": "009150", "market": "KRX", "source": "kis"},
    "108490": {"name": "로보티즈", "ticker": "108490", "market": "KOSDAQ", "source": "kis"},
    "NVDA": {"name": "엔비디아", "ticker": "NVDA", "market": "US", "source": "yf"},
    "TSLA": {"name": "테슬라", "ticker": "TSLA", "market": "US", "source": "yf"},
    "AAPL": {"name": "애플", "ticker": "AAPL", "market": "US", "source": "yf"},
    "MSFT": {"name": "마이크로소프트", "ticker": "MSFT", "market": "US", "source": "yf"},
    "META": {"name": "메타", "ticker": "META", "market": "US", "source": "yf"},
    "AMZN": {"name": "아마존", "ticker": "AMZN", "market": "US", "source": "yf"},
    "GOOGL": {"name": "알파벳A", "ticker": "GOOGL", "market": "US", "source": "yf"},
}

INDEX_TICKERS = {
    "KOSPI": "^KS11",
    "KOSDAQ": "^KQ11",
    "DOW": "^DJI",
    "NASDAQ": "^IXIC",
    # 차트 안정성을 위해 선물 키도 지수로 fallback 가능하게 둠
    "DOW_FUT": "^DJI",
    "NASDAQ_FUT": "^IXIC",
    "WTI": "CL=F",
    "BRENT": "BZ=F",
    "GOLD": "GC=F",
    "SILVER": "SI=F",
    "COPPER": "HG=F",
    "STEEL": "HRC=F",
    "USDKRW": "KRW=X",
    "JPYKRW": "JPYKRW=X",
}

MARKET_CHART_FALLBACKS = {
    "DOW_FUT": ["^DJI", "YM=F"],
    "NASDAQ_FUT": ["^IXIC", "NQ=F"],
    "USDKRW": ["KRW=X", "USDKRW=X"],
    "JPYKRW": ["JPYKRW=X"],
    "WTI": ["CL=F"],
    "BRENT": ["BZ=F"],
    "GOLD": ["GC=F"],
    "SILVER": ["SI=F"],
    "COPPER": ["HG=F"],
    "STEEL": ["HRC=F"],
    "KOSPI": ["^KS11"],
    "KOSDAQ": ["^KQ11"],
    "DOW": ["^DJI"],
    "NASDAQ": ["^IXIC"],
}

INDEX_LABELS = {
    "KOSPI": "코스피",
    "KOSDAQ": "코스닥",
    "DOW": "다우",
    "NASDAQ": "나스닥",
    "DOW_FUT": "다우선물",
    "NASDAQ_FUT": "나스닥선물",
    "WTI": "WTI",
    "BRENT": "브렌트유",
    "GOLD": "금",
    "SILVER": "은",
    "COPPER": "구리",
    "STEEL": "철강",
    "USDKRW": "달러/원",
    "JPYKRW": "엔/원",
}

INDEX_ORDER = [
    "DOW_FUT", "NASDAQ_FUT",
    "KOSPI", "KOSDAQ", "DOW", "NASDAQ",
    "WTI", "BRENT",
    "GOLD", "SILVER",
    "COPPER", "STEEL",
    "USDKRW", "JPYKRW",
]

KIS_EXECUTION_FIELDS = [
    "MKSC_SHRN_ISCD",
    "STCK_CNTG_HOUR",
    "STCK_PRPR",
    "PRDY_VRSS_SIGN",
    "PRDY_VRSS",
    "PRDY_CTRT",
    "WGHN_AVRG_STCK_PRC",
    "STCK_OPRC",
    "STCK_HGPR",
    "STCK_LWPR",
    "ASKP1",
    "BIDP1",
    "CNTG_VOL",
    "ACML_VOL",
    "ACML_TR_PBMN",
    "SELN_CNTG_CSNU",
    "SHNU_CNTG_CSNU",
    "NTBY_CNTG_CSNU",
    "CTTR",
    "SELN_CNTG_SMTN",
    "SHNU_CNTG_SMTN",
    "CCLD_DVSN",
    "SHNU_RATE",
    "PRDY_VOL_VRSS_ACML_VOL_RATE",
    "OPRC_HOUR",
    "OPRC_VRSS_PRPR_SIGN",
    "OPRC_VRSS_PRPR",
    "HGPR_HOUR",
    "HGPR_VRSS_PRPR_SIGN",
    "HGPR_VRSS_PRPR",
    "LWPR_HOUR",
    "LWPR_VRSS_PRPR_SIGN",
    "LWPR_VRSS_PRPR",
    "BSOP_DATE",
    "NEW_MKOP_CLS_CODE",
    "TRHT_YN",
    "ASKP_RSQN1",
    "BIDP_RSQN1",
    "TOTAL_ASKP_RSQN",
    "TOTAL_BIDP_RSQN",
    "VOL_TNRT",
    "PRDY_SMNS_HOUR_ACML_VOL",
    "PRDY_SMNS_HOUR_ACML_VOL_RATE",
    "HOUR_CLS_CODE",
    "MRKT_TRTM_CLS_CODE",
    "VI_STND_PRC",
]

_TOKEN_CACHE: dict[str, Any] = {"value": None, "expires_at": None}
_APPROVAL_CACHE: dict[str, Any] = {"value": None, "expires_at": None}
NXT_CACHE: dict[str, dict[str, Any]] = {}
REALTIME_QUOTES_CACHE: dict[str, dict[str, Any]] = {}
LAST_GOOD_QUOTES_CACHE: dict[str, dict[str, Any]] = {}
_KIS_DISABLED_UNTIL: datetime | None = None

MARKET_CACHE_TTL_SECONDS = 45
NEWS_CACHE_TTL_SECONDS = 45
CHART_CACHE_TTL_SECONDS = 600
_MARKET_CACHE: dict[str, Any] = {"items": None, "fetched_at": None}
_NEWS_CACHE: dict[str, dict[str, Any]] = {}
_CHART_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()


def _strip_possible_isin_prefix(text: str) -> str:
    value = (text or "").strip()
    return re.sub(r"^[A-Z]{2}[A-Z0-9]{10}", "", value).strip()


def _normalize_kis_master_entries(raw: Any) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []

    def push(code: Any, name: Any) -> None:
        code_str = str(code or "").strip().upper()
        name_str = _strip_possible_isin_prefix(str(name or "").strip())
        if code_str and name_str:
            entries.append({"code": code_str, "name": name_str})

    if isinstance(raw, dict):
        for code, value in raw.items():
            if isinstance(value, dict):
                push(value.get("code") or code, value.get("name") or value.get("hts_kor_isnm") or value.get("prdt_name"))
            else:
                push(code, value)
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                push(
                    item.get("code") or item.get("ticker") or item.get("mksc_shrn_iscd") or item.get("stck_shrn_iscd"),
                    item.get("name") or item.get("hts_kor_isnm") or item.get("prdt_name") or item.get("stock_name"),
                )
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                push(item[0], item[1])

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        key = entry["code"]
        if key not in seen:
            seen.add(key)
            deduped.append(entry)
    return deduped


KIS_MASTER_ENTRIES = _normalize_kis_master_entries(KIS_MASTER)
KIS_MASTER_NAME_MAP = {item["code"]: item["name"] for item in KIS_MASTER_ENTRIES}


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", unescape(text or "")).strip()


def normalize_search_input(raw: str) -> str:
    return (raw or "").strip().upper()


def is_korean_code(text: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", (text or "").strip()))


def is_korean_ticker_with_suffix(text: str) -> bool:
    return bool(re.fullmatch(r"\d{6}\.(KS|KQ)", normalize_search_input(text)))


def resolve_korean_name(code: str, fallback: str | None = None) -> str:
    base = normalize_search_input(code).split(".")[0]
    if base in KIS_MASTER_NAME_MAP:
        return KIS_MASTER_NAME_MAP[base]
    if base in KOREAN_NAME_MAP:
        return str(KOREAN_NAME_MAP[base])
    item = STOCK_MASTER.get(base)
    if item and item.get("name"):
        return str(item["name"])
    return fallback or base


def normalize_chart_ticker(raw: str) -> str:
    return normalize_search_input(raw)


def _to_float_or_none(value: Any) -> float | None:
    try:
        text = str(value).replace(",", "").strip()
        return None if text == "" else float(text)
    except Exception:
        return None


def kis_available() -> bool:
    global _KIS_DISABLED_UNTIL
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return False
    if _KIS_DISABLED_UNTIL and datetime.now() < _KIS_DISABLED_UNTIL:
        return False
    return True


def mark_kis_temporarily_unavailable(minutes: int = 10) -> None:
    global _KIS_DISABLED_UNTIL
    _KIS_DISABLED_UNTIL = datetime.now() + timedelta(minutes=minutes)


def parse_kis_quote(output: dict[str, Any], name: str, ticker: str) -> dict[str, Any]:
    price = _to_float_or_none(output.get("stck_prpr"))
    change = _to_float_or_none(output.get("prdy_vrss"))
    rate = _to_float_or_none(output.get("prdy_ctrt"))
    sign = str(output.get("prdy_vrss_sign", "")).strip()

    if sign == "5":
        change = -abs(change) if change is not None else None
        rate = -abs(rate) if rate is not None else None
    elif sign in {"1", "2", "4"}:
        change = abs(change) if change is not None else None
        rate = abs(rate) if rate is not None else None

    return {
        "name": name,
        "ticker": ticker,
        "price": price,
        "change": change,
        "changePercent": rate,
    }


def parse_kis_overtime_quote(output: dict[str, Any], name: str, ticker: str) -> dict[str, Any]:
    price = _to_float_or_none(output.get("ovtm_untp_prpr"))
    change = _to_float_or_none(output.get("ovtm_untp_prdy_vrss"))
    rate = _to_float_or_none(output.get("ovtm_untp_ctng_ctrt"))
    if rate is None:
        rate = _to_float_or_none(output.get("ovtm_untp_prdy_ctrt"))
    sign = str(output.get("ovtm_untp_prdy_vrss_sign", "")).strip()
    if sign == "5":
        change = -abs(change) if change is not None else None
        rate = -abs(rate) if rate is not None else None
    elif sign in {"1", "2", "4"}:
        change = abs(change) if change is not None else None
        rate = abs(rate) if rate is not None else None
    return {
        "name": name,
        "ticker": ticker,
        "price": price,
        "change": change,
        "changePercent": rate,
        "session": "NXT",
        "label": "NXT",
        "source": "kis_overtime",
        "raw": output or {},
    }


def parse_kis_ws_execution(fields: list[str]) -> dict[str, Any] | None:
    if not fields:
        return None

    values = {key: fields[idx] if idx < len(fields) else "" for idx, key in enumerate(KIS_EXECUTION_FIELDS)}
    ticker = normalize_search_input(values.get("MKSC_SHRN_ISCD", ""))
    if not is_korean_code(ticker):
        return None

    parsed = parse_kis_quote(
        {
            "stck_prpr": values.get("STCK_PRPR"),
            "prdy_vrss": values.get("PRDY_VRSS"),
            "prdy_ctrt": values.get("PRDY_CTRT"),
            "prdy_vrss_sign": values.get("PRDY_VRSS_SIGN"),
        },
        resolve_korean_name(ticker, ticker),
        ticker,
    )
    parsed["source"] = "kis_ws"
    parsed["tradeTime"] = values.get("STCK_CNTG_HOUR")
    parsed["accumulatedVolume"] = _to_float_or_none(values.get("ACML_VOL"))
    parsed["volume"] = _to_float_or_none(values.get("CNTG_VOL"))
    parsed["ask1"] = _to_float_or_none(values.get("ASKP1"))
    parsed["bid1"] = _to_float_or_none(values.get("BIDP1"))
    parsed["strength"] = _to_float_or_none(values.get("CTTR"))
    parsed["raw"] = values
    return parsed


def get_kis_access_token() -> str:
    cached = _TOKEN_CACHE.get("value")
    expires_at = _TOKEN_CACHE.get("expires_at")
    if cached and isinstance(expires_at, datetime) and datetime.now() < expires_at:
        return cached

    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 환경변수가 필요합니다.")
    if not kis_available():
        raise RuntimeError("KIS API가 일시적으로 비활성 상태입니다.")

    try:
        response = requests.post(
            f"{KIS_BASE_URL}/oauth2/tokenP",
            json={"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET},
            timeout=10,
        )
        response.raise_for_status()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 403:
            mark_kis_temporarily_unavailable(10)
            raise RuntimeError("KIS 인증이 거부되었습니다. 앱키/시크릿 또는 API 신청 상태를 확인해 주세요.") from exc
        raise
    except Exception:
        mark_kis_temporarily_unavailable(2)
        raise

    data = response.json() or {}
    token = data.get("access_token")
    if not token:
        mark_kis_temporarily_unavailable(10)
        raise RuntimeError(f"KIS access token 발급 실패: {data}")

    _TOKEN_CACHE["value"] = token
    _TOKEN_CACHE["expires_at"] = datetime.now() + timedelta(hours=23)
    return token


def get_kis_approval_key() -> str:
    cached = _APPROVAL_CACHE.get("value")
    expires_at = _APPROVAL_CACHE.get("expires_at")
    if cached and isinstance(expires_at, datetime) and datetime.now() < expires_at:
        return cached

    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 환경변수가 필요합니다.")
    if not kis_available():
        raise RuntimeError("KIS API가 일시적으로 비활성 상태입니다.")

    response = requests.post(
        f"{KIS_BASE_URL}/oauth2/Approval",
        json={"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "secretkey": KIS_APP_SECRET},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}
    approval_key = data.get("approval_key")
    if not approval_key:
        raise RuntimeError(f"KIS approval_key 발급 실패: {data}")

    _APPROVAL_CACHE["value"] = approval_key
    _APPROVAL_CACHE["expires_at"] = datetime.now() + timedelta(hours=23)
    return approval_key


def get_kis_quote(code: str, name: str | None = None) -> dict[str, Any]:
    token = get_kis_access_token()
    response = requests.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
        headers={
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST01010100",
        },
        params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}
    output = data.get("output") or {}
    if data.get("rt_cd") not in (None, "0"):
        raise RuntimeError(f"KIS quote 실패: {data}")
    return parse_kis_quote(output, resolve_korean_name(code, name or code), code)


def get_kis_overtime_quote(code: str, name: str | None = None) -> dict[str, Any]:
    token = get_kis_access_token()
    response = requests.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-overtimeprice",
        headers={
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHPST02320000",
        },
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}
    output = data.get("output1") or {}
    if data.get("rt_cd") not in (None, "0"):
        raise RuntimeError(f"KIS overtime quote 실패: {data}")
    return parse_kis_overtime_quote(output, resolve_korean_name(code, name or code), code)


def build_nxt_payload(code: str, name: str | None = None) -> dict[str, Any] | None:
    normalized = normalize_search_input(code).split(".")[0]
    if normalized in NXT_CACHE:
        return NXT_CACHE[normalized]
    if not kis_available():
        return None
    try:
        nxt = get_kis_overtime_quote(normalized, name=name)
        if nxt and nxt.get("price") is not None:
            NXT_CACHE[normalized] = nxt
        return nxt
    except Exception as exc:
        print("nxt quote error:", normalized, exc)
        return None


def fetch_yahoo_search(query: str, limit: int = 12) -> list[dict[str, Any]]:
    response = requests.get(
        YAHOO_SEARCH_URL,
        params={"q": query, "quotesCount": limit, "newsCount": 0, "listsCount": 0, "enableFuzzyQuery": True},
        headers={"User-Agent": USER_AGENT},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}
    items: list[dict[str, Any]] = []
    for quote in data.get("quotes", []) or []:
        symbol = str(quote.get("symbol") or "").upper().strip()
        name = str(quote.get("shortname") or quote.get("longname") or symbol).strip()
        quote_type = str(quote.get("quoteType") or "").upper()
        exchange = str(quote.get("exchange") or quote.get("exchDisp") or "").upper()
        if not symbol or quote_type not in {"EQUITY", "ETF"}:
            continue
        market = "US" if exchange in {"NMS", "NAS", "NYSE", "PCX", "ASE", "BTS"} or "NASDAQ" in exchange or "NYSE" in exchange else exchange or "GLOBAL"
        items.append({"name": name, "ticker": symbol, "market": market, "source": "yf"})
    return items


def get_us_extended_payload(info: dict[str, Any], regular_price: float | None, previous_close: float | None) -> dict[str, Any] | None:
    post_price = _to_float_or_none(info.get("postMarketPrice"))
    pre_price = _to_float_or_none(info.get("preMarketPrice"))

    if post_price is not None:
        base_price = regular_price if regular_price is not None else previous_close
        change = (post_price - base_price) if base_price is not None else _to_float_or_none(info.get("postMarketChange"))
        rate = (change / base_price * 100) if change is not None and base_price not in (None, 0) else _to_float_or_none(info.get("postMarketChangePercent"))
        return {
            "price": round(post_price, 4),
            "change": round(change, 4) if change is not None else None,
            "changePercent": round(rate, 4) if rate is not None else None,
            "session": "POST",
            "label": "애프터",
            "source": "yfinance",
        }

    if pre_price is not None:
        base_price = previous_close if previous_close is not None else regular_price
        change = (pre_price - base_price) if base_price is not None else _to_float_or_none(info.get("preMarketChange"))
        rate = (change / base_price * 100) if change is not None and base_price not in (None, 0) else _to_float_or_none(info.get("preMarketChangePercent"))
        return {
            "price": round(pre_price, 4),
            "change": round(change, 4) if change is not None else None,
            "changePercent": round(rate, 4) if rate is not None else None,
            "session": "PRE",
            "label": "프리",
            "source": "yfinance",
        }

    return None


def get_yf_quote(ticker: str, name: str | None = None) -> dict[str, Any]:
    normalized_ticker = normalize_search_input(ticker)
    yt = yf.Ticker(normalized_ticker)
    history = yt.history(period="5d", interval="1d", auto_adjust=False, prepost=False)
    info: dict[str, Any] = {}
    try:
        info = yt.info or {}
    except Exception:
        info = {}

    closes = history["Close"].dropna() if history is not None and not history.empty and "Close" in history else []
    last_close = float(closes.iloc[-1]) if len(closes) >= 1 else _to_float_or_none(info.get("regularMarketPrice"))
    prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else _to_float_or_none(info.get("regularMarketPreviousClose"))

    regular_price = _to_float_or_none(info.get("regularMarketPrice"))
    if regular_price is None:
        regular_price = last_close

    change = None
    change_percent = None
    if regular_price is not None and prev_close not in (None, 0):
        change = regular_price - prev_close
        change_percent = (change / prev_close) * 100

    base_code = normalized_ticker.split(".")[0]
    display_name = str(info.get("shortName") or info.get("longName") or "").strip()
    if is_korean_code(base_code):
        display_name = resolve_korean_name(base_code, display_name or name or normalized_ticker)
    else:
        display_name = name or display_name or normalized_ticker

    quote = {
        "name": display_name,
        "ticker": normalized_ticker,
        "price": round(regular_price, 4) if regular_price is not None else None,
        "change": round(change, 4) if change is not None else None,
        "changePercent": round(change_percent, 4) if change_percent is not None else None,
    }

    extended = get_us_extended_payload(info, regular_price=regular_price, previous_close=prev_close)
    quote["extended"] = extended
    quote["hasExtended"] = extended is not None and extended.get("price") is not None
    return quote


def get_market_quote_light(ticker: str, name: str | None = None) -> dict[str, Any]:
    normalized_ticker = normalize_search_input(ticker)
    display_name = name or normalized_ticker

    items = fetch_yf_chart(normalized_ticker, period="5d")
    if len(items) >= 2:
        last = items[-1]
        prev = items[-2]
        price = _to_float_or_none(last.get("close"))
        prev_close = _to_float_or_none(prev.get("close"))
        change = None
        change_percent = None
        if price is not None and prev_close not in (None, 0):
            change = price - prev_close
            change_percent = (change / prev_close) * 100
        return {
            "name": display_name,
            "ticker": normalized_ticker,
            "price": round(price, 4) if price is not None else None,
            "change": round(change, 4) if change is not None else None,
            "changePercent": round(change_percent, 4) if change_percent is not None else None,
            "source": "yf_history",
        }

    if len(items) == 1:
        last = items[-1]
        price = _to_float_or_none(last.get("close"))
        return {
            "name": display_name,
            "ticker": normalized_ticker,
            "price": round(price, 4) if price is not None else None,
            "change": None,
            "changePercent": None,
            "source": "yf_history",
        }

    return {
        "name": display_name,
        "ticker": normalized_ticker,
        "price": None,
        "change": None,
        "changePercent": None,
        "source": "yf_history_empty",
    }


def get_yf_quote_with_fallback(ticker: str, name: str | None = None) -> dict[str, Any]:
    normalized = normalize_search_input(ticker)
    attempts: list[str] = []
    if is_korean_code(normalized):
        attempts = [f"{normalized}.KS", f"{normalized}.KQ", normalized]
    elif is_korean_ticker_with_suffix(normalized):
        code = normalized.split(".")[0]
        suffix = normalized.split(".")[1]
        attempts = [normalized, f"{code}.{'KQ' if suffix == 'KS' else 'KS'}", code]
    else:
        attempts = [normalized]

    last_exc: Exception | None = None
    for one in attempts:
        try:
            quote = get_yf_quote(one, name=name)
            if quote.get("price") is not None or quote.get("name"):
                return quote
        except Exception as exc:
            last_exc = exc
            continue

    if last_exc:
        raise last_exc
    return get_yf_quote(normalized, name=name)


def get_cached_realtime_quote(code: str) -> dict[str, Any] | None:
    normalized = normalize_search_input(code).split(".")[0]
    cached = REALTIME_QUOTES_CACHE.get(normalized)
    if not cached:
        return None
    ts = cached.get("_cached_at")
    if isinstance(ts, datetime) and datetime.now() - ts <= timedelta(seconds=KIS_WS_CACHE_TTL_SECONDS):
        payload = {key: value for key, value in cached.items() if not key.startswith("_")}
        payload["asOf"] = ts.isoformat() if isinstance(ts, datetime) else None
        payload["nxt"] = build_nxt_payload(normalized, name=payload.get("name"))
        payload["hasNxt"] = payload["nxt"] is not None and payload["nxt"].get("price") is not None
        remember_last_good_quote(payload)
        return payload
    return None


def remember_last_good_quote(payload: dict[str, Any]) -> dict[str, Any]:
    ticker = normalize_search_input(str(payload.get("ticker") or "")).split(".")[0]
    price = _to_float_or_none(payload.get("price"))
    if ticker and price not in (None, 0):
        LAST_GOOD_QUOTES_CACHE[ticker] = {**payload, "_cached_at": datetime.now()}
    return payload


def get_last_good_quote(code: str) -> dict[str, Any] | None:
    ticker = normalize_search_input(code).split(".")[0]
    cached = LAST_GOOD_QUOTES_CACHE.get(ticker)
    if not cached:
        return None
    ts = cached.get("_cached_at")
    if isinstance(ts, datetime) and datetime.now() - ts <= timedelta(seconds=LAST_GOOD_QUOTE_TTL_SECONDS):
        payload = {key: value for key, value in cached.items() if not key.startswith("_")}
        payload["stale"] = True
        payload["source"] = payload.get("source") or "last_good"
        payload["nxt"] = build_nxt_payload(ticker, name=payload.get("name"))
        payload["hasNxt"] = payload["nxt"] is not None and payload["nxt"].get("price") is not None
        return payload
    return None


def get_korean_quote_strict(code: str, name: str | None = None) -> dict[str, Any]:
    normalized = normalize_search_input(code).split(".")[0]
    cached = get_cached_realtime_quote(normalized)
    if cached and cached.get("price") not in (None, 0):
        return remember_last_good_quote(cached)

    try:
        quote = get_kis_quote(normalized, name=name)
        quote["ticker"] = normalized
        quote["name"] = quote.get("name") or resolve_korean_name(normalized, name or normalized)
        quote["source"] = "kis_rest"
        quote["nxt"] = build_nxt_payload(normalized, name=quote.get("name"))
        quote["hasNxt"] = quote["nxt"] is not None and quote["nxt"].get("price") is not None
        return remember_last_good_quote(quote)
    except Exception as exc:
        stale = get_last_good_quote(normalized)
        if stale:
            stale["note"] = "실시간 갱신 지연 중, 마지막 정상값 유지"
            return stale
        raise exc


def build_korean_fallback_quote(code: str, name: str | None = None) -> dict[str, Any]:
    normalized = normalize_search_input(code).split(".")[0]
    resolved_name = resolve_korean_name(normalized, name or normalized)

    try:
        quote = get_yf_quote_with_fallback(normalized, name=resolved_name)
        quote["ticker"] = normalized
        quote["name"] = quote.get("name") or resolved_name
        quote["nxt"] = None
        quote["hasNxt"] = False
        quote["source"] = "kr_fallback"
        quote["note"] = "장중에는 실시간 체결 우선, 장마감 후에는 마지막 가격/종가 표시"
        return quote
    except Exception:
        return {
            "name": resolved_name,
            "ticker": normalized,
            "price": None,
            "change": None,
            "changePercent": None,
            "nxt": None,
            "hasNxt": False,
            "source": "kr_fallback_empty",
            "note": "가격을 아직 불러오지 못했습니다.",
        }


def get_quote_by_item(item: dict[str, Any]) -> dict[str, Any]:
    source = item.get("source")
    ticker = str(item.get("ticker") or "")
    name = str(item.get("name") or ticker)
    if source == "kis":
        code = ticker.split(".")[0]
        return get_korean_quote_strict(code, name=name)
    return get_yf_quote_with_fallback(ticker, name=name)


def get_quote_for_input(ticker: str) -> dict[str, Any]:
    normalized = normalize_search_input(ticker)
    item = STOCK_MASTER.get(normalized)
    if item:
        return get_quote_by_item(item)
    if is_korean_code(normalized):
        return get_korean_quote_strict(normalized, name=resolve_korean_name(normalized, normalized))
    if is_korean_ticker_with_suffix(normalized):
        base_code = normalized.split(".")[0]
        return get_yf_quote_with_fallback(normalized, name=resolve_korean_name(base_code, base_code))
    return get_yf_quote_with_fallback(normalized, name=normalized)


def _history_to_items(history: Any) -> list[dict[str, Any]]:
    if history is None or getattr(history, "empty", True):
        return []
    items: list[dict[str, Any]] = []
    for idx, row in history.iterrows():
        try:
            items.append(
                {
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": float(row["Volume"]),
                }
            )
        except Exception:
            continue
    return items


def fetch_yf_chart(normalized: str, period: str) -> list[dict[str, Any]]:
    attempts: list[str] = []
    if isinstance(normalized, str) and "|" in normalized:
        attempts = [one for one in normalized.split("|") if one]
    elif is_korean_code(normalized):
        attempts = [f"{normalized}.KS", f"{normalized}.KQ", normalized]
    else:
        attempts = [normalized]

    for one in attempts:
        try:
            history = yf.Ticker(one).history(period=period, interval="1d", auto_adjust=False, prepost=False)
            items = _history_to_items(history)
            if items:
                return items
        except Exception:
            pass

        try:
            history = yf.download(
                one,
                period=period,
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            items = _history_to_items(history)
            if items:
                return items
        except Exception:
            continue

    return []


def get_cached_chart_items(ticker: str, period: str = "1mo") -> list[dict[str, Any]]:
    normalized = normalize_chart_ticker(ticker)
    cache_key = f"chart::{normalized}::{period}"
    with _CACHE_LOCK:
        entry = _CHART_CACHE.get(cache_key, {})
        cached_at = entry.get("fetched_at")
        cached_items = entry.get("items")
        if _is_cache_fresh(cached_at, CHART_CACHE_TTL_SECONDS) and cached_items:
            return _clone_items(cached_items)

    items = fetch_yf_chart(normalized, period=period)

    # 빈 차트는 캐시에 넣지 않음. 다음 요청에서 다시 시도.
    if items:
        with _CACHE_LOCK:
            _CHART_CACHE[cache_key] = {"items": items, "fetched_at": datetime.now()}

    return _clone_items(items)


def prewarm_chart_cache() -> None:
    """Warm only stock charts for the stable build.

    Market charts are intentionally not prewarmed because index/commodity/fx chart
    sources have been noisy and can trigger repeated Yahoo failures on Render.
    """
    tickers = [item.get("ticker") for item in BASE_STOCKS if item.get("ticker")]
    deduped: list[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        normalized = normalize_chart_ticker(str(ticker or ""))
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    for ticker in deduped:
        try:
            get_cached_chart_items(ticker, "1mo")
        except Exception as exc:
            print("chart prewarm error:", ticker, exc)


def fetch_naver_news_once(query: str, display: int = 6) -> list[dict[str, Any]]:
    response = requests.get(
        "https://openapi.naver.com/v1/search/news.json",
        headers={"X-Naver-Client-Id": NAVER_CLIENT_ID, "X-Naver-Client-Secret": NAVER_CLIENT_SECRET},
        params={"query": query, "display": display, "sort": "date"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}
    items: list[dict[str, Any]] = []
    for item in data.get("items", []) or []:
        pub_raw = item.get("pubDate", "")
        try:
            pub_text = parsedate_to_datetime(pub_raw).strftime("%Y-%m-%d %H:%M")
        except Exception:
            pub_text = pub_raw
        items.append(
            {
                "title": strip_html(item.get("title", "")),
                "description": strip_html(item.get("description", "")),
                "link": item.get("originallink") or item.get("link") or "#",
                "pubDate": pub_text,
                "source": "네이버 뉴스검색",
            }
        )
    return items


class RealtimeHub:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()
        self.subscriptions: dict[WebSocket, set[str]] = {}
        self.task: asyncio.Task | None = None

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.add(websocket)
        self.subscriptions[websocket] = set()

    async def disconnect(self, websocket: WebSocket) -> None:
        self.connections.discard(websocket)
        self.subscriptions.pop(websocket, None)
        await sync_kis_realtime_subscriptions()

    async def set_tickers(self, websocket: WebSocket, tickers: list[str]) -> None:
        normalized = {normalize_search_input(one) for one in tickers if normalize_search_input(one)}
        self.subscriptions[websocket] = normalized
        await sync_kis_realtime_subscriptions()

    def all_tickers(self) -> set[str]:
        merged: set[str] = set()
        for tickers in self.subscriptions.values():
            merged |= tickers
        return merged

    async def broadcast_items(self, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        ts = datetime.now().isoformat()
        normalized_map = {normalize_search_input(item.get("ticker", "")): item for item in items if item.get("ticker")}
        stale: list[WebSocket] = []
        for websocket in list(self.connections):
            try:
                subs = self.subscriptions.get(websocket, set())
                payload = [normalized_map[ticker] for ticker in subs if ticker in normalized_map]
                if payload:
                    await websocket.send_json({"type": "quotes", "items": payload, "ts": ts})
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self.connections.discard(websocket)
            self.subscriptions.pop(websocket, None)

    async def publish_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            if not self.connections:
                continue

            tickers = self.all_tickers()
            if not tickers:
                continue

            payload_items: list[dict[str, Any]] = []
            for ticker in sorted(tickers):
                try:
                    if is_korean_code(ticker):
                        cached = get_cached_realtime_quote(ticker)
                        if cached:
                            payload_items.append(cached)
                            continue
                    payload_items.append(get_quote_for_input(ticker))
                except Exception as exc:
                    payload_items.append(
                        {
                            "ticker": ticker,
                            "name": resolve_korean_name(ticker, ticker),
                            "price": None,
                            "change": None,
                            "changePercent": None,
                            "error": str(exc),
                        }
                    )

            await self.broadcast_items(payload_items)


class KISRealtimeManager:
    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.desired_tickers: set[str] = set()
        self.active_tickers: set[str] = set()
        self.ws: Any | None = None
        self.lock = asyncio.Lock()
        self.kick = asyncio.Event()

    async def ensure_running(self) -> None:
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run_forever())

    async def update_tickers(self, tickers: set[str]) -> None:
        normalized = {normalize_search_input(one).split(".")[0] for one in tickers if is_korean_code(normalize_search_input(one).split(".")[0])}
        self.desired_tickers = normalized
        self.kick.set()
        await self.ensure_running()

    async def run_forever(self) -> None:
        backoff = 1
        while True:
            try:
                if not kis_available():
                    await asyncio.sleep(5)
                    continue
                if not self.desired_tickers:
                    self.kick.clear()
                    await self.kick.wait()
                    continue

                approval_key = get_kis_approval_key()
                async with websockets.connect(KIS_WS_URL, ping_interval=30, ping_timeout=30, open_timeout=10, close_timeout=5) as ws:
                    self.ws = ws
                    self.active_tickers = set()
                    await self.sync_subscriptions()
                    backoff = 1

                    while True:
                        if self.kick.is_set():
                            self.kick.clear()
                            await self.sync_subscriptions()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=20)
                        except asyncio.TimeoutError:
                            await self.sync_subscriptions()
                            continue
                        await self.handle_message(raw)
            except Exception as exc:
                print("kis realtime loop error:", exc)
                self.ws = None
                self.active_tickers = set()
                await asyncio.sleep(min(backoff, 20))
                backoff = min(backoff * 2, 20)

    async def sync_subscriptions(self) -> None:
        async with self.lock:
            if self.ws is None:
                return
            to_subscribe = sorted(self.desired_tickers - self.active_tickers)
            to_unsubscribe = sorted(self.active_tickers - self.desired_tickers)
            for code in to_subscribe:
                await self.send_subscription(code, subscribe=True)
                self.active_tickers.add(code)
            for code in to_unsubscribe:
                await self.send_subscription(code, subscribe=False)
                self.active_tickers.discard(code)

    async def send_subscription(self, code: str, subscribe: bool) -> None:
        if self.ws is None:
            return
        payload = {
            "header": {
                "approval_key": get_kis_approval_key(),
                "custtype": KIS_CUSTTYPE,
                "tr_type": KIS_SUBSCRIBE if subscribe else KIS_UNSUBSCRIBE,
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": KIS_EXECUTION_TR_ID,
                    "tr_key": code,
                }
            },
        }
        await self.ws.send(json.dumps(payload, ensure_ascii=False))

    async def handle_message(self, raw: Any) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        text = str(raw or "")
        if not text:
            return

        if text == "PINGPONG":
            return

        if text.startswith("{"):
            try:
                data = json.loads(text)
            except Exception:
                print("kis realtime json parse error:", text)
                return
            header = data.get("header") or {}
            tr_id = header.get("tr_id") or header.get("trid") or data.get("tr_id")
            if tr_id == "PINGPONG":
                return
            rt_cd = header.get("rt_cd") or data.get("rt_cd")
            msg1 = header.get("msg1") or data.get("msg1") or ""
            if rt_cd not in (None, "0"):
                print("kis realtime control:", rt_cd, msg1, data)
            return

        if re.match(r"^\d+\|H0STCNT0\|\d+\|", text):
            parts = text.split("|", 3)
            if len(parts) < 4:
                return
            rows = parts[3].split("^")
            chunk_size = len(KIS_EXECUTION_FIELDS)
            if not rows:
                return
            payloads: list[dict[str, Any]] = []
            if len(rows) <= chunk_size:
                parsed = parse_kis_ws_execution(rows)
                if parsed:
                    payloads.append(parsed)
            else:
                for idx in range(0, len(rows), chunk_size):
                    parsed = parse_kis_ws_execution(rows[idx: idx + chunk_size])
                    if parsed:
                        payloads.append(parsed)
            if payloads:
                now = datetime.now()
                for payload in payloads:
                    enriched = {**payload, "_cached_at": now, "asOf": now.isoformat()}
                    REALTIME_QUOTES_CACHE[payload["ticker"]] = enriched
                    remember_last_good_quote(enriched)
                await REALTIME_HUB.broadcast_items([{**p, "asOf": now.isoformat()} for p in payloads])
            return

        print("kis realtime raw:", text[:200])


REALTIME_HUB = RealtimeHub()
KIS_REALTIME = KISRealtimeManager()


async def sync_kis_realtime_subscriptions() -> None:
    wanted = {ticker for ticker in REALTIME_HUB.all_tickers() if is_korean_code(ticker.split(".")[0])}
    await KIS_REALTIME.update_tickers(wanted)


def _is_cache_fresh(cached_at: datetime | None, ttl_seconds: int) -> bool:
    return isinstance(cached_at, datetime) and (datetime.now() - cached_at) <= timedelta(seconds=ttl_seconds)


def _clone_items(items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not items:
        return []
    return copy.deepcopy(items)



def _normalize_text_for_parse(value: str) -> str:
    value = unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def get_korean_index_from_naver(index_type: str) -> dict[str, Any] | None:
    def _safe_float(raw: str | None) -> float | None:
        cleaned = str(raw or "").replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None

    try:
        response = requests.get(
            "https://finance.naver.com/sise/",
            headers={"User-Agent": USER_AGENT, "Referer": "https://finance.naver.com/"},
            timeout=10,
        )
        response.raise_for_status()

        html = response.text
        target_name = "코스피" if index_type == "KOSPI" else "코스닥"

        search_texts: list[str] = []
        search_texts.append(_normalize_text_for_parse(html))
        for raw in re.split(r"</?(?:dd|dt|li|span|div|p|br|a|em|strong|td|tr|th)[^>]*>", html, flags=re.IGNORECASE):
            line = _normalize_text_for_parse(raw)
            if line:
                search_texts.append(line)

        patterns = [
            rf"{target_name}\s*지수\s*([\d,]+(?:\.\d+)?)\s*전일대비\s*(상승|하락)\s*([\d,]+(?:\.\d+)?)\s*(?:플러스|마이너스)?\s*([\d,]+(?:\.\d+)?)\s*퍼센트",
            rf"{target_name}\s*지수\s*([\d,]+(?:\.\d+)?)\s*전일대비\s*(상승|하락)\s*([\d,]+(?:\.\d+)?)\s*(?:플러스|마이너스)?\s*([\d,]+(?:\.\d+)?)%",
            rf"{target_name}\s*지수\s*([\d,]+(?:\.\d+)?)\s*전일대비\s*(상승|하락)\s*([\d,]+(?:\.\d+)?)\s*(?:플러스|마이너스)?\s*([\d,]+(?:\.\d+)?)",
        ]

        for text in search_texts:
            if target_name not in text or "전일대비" not in text:
                continue

            for pattern in patterns:
                match = re.search(pattern, text)
                if not match:
                    continue

                price = _safe_float(match.group(1))
                direction = match.group(2)
                change = _safe_float(match.group(3))
                change_percent = _safe_float(match.group(4))

                if price is None or change is None or change_percent is None:
                    continue

                if direction == "하락":
                    change = -abs(change)
                    change_percent = -abs(change_percent)
                else:
                    change = abs(change)
                    change_percent = abs(change_percent)

                return {
                    "name": INDEX_LABELS.get(index_type, index_type),
                    "ticker": INDEX_TICKERS.get(index_type, index_type),
                    "price": price,
                    "change": change,
                    "changePercent": change_percent,
                    "source": "naver_index",
                }

            numbers = re.findall(r"[\d,]+(?:\.\d+)?", text)
            parsed = [_safe_float(number) for number in numbers[:3]]
            if len(parsed) >= 3 and all(value is not None for value in parsed):
                price, change, change_percent = parsed
                if "하락" in text or "마이너스" in text:
                    change = -abs(change)
                    change_percent = -abs(change_percent)
                elif "상승" in text or "플러스" in text:
                    change = abs(change)
                    change_percent = abs(change_percent)
                return {
                    "name": INDEX_LABELS.get(index_type, index_type),
                    "ticker": INDEX_TICKERS.get(index_type, index_type),
                    "price": price,
                    "change": change,
                    "changePercent": change_percent,
                    "source": "naver_index_fallback",
                }

        raise RuntimeError(f"{index_type} text parse failed")

    except Exception as exc:
        print("naver index parse error:", index_type, exc)
        return None

def _fetch_single_market_item(key: str, ticker: str) -> dict[str, Any]:
    try:
        if key in {"KOSPI", "KOSDAQ"}:
            quote = get_korean_index_from_naver(key)
            if not quote:
                quote = get_market_quote_light(ticker, INDEX_LABELS.get(key, key))
        else:
            quote = get_market_quote_light(ticker, INDEX_LABELS.get(key, key))
    except Exception as exc:
        print("market fetch error:", key, ticker, exc)
        quote = {"name": INDEX_LABELS.get(key, key), "ticker": ticker, "price": None, "change": None, "changePercent": None}
    quote["key"] = key
    quote.pop("extended", None)
    quote.pop("hasExtended", None)
    return quote


def build_market_items() -> list[dict[str, Any]]:
    keys = list(INDEX_TICKERS.items())
    if not keys:
        return []

    items: list[dict[str, Any]] = []
    max_workers = min(6, len(keys)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_single_market_item, key, ticker) for key, ticker in keys]
        for future in futures:
            try:
                items.append(future.result())
            except Exception as exc:
                print("market future error:", exc)

    items.sort(key=lambda item: INDEX_ORDER.index(item["key"]) if item.get("key") in INDEX_ORDER else 999)
    return items


def get_cached_market_items(force_refresh: bool = False) -> list[dict[str, Any]]:
    with _CACHE_LOCK:
        cached_at = _MARKET_CACHE.get("fetched_at")
        cached_items = _MARKET_CACHE.get("items")
        if not force_refresh and _is_cache_fresh(cached_at, MARKET_CACHE_TTL_SECONDS) and cached_items:
            return _clone_items(cached_items)

    items = build_market_items()

    with _CACHE_LOCK:
        _MARKET_CACHE["items"] = items
        _MARKET_CACHE["fetched_at"] = datetime.now()

    return _clone_items(items)


def build_news_items(query: str = "증시") -> list[dict[str, Any]]:
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return []

    queries = [query]
    if query == "증시":
        queries = ["증시", "주식", "코스피", "나스닥"]

    results_by_query: dict[str, list[dict[str, Any]]] = {one_query: [] for one_query in queries}
    max_workers = min(4, len(queries)) or 1

    def fetch_one(one_query: str) -> tuple[str, list[dict[str, Any]]]:
        try:
            return one_query, fetch_naver_news_once(one_query, display=6)
        except Exception as exc:
            print(f"news fetch error ({one_query}):", exc)
            return one_query, []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_one, one_query) for one_query in queries]
        for future in futures:
            try:
                one_query, items = future.result()
                results_by_query[one_query] = items
            except Exception as exc:
                print("news future error:", exc)

    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for one_query in queries:
        for item in results_by_query.get(one_query, []):
            key = item.get("link") or item.get("title")
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= 10:
                return merged[:10]

    return merged[:10]


def get_cached_news_items(query: str = "증시", force_refresh: bool = False) -> list[dict[str, Any]]:
    with _CACHE_LOCK:
        cache_entry = _NEWS_CACHE.get(query, {})
        cached_at = cache_entry.get("fetched_at")
        cached_items = cache_entry.get("items")
        if not force_refresh and _is_cache_fresh(cached_at, NEWS_CACHE_TTL_SECONDS) and cached_items is not None:
            return _clone_items(cached_items)

    items = build_news_items(query)

    with _CACHE_LOCK:
        _NEWS_CACHE[query] = {"items": items, "fetched_at": datetime.now()}

    return _clone_items(items)


async def prewarm_light_data() -> None:
    try:
        await asyncio.gather(
            asyncio.to_thread(get_cached_market_items, True),
            asyncio.to_thread(get_cached_news_items, "증시", True),
        )
    except Exception as exc:
        print("prewarm error:", exc)


@app.get("/")
def root():
    return {"message": "ok"}


@app.get("/health")
def health():
    return {"ok": True, "service": "stock-dashboard-api"}


@app.get("/market")
def market():
    return {"items": get_cached_market_items()}


@app.get("/stocks")
def stocks():
    items = []
    for stock in BASE_STOCKS:
        try:
            items.append(get_quote_by_item(stock))
        except Exception as exc:
            print("stocks error:", stock["ticker"], exc)
            items.append({"name": stock["name"], "ticker": stock["ticker"], "price": None, "change": None, "changePercent": None})
    return {"items": items}


@app.get("/quote/{ticker}")
def quote(ticker: str):
    try:
        return get_quote_for_input(ticker)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"quote error: {exc}") from exc


@app.get("/nxt/{ticker}")
def nxt_quote(ticker: str):
    normalized = normalize_search_input(ticker).split(".")[0]
    if not is_korean_code(normalized):
        return {"ticker": normalized, "nxt": None, "message": "NXT는 국내 6자리 종목코드만 지원합니다."}
    try:
        return {"ticker": normalized, "nxt": build_nxt_payload(normalized, name=resolve_korean_name(normalized, normalized))}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"nxt quote error: {exc}") from exc


@app.get("/nxt-cache")
def nxt_cache():
    return NXT_CACHE


@app.get("/chart/{ticker}")
def chart(ticker: str, period: str = Query(default="1mo")):
    normalized = normalize_chart_ticker(ticker)
    try:
        return {"ticker": normalized, "requested": ticker, "items": get_cached_chart_items(normalized, period=period), "cached": True}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"chart error: {exc}") from exc


@app.get("/market-chart/{key}")
def market_chart(key: str, period: str = Query(default="1mo")):
    normalized_key = normalize_search_input(key)
    tickers = MARKET_CHART_FALLBACKS.get(normalized_key)
    if not tickers:
        yf_ticker = INDEX_TICKERS.get(normalized_key)
        if not yf_ticker:
            raise HTTPException(status_code=404, detail=f"market chart key not found: {key}")
        tickers = [str(yf_ticker)]

    try:
        for one in tickers:
            items = get_cached_chart_items(one, period=period)
            if items:
                return {"key": normalized_key, "ticker": one, "items": items, "cached": True}
        return {"key": normalized_key, "ticker": tickers[0], "items": [], "cached": False}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"market chart error: {exc}") from exc


@app.get("/news")
def news(q: str = Query(default="증시")):
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return {"items": []}

    queries = [q]
    if q == "증시":
        queries = ["증시", "주식", "코스피", "나스닥"]

    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for one_query in queries:
        try:
            for item in fetch_naver_news_once(one_query, display=6):
                key = item.get("link") or item.get("title")
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
                if len(merged) >= 12:
                    break
            if len(merged) >= 12:
                break
        except Exception as exc:
            print(f"news fetch error ({one_query}):", exc)

    return {"items": merged[:10]}


@app.get("/search-stock")
def search_stock(q: str = Query(default="")):
    keyword_raw = q.strip()
    keyword = keyword_raw.lower()
    keyword_upper = keyword_raw.upper()
    if not keyword_raw:
        return {"items": []}

    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_result(name: str, ticker: str, market: str = "", direct: bool = False, source: str | None = None) -> None:
        key = str(ticker).upper()
        if key in seen:
            return
        seen.add(key)
        results.append({"name": name, "ticker": ticker, "market": market, "direct": direct, "source": source})

    for item in STOCK_MASTER.values():
        name = str(item.get("name", ""))
        ticker = str(item.get("ticker", ""))
        if keyword in name.lower() or keyword in ticker.lower():
            add_result(name, ticker, str(item.get("market", "")), False, str(item.get("source", "")))

    for item in KIS_MASTER_ENTRIES:
        code = item["code"]
        display_name = item["name"]
        if keyword in display_name.lower() or keyword in code.lower():
            add_result(display_name, code, "KRX", False, "kis")

    if not is_korean_code(keyword_upper) and re.search(r"[A-Z]", keyword_upper):
        try:
            for item in fetch_yahoo_search(keyword_raw, limit=16):
                add_result(str(item["name"]), str(item["ticker"]), str(item.get("market", "US")), False, "yf")
        except Exception as exc:
            print("yahoo search error:", exc)

    if is_korean_code(keyword_upper):
        add_result(resolve_korean_name(keyword_upper, f"{keyword_upper} 직접추가"), keyword_upper, "KRX", True, "kis")
    elif re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", keyword_upper):
        add_result(f"{keyword_upper} 직접추가", keyword_upper, "US", True, "yf")

    return {"items": results[:30]}


@app.get("/realtime-status")
def realtime_status():
    return {
        "kis_available": kis_available(),
        "ws_url": KIS_WS_URL,
        "desired_tickers": sorted(KIS_REALTIME.desired_tickers),
        "active_tickers": sorted(KIS_REALTIME.active_tickers),
        "cache_keys": sorted(REALTIME_QUOTES_CACHE.keys()),
    }


@app.websocket("/ws/realtime")
async def realtime_ws(websocket: WebSocket):
    await REALTIME_HUB.connect(websocket)
    try:
        await websocket.send_json({"type": "hello", "message": "connected"})
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                data = {"tickers": []}
            tickers = data.get("tickers") or []
            if isinstance(tickers, list):
                await REALTIME_HUB.set_tickers(websocket, tickers)
                await websocket.send_json({"type": "subscribed", "tickers": sorted(REALTIME_HUB.subscriptions.get(websocket, set()))})
    except WebSocketDisconnect:
        await REALTIME_HUB.disconnect(websocket)
    except Exception:
        await REALTIME_HUB.disconnect(websocket)


@app.on_event("startup")
async def startup_event():
    await KIS_REALTIME.ensure_running()
    if REALTIME_HUB.task is None:
        REALTIME_HUB.task = asyncio.create_task(REALTIME_HUB.publish_loop())
    asyncio.create_task(prewarm_light_data())
    asyncio.create_task(asyncio.to_thread(prewarm_chart_cache))


@app.on_event("shutdown")
async def shutdown_event():
    if REALTIME_HUB.task:
        REALTIME_HUB.task.cancel()
    if KIS_REALTIME.task:
        KIS_REALTIME.task.cancel()