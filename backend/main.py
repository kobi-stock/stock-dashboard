import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
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
KIS_WS_CACHE_TTL_SECONDS = 15
MARKET_CACHE_TTL_SECONDS = 45
NEWS_CACHE_TTL_SECONDS = 45


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
    "DOW_FUT": "YM=F",
    "NASDAQ_FUT": "NQ=F",
    "GOLD": "GC=F",
    "WTI": "CL=F",
    "BRENT": "BZ=F",
    "USDKRW": "KRW=X",
    "JPYKRW": "JPYKRW=X",
}

INDEX_LABELS = {
    "KOSPI": "코스피",
    "KOSDAQ": "코스닥",
    "DOW": "다우",
    "NASDAQ": "나스닥",
    "DOW_FUT": "다우선물",
    "NASDAQ_FUT": "나스닥선물",
    "GOLD": "금",
    "WTI": "WTI",
    "BRENT": "브렌트유",
    "USDKRW": "달러/원",
    "JPYKRW": "엔/원",
}

INDEX_ORDER = [
    "DOW_FUT", "NASDAQ_FUT",
    "KOSPI", "KOSDAQ", "DOW", "NASDAQ",
    "GOLD", "WTI", "BRENT",
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
_KIS_DISABLED_UNTIL: datetime | None = None
_MARKET_CACHE: dict[str, Any] = {"items": [], "cached_at": None}
_NEWS_CACHE: dict[str, Any] = {"items": [], "cached_at": None, "query": "증시"}



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
        payload["nxt"] = build_nxt_payload(normalized, name=payload.get("name"))
        payload["hasNxt"] = payload["nxt"] is not None and payload["nxt"].get("price") is not None
        return payload
    return None


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
        cached = get_cached_realtime_quote(code)
        if cached:
            return cached
        return build_korean_fallback_quote(code, name=name)
    return get_yf_quote_with_fallback(ticker, name=name)


def get_quote_for_input(ticker: str) -> dict[str, Any]:
    normalized = normalize_search_input(ticker)
    item = STOCK_MASTER.get(normalized)
    if item:
        return get_quote_by_item(item)
    if is_korean_code(normalized):
        cached = get_cached_realtime_quote(normalized)
        if cached:
            return cached
        return build_korean_fallback_quote(normalized, name=resolve_korean_name(normalized, normalized))
    if is_korean_ticker_with_suffix(normalized):
        base_code = normalized.split(".")[0]
        return get_yf_quote_with_fallback(normalized, name=resolve_korean_name(base_code, base_code))
    return get_yf_quote_with_fallback(normalized, name=normalized)


def fetch_yf_chart(normalized: str, period: str) -> list[dict[str, Any]]:
    attempts: list[str] = []
    if is_korean_code(normalized):
        attempts = [f"{normalized}.KS", f"{normalized}.KQ", normalized]
    else:
        attempts = [normalized]

    for one in attempts:
        try:
            history = yf.Ticker(one).history(period=period, interval="1d", auto_adjust=False, prepost=True)
        except Exception:
            continue
        if history is None or history.empty:
            continue
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
        if items:
            return items
    return []


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
            await asyncio.sleep(2)
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
                    REALTIME_QUOTES_CACHE[payload["ticker"]] = {**payload, "_cached_at": now}
                await REALTIME_HUB.broadcast_items(payloads)
            return

        print("kis realtime raw:", text[:200])


REALTIME_HUB = RealtimeHub()
KIS_REALTIME = KISRealtimeManager()


async def sync_kis_realtime_subscriptions() -> None:
    wanted = {ticker for ticker in REALTIME_HUB.all_tickers() if is_korean_code(ticker.split(".")[0])}
    await KIS_REALTIME.update_tickers(wanted)


def _cache_age_seconds(cached_at: Any) -> float | None:
    if isinstance(cached_at, datetime):
        return max(0.0, (datetime.now() - cached_at).total_seconds())
    return None


def get_market_items_cached(force: bool = False) -> list[dict[str, Any]]:
    cached_at = _MARKET_CACHE.get("cached_at")
    cache_age = _cache_age_seconds(cached_at)
    if not force and cache_age is not None and cache_age <= MARKET_CACHE_TTL_SECONDS and _MARKET_CACHE.get("items"):
        return list(_MARKET_CACHE["items"])

    items: list[dict[str, Any]] = []
    previous_map = {str(item.get("key")): item for item in (_MARKET_CACHE.get("items") or []) if item.get("key")}
    for key, ticker in INDEX_TICKERS.items():
        try:
            quote = get_yf_quote(ticker, INDEX_LABELS.get(key, key))
        except Exception:
            quote = previous_map.get(key) or {
                "name": INDEX_LABELS.get(key, key),
                "ticker": ticker,
                "price": None,
                "change": None,
                "changePercent": None,
            }
        quote["key"] = key
        quote.pop("extended", None)
        quote.pop("hasExtended", None)
        items.append(quote)

    items.sort(key=lambda item: INDEX_ORDER.index(item["key"]) if item.get("key") in INDEX_ORDER else 999)
    _MARKET_CACHE["items"] = items
    _MARKET_CACHE["cached_at"] = datetime.now()
    return list(items)


def get_news_items_cached(query: str = "증시", force: bool = False) -> list[dict[str, Any]]:
    query = (query or "증시").strip() or "증시"
    cached_at = _NEWS_CACHE.get("cached_at")
    cache_age = _cache_age_seconds(cached_at)
    same_query = _NEWS_CACHE.get("query") == query
    if not force and same_query and cache_age is not None and cache_age <= NEWS_CACHE_TTL_SECONDS and _NEWS_CACHE.get("items"):
        return list(_NEWS_CACHE["items"])

    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        _NEWS_CACHE["items"] = []
        _NEWS_CACHE["cached_at"] = datetime.now()
        _NEWS_CACHE["query"] = query
        return []

    queries = [query]
    if query == "증시":
        queries = ["증시", "주식", "코스피", "나스닥"]

    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    previous_items = list(_NEWS_CACHE.get("items") or []) if same_query else []
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

    final_items = merged[:10] if merged else previous_items[:10]
    _NEWS_CACHE["items"] = final_items
    _NEWS_CACHE["cached_at"] = datetime.now()
    _NEWS_CACHE["query"] = query
    return list(final_items)


@app.get("/")
def root():
    return {"message": "ok"}


@app.get("/health")
def health():
    market_cached_at = _MARKET_CACHE.get("cached_at")
    news_cached_at = _NEWS_CACHE.get("cached_at")
    return {
        "ok": True,
        "service": "stock-dashboard-api",
        "time": datetime.now().isoformat(),
        "kis_available": kis_available(),
        "market_cache_age_seconds": _cache_age_seconds(market_cached_at),
        "news_cache_age_seconds": _cache_age_seconds(news_cached_at),
        "market_items": len(_MARKET_CACHE.get("items") or []),
        "news_items": len(_NEWS_CACHE.get("items") or []),
        "realtime_clients": len(REALTIME_HUB.connections),
    }


@app.get("/market")
def market():
    return {"items": get_market_items_cached()}


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
        return {"ticker": normalized, "requested": ticker, "items": fetch_yf_chart(normalized, period=period)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"chart error: {exc}") from exc


@app.get("/news")
def news(q: str = Query(default="증시")):
    return {"items": get_news_items_cached(query=q)}


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

    async def warm_caches() -> None:
        for _ in range(2):
            try:
                await asyncio.to_thread(get_market_items_cached, True)
            except Exception as exc:
                print("startup market warmup error:", exc)
            try:
                await asyncio.to_thread(get_news_items_cached, "증시", True)
            except Exception as exc:
                print("startup news warmup error:", exc)
            await asyncio.sleep(0.1)

    asyncio.create_task(warm_caches())


@app.on_event("shutdown")
async def shutdown_event():
    if REALTIME_HUB.task:
        REALTIME_HUB.task.cancel()
    if KIS_REALTIME.task:
        KIS_REALTIME.task.cancel()
