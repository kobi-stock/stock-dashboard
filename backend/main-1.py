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
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()

USER_AGENT = "Mozilla/5.0"
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"

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

_TOKEN_CACHE: dict[str, Any] = {"value": None, "expires_at": None}
NXT_CACHE: dict[str, dict[str, Any]] = {}
_KIS_DISABLED_UNTIL: datetime | None = None



class RealtimeHub:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()
        self.subscriptions: dict[WebSocket, set[str]] = {}
        self.task: asyncio.Task | None = None

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.add(websocket)
        self.subscriptions[websocket] = set()

    def disconnect(self, websocket: WebSocket) -> None:
        self.connections.discard(websocket)
        self.subscriptions.pop(websocket, None)

    async def set_tickers(self, websocket: WebSocket, tickers: list[str]) -> None:
        normalized = {normalize_search_input(one) for one in tickers if normalize_search_input(one)}
        self.subscriptions[websocket] = normalized

    def all_tickers(self) -> set[str]:
        merged: set[str] = set()
        for tickers in self.subscriptions.values():
            merged |= tickers
        return merged

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

            payload = {
                "type": "quotes",
                "items": payload_items,
                "ts": datetime.now().isoformat(),
            }

            stale: list[WebSocket] = []
            for websocket in list(self.connections):
                try:
                    subs = self.subscriptions.get(websocket, set())
                    items = [item for item in payload_items if normalize_search_input(item.get("ticker", "")) in subs]
                    await websocket.send_json({"type": "quotes", "items": items, "ts": payload["ts"]})
                except Exception:
                    stale.append(websocket)

            for websocket in stale:
                self.disconnect(websocket)


REALTIME_HUB = RealtimeHub()


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
    text = normalize_search_input(raw)
    if is_korean_code(text):
        return text
    return text


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


def is_korean_ticker_with_suffix(text: str) -> bool:
    return bool(re.fullmatch(r"\d{6}\.(KS|KQ)", normalize_search_input(text)))


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


def get_quote_by_item(item: dict[str, Any]) -> dict[str, Any]:
    source = item.get("source")
    ticker = str(item.get("ticker") or "")
    name = str(item.get("name") or ticker)
    if source == "kis":
        code = ticker.split(".")[0]
        try:
            quote = get_kis_quote(code, name=name)
            quote["nxt"] = build_nxt_payload(code, name=name)
            quote["hasNxt"] = quote["nxt"] is not None and quote["nxt"].get("price") is not None
            return quote
        except Exception as exc:
            print("kis quote fallback:", code, exc)
            quote = get_yf_quote_with_fallback(code, name=name)
            quote["ticker"] = code
            quote["nxt"] = None
            quote["hasNxt"] = False
            return quote
    return get_yf_quote_with_fallback(ticker, name=name)


def get_quote_for_input(ticker: str) -> dict[str, Any]:
    normalized = normalize_search_input(ticker)
    item = STOCK_MASTER.get(normalized)
    if item:
        return get_quote_by_item(item)
    if is_korean_code(normalized):
        try:
            quote = get_kis_quote(normalized, name=resolve_korean_name(normalized, normalized))
            quote["nxt"] = build_nxt_payload(normalized, name=resolve_korean_name(normalized, normalized))
            quote["hasNxt"] = quote["nxt"] is not None and quote["nxt"].get("price") is not None
            return quote
        except Exception as exc:
            print("quote input kis fallback:", normalized, exc)
            quote = get_yf_quote_with_fallback(normalized, name=resolve_korean_name(normalized, normalized))
            quote["ticker"] = normalized
            quote["nxt"] = None
            quote["hasNxt"] = False
            return quote
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


@app.get("/")
def root():
    return {"message": "ok"}


@app.get("/market")
def market():
    items = []
    for key, ticker in INDEX_TICKERS.items():
        try:
            quote = get_yf_quote(ticker, INDEX_LABELS.get(key, key))
        except Exception:
            quote = {"name": INDEX_LABELS.get(key, key), "ticker": ticker, "price": None, "change": None, "changePercent": None}
        quote["key"] = key
        quote.pop("extended", None)
        quote.pop("hasExtended", None)
        items.append(quote)
    items.sort(key=lambda item: INDEX_ORDER.index(item["key"]) if item.get("key") in INDEX_ORDER else 999)
    return {"items": items}


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
        REALTIME_HUB.disconnect(websocket)
    except Exception:
        REALTIME_HUB.disconnect(websocket)


@app.on_event("startup")
async def startup_event():
    if kis_available():
        for stock in BASE_STOCKS:
            ticker = str(stock.get("ticker", "")).upper()
            code = ticker.split(".")[0]
            if is_korean_code(code):
                build_nxt_payload(code, name=stock.get("name"))
    if REALTIME_HUB.task is None:
        REALTIME_HUB.task = asyncio.create_task(REALTIME_HUB.publish_loop())
