import os
import json
import re
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any

import requests
import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
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


def _strip_possible_isin_prefix(text: str) -> str:
    value = (text or "").strip()
    # 예: KR7000020008동화약품 -> 동화약품
    value = re.sub(r"^[A-Z]{2}[A-Z0-9]{10}", "", value).strip()
    return value


def _normalize_kis_master_entries(raw: Any) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []

    def push(code: Any, name: Any) -> None:
        code_str = str(code or "").strip().upper()
        name_str = _strip_possible_isin_prefix(str(name or "").strip())
        if not code_str or not name_str:
            return
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
                    item.get("code")
                    or item.get("ticker")
                    or item.get("mksc_shrn_iscd")
                    or item.get("stck_shrn_iscd"),
                    item.get("name")
                    or item.get("hts_kor_isnm")
                    or item.get("prdt_name")
                    or item.get("stock_name"),
                )
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                push(item[0], item[1])

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        key = entry["code"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


KIS_MASTER_ENTRIES = _normalize_kis_master_entries(KIS_MASTER)
KIS_MASTER_NAME_MAP = {item["code"]: item["name"] for item in KIS_MASTER_ENTRIES}

US_NAME_MAP = {
    "APPLE": {"name": "Apple", "ticker": "AAPL", "market": "US"},
    "AAPL": {"name": "Apple", "ticker": "AAPL", "market": "US"},
    "MICROSOFT": {"name": "Microsoft", "ticker": "MSFT", "market": "US"},
    "MSFT": {"name": "Microsoft", "ticker": "MSFT", "market": "US"},
    "NVIDIA": {"name": "NVIDIA", "ticker": "NVDA", "market": "US"},
    "NVDA": {"name": "NVIDIA", "ticker": "NVDA", "market": "US"},
    "TESLA": {"name": "Tesla", "ticker": "TSLA", "market": "US"},
    "TSLA": {"name": "Tesla", "ticker": "TSLA", "market": "US"},
    "AMAZON": {"name": "Amazon", "ticker": "AMZN", "market": "US"},
    "AMZN": {"name": "Amazon", "ticker": "AMZN", "market": "US"},
    "GOOGLE": {"name": "Google", "ticker": "GOOGL", "market": "US"},
    "GOOGL": {"name": "Google", "ticker": "GOOGL", "market": "US"},
    "GOOG": {"name": "Google", "ticker": "GOOG", "market": "US"},
    "META": {"name": "Meta", "ticker": "META", "market": "US"},
    "FACEBOOK": {"name": "Meta", "ticker": "META", "market": "US"},
    "NETFLIX": {"name": "Netflix", "ticker": "NFLX", "market": "US"},
    "NFLX": {"name": "Netflix", "ticker": "NFLX", "market": "US"},
}

app = FastAPI(title="Stock Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "")
KIS_BASE_URL = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")

BASE_STOCKS = [
    {"name": "삼성전자", "ticker": "005930", "market": "KRX", "source": "kis"},
    {"name": "현대차", "ticker": "005380", "market": "KRX", "source": "kis"},
    {"name": "LG전자", "ticker": "066570", "market": "KRX", "source": "kis"},
    {"name": "SK하이닉스", "ticker": "000660", "market": "KRX", "source": "kis"},
    {"name": "엔비디아", "ticker": "NVDA", "market": "US", "source": "yf"},
    {"name": "테슬라", "ticker": "TSLA", "market": "US", "source": "yf"},
]

# 검색 편의를 위한 내부 종목 사전. 여기에 없더라도 직접 추가 가능.
STOCK_MASTER = {
    "005930": {"name": "삼성전자", "ticker": "005930", "market": "KRX", "source": "kis"},
    "005930.KS": {"name": "삼성전자", "ticker": "005930", "market": "KRX", "source": "kis"},
    "005380": {"name": "현대차", "ticker": "005380", "market": "KRX", "source": "kis"},
    "005380.KS": {"name": "현대차", "ticker": "005380", "market": "KRX", "source": "kis"},
    "003550": {"name": "LG", "ticker": "003550.KS", "market": "KRX", "source": "yf"},
    "003550.KS": {"name": "LG", "ticker": "003550.KS", "market": "KRX", "source": "yf"},
    "066570": {"name": "LG전자", "ticker": "066570", "market": "KRX", "source": "kis"},
    "066570.KS": {"name": "LG전자", "ticker": "066570", "market": "KRX", "source": "kis"},
    "000660": {"name": "SK하이닉스", "ticker": "000660", "market": "KRX", "source": "kis"},
    "000660.KS": {"name": "SK하이닉스", "ticker": "000660", "market": "KRX", "source": "kis"},
    "009150": {"name": "삼성전기", "ticker": "009150", "market": "KRX", "source": "kis"},
    "009150.KS": {"name": "삼성전기", "ticker": "009150", "market": "KRX", "source": "kis"},
    "035420": {"name": "NAVER", "ticker": "035420.KS", "market": "KRX", "source": "yf"},
    "035420.KS": {"name": "NAVER", "ticker": "035420.KS", "market": "KRX", "source": "yf"},
    "035720": {"name": "카카오", "ticker": "035720.KS", "market": "KRX", "source": "yf"},
    "035720.KS": {"name": "카카오", "ticker": "035720.KS", "market": "KRX", "source": "yf"},
    "051910": {"name": "LG화학", "ticker": "051910.KS", "market": "KRX", "source": "yf"},
    "051910.KS": {"name": "LG화학", "ticker": "051910.KS", "market": "KRX", "source": "yf"},
    "068270": {"name": "셀트리온", "ticker": "068270.KS", "market": "KRX", "source": "yf"},
    "068270.KS": {"name": "셀트리온", "ticker": "068270.KS", "market": "KRX", "source": "yf"},
    "207940": {"name": "삼성바이오로직스", "ticker": "207940.KS", "market": "KRX", "source": "yf"},
    "207940.KS": {"name": "삼성바이오로직스", "ticker": "207940.KS", "market": "KRX", "source": "yf"},
    "035900": {"name": "JYP Ent.", "ticker": "035900.KQ", "market": "KOSDAQ", "source": "yf"},
    "035900.KQ": {"name": "JYP Ent.", "ticker": "035900.KQ", "market": "KOSDAQ", "source": "yf"},
    "091990": {"name": "셀트리온헬스케어", "ticker": "091990.KQ", "market": "KOSDAQ", "source": "yf"},
    "091990.KQ": {"name": "셀트리온헬스케어", "ticker": "091990.KQ", "market": "KOSDAQ", "source": "yf"},
    "247540": {"name": "에코프로비엠", "ticker": "247540.KQ", "market": "KOSDAQ", "source": "yf"},
    "247540.KQ": {"name": "에코프로비엠", "ticker": "247540.KQ", "market": "KOSDAQ", "source": "yf"},
    "086520": {"name": "에코프로", "ticker": "086520.KQ", "market": "KOSDAQ", "source": "yf"},
    "086520.KQ": {"name": "에코프로", "ticker": "086520.KQ", "market": "KOSDAQ", "source": "yf"},
    "108490": {"name": "로보티즈", "ticker": "108490", "market": "KOSDAQ", "source": "kis"},
    "108490.KQ": {"name": "로보티즈", "ticker": "108490", "market": "KOSDAQ", "source": "kis"},
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


def strip_html(text: str) -> str:
    text = unescape(text or "")
    return re.sub(r"<[^>]+>", "", text).strip()


def is_korean_code(text: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", (text or "").strip()))


def normalize_search_input(raw: str) -> str:
    return (raw or "").strip().upper()


def clean_stock_name(name: str, ticker: str) -> str:
    cleaned = (name or "").strip()
    if cleaned:
        return cleaned
    return resolve_korean_name(ticker, ticker)


def resolve_korean_name(code: str, fallback: str | None = None) -> str:
    base = str(code or "").split(".")[0].upper()
    if base in KIS_MASTER_NAME_MAP:
        return KIS_MASTER_NAME_MAP[base]
    if base in KOREAN_NAME_MAP:
        return str(KOREAN_NAME_MAP[base])
    stock_item = STOCK_MASTER.get(base) or STOCK_MASTER.get(f"{base}.KS") or STOCK_MASTER.get(f"{base}.KQ")
    if stock_item and stock_item.get("name"):
        return str(stock_item["name"])
    return fallback or base


def normalize_chart_ticker(raw: str) -> str:
    text = normalize_search_input(raw)
    # 차트는 yfinance 기준으로 조회하므로 국내 6자리는 .KS/.KQ 시도를 우선합니다.
    if re.fullmatch(r"\d{6}\.(KS|KQ)", text):
        return text
    if is_korean_code(text):
        return text
    if text in STOCK_MASTER:
        mapped = str(STOCK_MASTER[text]["ticker"]).upper()
        if re.fullmatch(r"\d{6}\.(KS|KQ)", mapped):
            return mapped
        code_only = mapped.split(".")[0]
        if is_korean_code(code_only):
            return code_only
        return mapped
    return text


def get_kis_access_token() -> str:
    cached = _TOKEN_CACHE.get("value")
    expires_at = _TOKEN_CACHE.get("expires_at")
    if cached and isinstance(expires_at, datetime) and datetime.now() < expires_at:
        return cached

    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 환경변수가 필요합니다.")

    response = requests.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        },
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"KIS access token 발급 실패: {data}")

    _TOKEN_CACHE["value"] = token
    _TOKEN_CACHE["expires_at"] = datetime.now() + timedelta(hours=23)
    return token


def parse_kis_quote(output: dict[str, Any], name: str, ticker: str) -> dict[str, Any]:
    if not output:
        return {
            "name": name,
            "ticker": ticker,
            "price": None,
            "change": None,
            "changePercent": None,
        }

    price = output.get("stck_prpr")
    change = output.get("prdy_vrss")
    sign = str(output.get("prdy_vrss_sign", "")).strip()
    rate = output.get("prdy_ctrt")

    price_f = float(price) if price not in (None, "") else None
    change_f = float(change) if change not in (None, "") else None
    rate_f = float(rate) if rate not in (None, "") else None

    # KIS sign code: 1 상한, 2 상승, 3 보합, 4 하한, 5 하락
    if sign in {"1", "2", "4"}:
        if change_f is not None:
            change_f = abs(change_f)
        if rate_f is not None:
            rate_f = abs(rate_f)
    elif sign == "5":
        if change_f is not None:
            change_f = -abs(change_f)
        if rate_f is not None:
            rate_f = -abs(rate_f)
    elif sign == "3":
        if change_f is not None and abs(change_f) < 1e-12:
            change_f = 0.0
        if rate_f is not None and abs(rate_f) < 1e-12:
            rate_f = 0.0

    return {
        "name": name,
        "ticker": ticker,
        "price": price_f,
        "change": change_f,
        "changePercent": rate_f,
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

    resolved_name = resolve_korean_name(code, name or STOCK_MASTER.get(code, {}).get("name") or code)
    return parse_kis_quote(output, name=resolved_name, ticker=code)


def get_yf_quote(ticker: str, name: str | None = None) -> dict[str, Any]:
    yf_ticker = yf.Ticker(ticker)
    hist = yf_ticker.history(period="5d", interval="1d", auto_adjust=False, prepost=True)

    if hist is None or hist.empty or "Close" not in hist:
        display_name = resolve_korean_name(ticker, name or ticker)
        return {
            "name": display_name,
            "ticker": ticker,
            "price": None,
            "change": None,
            "changePercent": None,
        }

    closes = hist["Close"].dropna()
    if closes.empty:
        display_name = resolve_korean_name(ticker, name or ticker)
        return {
            "name": display_name,
            "ticker": ticker,
            "price": None,
            "change": None,
            "changePercent": None,
        }

    last_close = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else last_close
    change = last_close - prev_close
    change_percent = (change / prev_close * 100) if prev_close else 0.0

    display_name = resolve_korean_name(ticker, name or ticker)
    return {
        "name": display_name,
        "ticker": ticker,
        "price": round(last_close, 2),
        "change": round(change, 2),
        "changePercent": round(change_percent, 2),
    }


def get_quote_by_item(item: dict[str, Any]) -> dict[str, Any]:
    try:
        if item.get("source") == "kis":
            code = item["ticker"].replace(".KS", "").replace(".KQ", "")
            return get_kis_quote(code, name=item["name"])
        return get_yf_quote(item["ticker"], name=item["name"])
    except Exception as exc:
        print("quote item error:", item.get("ticker"), exc)
        return {
            "name": clean_stock_name(item.get("name", ""), item.get("ticker", "")),
            "ticker": item.get("ticker", ""),
            "price": None,
            "change": None,
            "changePercent": None,
        }


def get_quote_for_input(ticker: str) -> dict[str, Any]:
    normalized = normalize_search_input(ticker)
    item = STOCK_MASTER.get(normalized)
    if item:
        return get_quote_by_item(item)

    # 국내 6자리 숫자, 또는 6자리.KS/.KQ 는 KIS 우선 사용
    if is_korean_code(normalized):
        try:
            return get_kis_quote(normalized, name=resolve_korean_name(normalized, normalized))
        except Exception as exc:
            print("quote input kis fallback:", normalized, exc)
            return get_yf_quote(f"{normalized}.KS", name=resolve_korean_name(normalized, normalized))

    m = re.fullmatch(r"(\d{6})\.(KS|KQ)", normalized)
    if m:
        try:
            return get_kis_quote(m.group(1), name=resolve_korean_name(m.group(1), normalized))
        except Exception as exc:
            print("quote input market fallback:", normalized, exc)
            return get_yf_quote(normalized, name=resolve_korean_name(m.group(1), normalized))

    return get_yf_quote(normalized, name=normalized)


def fetch_yf_chart(normalized: str, period: str) -> list[dict[str, Any]]:
    normalized = (normalized or "").strip().upper()
    attempts: list[str] = []

    def add_attempt(value: str) -> None:
        value = (value or "").strip().upper()
        if value and value not in attempts:
            attempts.append(value)

    if is_korean_code(normalized):
        add_attempt(f"{normalized}.KS")
        add_attempt(f"{normalized}.KQ")
        add_attempt(normalized)
    elif re.fullmatch(r"\d{6}\.(KS|KQ)", normalized):
        code = normalized.split(".")[0]
        market = normalized.split(".")[1]
        add_attempt(normalized)
        add_attempt(f"{code}.{'KQ' if market == 'KS' else 'KS'}")
        add_attempt(code)
    else:
        add_attempt(normalized)

    for ticker in attempts:
        try:
            history = yf.Ticker(ticker).history(
                period=period,
                interval="1d",
                auto_adjust=False,
                prepost=True,
            )
        except Exception:
            continue

        if history is None or history.empty:
            continue

        items = []
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
        headers={
            "X-Naver-Client-Id": NAVER_CLIENT_ID,
            "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        },
        params={"query": query, "display": display, "sort": "date"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json() or {}

    items: list[dict[str, Any]] = []
    for item in data.get("items", []):
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
            quote = {
                "name": INDEX_LABELS.get(key, key),
                "ticker": ticker,
                "price": None,
                "change": None,
                "changePercent": None,
            }
        quote["key"] = key
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
            items.append(
                {
                    "name": stock["name"],
                    "ticker": stock["ticker"],
                    "price": None,
                    "change": None,
                    "changePercent": None,
                }
            )
    return {"items": items}


@app.get("/kis-samsung")
def kis_samsung():
    try:
        return get_kis_quote("005930", name="삼성전자")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"kis samsung error: {exc}") from exc


@app.get("/quote/{ticker}")
def quote(ticker: str):
    try:
        return get_quote_for_input(ticker)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"quote error: {exc}") from exc


@app.get("/chart/{ticker}")
def chart(ticker: str, period: str = Query(default="1mo")):
    normalized = normalize_chart_ticker(ticker)
    try:
        items = fetch_yf_chart(normalized, period=period)
        return {"ticker": normalized, "requested": ticker, "items": items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"chart error: {exc}") from exc


@app.get("/news")
def news(q: str = Query(default="증시")):
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        print("news error: NAVER_CLIENT_ID / NAVER_CLIENT_SECRET not loaded")
        return {"items": []}

    queries = [q]
    if q == "증시":
        queries = ["증시", "주식", "코스피", "나스닥"]

    merged: list[dict[str, Any]] = []
    seen_links = set()

    for one_query in queries:
        try:
            for item in fetch_naver_news_once(one_query, display=6):
                link = item.get("link", "")
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
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

    results = []
    seen = set()

    def add_result(name: str, ticker: str, market: str = "", direct: bool = False):
        key = str(ticker).upper()
        if key in seen:
            return
        seen.add(key)
        results.append(
            {
                "name": name,
                "ticker": ticker,
                "market": market,
                "direct": direct,
            }
        )

    # 1) 기존 STOCK_MASTER 우선
    for item in STOCK_MASTER.values():
        name = str(item.get("name", ""))
        ticker = str(item.get("ticker", ""))
        ticker_plain = ticker.replace(".KS", "").replace(".KQ", "")
        if keyword in name.lower() or keyword in ticker.lower() or keyword in ticker_plain.lower():
            add_result(name, ticker, item.get("market", ""), False)

    # 2) KIS 종목 마스터 검색 (dict / list 모두 대응)
    for item in KIS_MASTER_ENTRIES:
        code = item["code"]
        display_name = item["name"]
        if keyword in display_name.lower() or keyword in code.lower():
            add_result(display_name, code, "KRX", False)

    # 3) 이름 맵 JSON 검색
    for code, name in KOREAN_NAME_MAP.items():
        code = str(code).upper()
        display_name = str(name)
        if keyword in display_name.lower() or keyword in code.lower():
            add_result(display_name, code, "KRX", False)

    # 4) 해외 유명 종목 이름/티커 매핑
    for alias, item in US_NAME_MAP.items():
        if keyword in alias.lower() or keyword in str(item["name"]).lower() or keyword == str(item["ticker"]).lower():
            add_result(str(item["name"]), str(item["ticker"]), str(item["market"]), False)

    # 5) 국내 6자리 직접 추가
    if is_korean_code(keyword_upper):
        add_result(resolve_korean_name(keyword_upper, f"{keyword_upper} 직접추가"), keyword_upper, "KR", True)

    # 6) 미국/기타 티커 직접 추가
    elif re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", keyword_upper):
        us_item = US_NAME_MAP.get(keyword_upper)
        if us_item:
            add_result(str(us_item["name"]), str(us_item["ticker"]), str(us_item["market"]), False)
        else:
            add_result(f"{keyword_upper} 직접추가", keyword_upper, "US/기타", True)

    return {"items": results[:30]}
