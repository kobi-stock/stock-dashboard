import { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

const API_HOST =
  import.meta.env.VITE_API_HOST ||
  (() => {
    const host = window.location.hostname;
    if (host === "localhost" || host === "127.0.0.1") return "127.0.0.1";
    return host;
  })();

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${API_HOST}:8000`;
const REFRESH_MS = 5000;
const EXTRA_STOCKS_KEY = "extraStocks";
const DEFAULT_STOCKS_KEY = "defaultStocks";
const HOLDINGS_KEY = "investmentHoldings";

function normalizeTicker(ticker) {
  return String(ticker || "").trim().toUpperCase();
}

function toNumber(value) {
  const n = Number(String(value ?? "").replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : null;
}

function normalizeSavedItems(items) {
  if (!Array.isArray(items)) return [];
  return items
    .map((item) => ({
      ticker: normalizeTicker(item?.ticker),
      name: item?.name || item?.ticker || "종목",
      price: item?.price ?? null,
      change: item?.change ?? null,
      changePercent: item?.changePercent ?? null,
    }))
    .filter((item) => item.ticker);
}

function normalizeHoldings(items) {
  if (!Array.isArray(items)) return [];
  return items
    .map((item) => {
      const quantity = toNumber(item?.quantity);
      const avgPrice = toNumber(item?.avgPrice);
      return {
        ticker: normalizeTicker(item?.ticker),
        name: item?.name || item?.ticker || "종목",
        quantity: quantity ?? 0,
        avgPrice: avgPrice ?? 0,
        price: item?.price ?? null,
        change: item?.change ?? null,
        changePercent: item?.changePercent ?? null,
      };
    })
    .filter((item) => item.ticker && item.quantity > 0 && item.avgPrice > 0);
}

function loadSavedItems(key, type = "stocks") {
  try {
    const raw = localStorage.getItem(key) || sessionStorage.getItem(key);
    const parsed = raw ? JSON.parse(raw) : [];
    return type === "holdings" ? normalizeHoldings(parsed) : normalizeSavedItems(parsed);
  } catch {
    return [];
  }
}

function persistItems(key, items) {
  try {
    const safeItems = Array.isArray(items) ? items : [];
    const raw = JSON.stringify(safeItems);
    localStorage.setItem(key, raw);
    sessionStorage.setItem(key, raw);
  } catch {}
}

function formatPrice(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "...";
  return n.toLocaleString("ko-KR", {
    minimumFractionDigits: n < 1000 && !Number.isInteger(n) ? 2 : 0,
    maximumFractionDigits: 2,
  });
}

function formatSigned(value, maxDigits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "...";
  const abs = Math.abs(n).toLocaleString("ko-KR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: maxDigits,
  });
  if (n > 0) return `+${abs}`;
  if (n < 0) return `-${abs}`;
  return abs;
}

function cleanHtml(text) {
  return String(text || "")
    .replace(/<[^>]+>/g, "")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .trim();
}

function ChangeText({ change, changePercent }) {
  if (change == null || changePercent == null) return <span>...</span>;
  return (
    <span>
      {formatSigned(change)} ({formatSigned(changePercent)}%)
    </span>
  );
}

function PriceCard({ item, removable = false, onRemove, onSelect, selected = false }) {
  const cp = Number(item?.changePercent ?? 0);
  const cls = cp > 0 ? "up" : cp < 0 ? "down" : "flat";

  function handleCardClick() {
    onSelect?.(item);
  }

  function handleRemoveClick(e) {
    e.stopPropagation();
    onRemove?.();
  }

  return (
    <article
      className={`price-card clickable ${selected ? "selected" : ""}`}
      onClick={handleCardClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          handleCardClick();
        }
      }}
    >
      <div className="card-head">
        <div className="card-title-wrap">
          <div className="card-title">{item?.name || item?.ticker}</div>
          <div className="card-subtitle">{item?.ticker}</div>
        </div>
        {removable ? (
          <button type="button" className="mini-btn" onClick={handleRemoveClick}>
            삭제
          </button>
        ) : null}
      </div>
      <div className="card-price">{formatPrice(item?.price)}</div>
      <div className={`card-change ${cls}`}>
        <ChangeText change={item?.change} changePercent={item?.changePercent} />
      </div>
    </article>
  );
}

function HoldingCard({ item, onRemove }) {
  const currentPrice = Number(item?.price);
  const quantity = Number(item?.quantity);
  const avgPrice = Number(item?.avgPrice);
  const buyTotal = Number.isFinite(quantity) && Number.isFinite(avgPrice) ? quantity * avgPrice : null;
  const evalTotal = Number.isFinite(quantity) && Number.isFinite(currentPrice) ? quantity * currentPrice : null;
  const returnRate =
    Number.isFinite(currentPrice) && Number.isFinite(avgPrice) && avgPrice > 0
      ? ((currentPrice - avgPrice) / avgPrice) * 100
      : null;
  const cls = Number(returnRate ?? 0) > 0 ? "up" : Number(returnRate ?? 0) < 0 ? "down" : "flat";

  return (
    <article className="holding-card">
      <div className="card-head">
        <div className="card-title-wrap">
          <div className="card-title">{item?.name || item?.ticker}</div>
          <div className="card-subtitle">{item?.ticker}</div>
        </div>
        <button type="button" className="mini-btn" onClick={onRemove}>
          삭제
        </button>
      </div>

      <div className="holding-top-grid">
        <div className="holding-cell">
          <div className="holding-label">종목</div>
          <div className="holding-value">{item?.name || item?.ticker}</div>
        </div>
        <div className="holding-cell">
          <div className="holding-label">현재주가</div>
          <div className="holding-value">{formatPrice(item?.price)}</div>
        </div>
        <div className="holding-cell">
          <div className="holding-label">수익률</div>
          <div className={`holding-value ${cls}`}>{returnRate == null ? "..." : `${formatSigned(returnRate)}%`}</div>
        </div>
      </div>

      <div className="holding-bottom-grid">
        <div className="holding-cell">
          <div className="holding-label">보유수량</div>
          <div className="holding-value">{formatPrice(item?.quantity)}</div>
        </div>
        <div className="holding-cell">
          <div className="holding-label">평균단가</div>
          <div className="holding-value">{formatPrice(item?.avgPrice)}</div>
        </div>
        <div className="holding-cell">
          <div className="holding-label">보유총액</div>
          <div className="holding-value">{formatPrice(buyTotal)}</div>
        </div>
        <div className="holding-cell">
          <div className="holding-label">총평가액</div>
          <div className="holding-value">{formatPrice(evalTotal)}</div>
        </div>
      </div>
    </article>
  );
}

function NewsCard({ item }) {
  return (
    <a className="news-card" href={item?.link || "#"} target="_blank" rel="noreferrer">
      <div className="news-title">{cleanHtml(item?.title) || "제목 없음"}</div>
      {item?.description ? <div className="news-desc">{cleanHtml(item.description)}</div> : null}
    </a>
  );
}

function LineChart({ data = [] }) {
  if (!Array.isArray(data) || data.length < 2) {
    return <div className="chart-empty">차트 데이터가 없습니다.</div>;
  }

  const width = 640;
  const height = 240;
  const padX = 18;
  const padTop = 18;
  const padBottom = 30;
  const closes = data.map((d) => Number(d.close)).filter((n) => Number.isFinite(n));
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const range = max - min || 1;

  const first = Number(data[0]?.close);
  const last = Number(data[data.length - 1]?.close);
  const diff = last - first;
  const diffPct = first ? (diff / first) * 100 : 0;
  const trendClass = last > first ? "up" : last < first ? "down" : "flat";
  const stroke = trendClass === "up" ? "#f87171" : trendClass === "down" ? "#60a5fa" : "#cbd5e1";
  const fillStart = trendClass === "up" ? "rgba(248,113,113,0.28)" : trendClass === "down" ? "rgba(96,165,250,0.24)" : "rgba(203,213,225,0.18)";
  const fillEnd = trendClass === "up" ? "rgba(248,113,113,0.03)" : trendClass === "down" ? "rgba(96,165,250,0.03)" : "rgba(203,213,225,0.03)";

  const pointPairs = data.map((d, idx) => {
    const x = padX + (idx * (width - padX * 2)) / Math.max(data.length - 1, 1);
    const y = height - padBottom - ((Number(d.close) - min) / range) * (height - padTop - padBottom);
    return [x, y];
  });

  const linePath = pointPairs.map(([x, y], idx) => `${idx === 0 ? "M" : "L"} ${x} ${y}`).join(" ");
  const areaPath = `${linePath} L ${pointPairs[pointPairs.length - 1][0]} ${height - padBottom} L ${pointPairs[0][0]} ${height - padBottom} Z`;

  const lastPoint = pointPairs[pointPairs.length - 1];
  const labels = [max, (max + min) / 2, min];
  const gradientId = `chart-gradient-${trendClass}`;

  return (
    <div className="chart-svg-wrap">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          gap: 12,
          marginBottom: 10,
          flexWrap: "wrap",
        }}
      >
        <div>
          <div style={{ fontSize: 28, fontWeight: 800, lineHeight: 1.1 }}>{formatPrice(last)}</div>
          <div style={{ marginTop: 6, fontSize: 14, fontWeight: 700, color: stroke }}>
            {formatSigned(diff)} ({formatSigned(diffPct)}%)
          </div>
        </div>
        <div
          style={{
            padding: "8px 10px",
            borderRadius: 12,
            border: `1px solid ${stroke}55`,
            background: trendClass === "up" ? "rgba(127,29,29,0.22)" : trendClass === "down" ? "rgba(30,58,138,0.22)" : "rgba(51,65,85,0.3)",
            color: "#e5eefc",
            fontSize: 12,
            lineHeight: 1.5,
            minWidth: 110,
          }}
        >
          <div>최고 {formatPrice(max)}</div>
          <div>최저 {formatPrice(min)}</div>
        </div>
      </div>

      <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" preserveAspectRatio="none">
        <defs>
          <linearGradient id={gradientId} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={fillStart} />
            <stop offset="100%" stopColor={fillEnd} />
          </linearGradient>
        </defs>

        {labels.map((value, idx) => {
          const y = padTop + (idx * (height - padTop - padBottom)) / 2;
          return (
            <g key={idx}>
              <line
                x1={padX}
                y1={y}
                x2={width - padX}
                y2={y}
                stroke="rgba(148,163,184,0.18)"
                strokeWidth="1"
                strokeDasharray="4 4"
              />
              <text x={width - padX} y={y - 6} fill="rgba(191,219,254,0.9)" fontSize="12" textAnchor="end">
                {formatPrice(value)}
              </text>
            </g>
          );
        })}

        <path d={areaPath} fill={`url(#${gradientId})`} />
        <path d={linePath} fill="none" stroke={stroke} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" />
        <circle cx={lastPoint[0]} cy={lastPoint[1]} r="5" fill={stroke} />
        <circle cx={lastPoint[0]} cy={lastPoint[1]} r="10" fill={stroke} opacity="0.15" />
      </svg>

      <div className="chart-date-row">
        <span>{data[0]?.date}</span>
        <span>{data[data.length - 1]?.date}</span>
      </div>
    </div>
  );
}

export default function App() {
  const [activeTab, setActiveTab] = useState("stocks");
  const [marketItems, setMarketItems] = useState([]);
  const [stockItems, setStockItems] = useState(loadSavedItems(DEFAULT_STOCKS_KEY));
  const [newsItems, setNewsItems] = useState([]);
  const [extraStocks, setExtraStocks] = useState(loadSavedItems(EXTRA_STOCKS_KEY));
  const [holdings, setHoldings] = useState(loadSavedItems(HOLDINGS_KEY, "holdings"));
  const [searchKeyword, setSearchKeyword] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  const [investKeyword, setInvestKeyword] = useState("");
  const [investResults, setInvestResults] = useState([]);
  const [isInvestSearching, setIsInvestSearching] = useState(false);
  const [selectedInvestStock, setSelectedInvestStock] = useState(null);
  const [holdingForm, setHoldingForm] = useState({
    ticker: "",
    name: "",
    quantity: "",
    avgPrice: "",
  });
  const [lastUpdated, setLastUpdated] = useState("");
  const [errorText, setErrorText] = useState("");
  const [selectedStock, setSelectedStock] = useState(null);
  const [chartPeriod, setChartPeriod] = useState("1mo");
  const [chartItems, setChartItems] = useState([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [defaultEditMode, setDefaultEditMode] = useState(false);

  const stockItemsRef = useRef(stockItems);
  const extraStocksRef = useRef(extraStocks);
  const holdingsRef = useRef(holdings);
  const searchTimerRef = useRef(null);
  const investSearchTimerRef = useRef(null);
  const refreshSeqRef = useRef({ holdings: 0 });
  const holdRefreshPauseUntilRef = useRef(0);

  useEffect(() => {
    stockItemsRef.current = stockItems;
    persistItems(DEFAULT_STOCKS_KEY, stockItems);
  }, [stockItems]);

  useEffect(() => {
    extraStocksRef.current = extraStocks;
    persistItems(EXTRA_STOCKS_KEY, extraStocks);
  }, [extraStocks]);

  useEffect(() => {
    holdingsRef.current = holdings;
    persistItems(HOLDINGS_KEY, holdings);
  }, [holdings]);

  useEffect(() => {
    const restoredDefault = loadSavedItems(DEFAULT_STOCKS_KEY);
    if (restoredDefault.length) {
      setStockItems((prev) => (prev.length ? prev : restoredDefault));
    }
    const restoredExtra = loadSavedItems(EXTRA_STOCKS_KEY);
    if (restoredExtra.length) {
      setExtraStocks((prev) => (prev.length ? prev : restoredExtra));
    }
    const restoredHoldings = loadSavedItems(HOLDINGS_KEY, "holdings");
    if (restoredHoldings.length) {
      setHoldings((prev) => (prev.length ? prev : restoredHoldings));
    }
  }, []);

  function extractItems(data) {
    if (Array.isArray(data)) return data;
    if (Array.isArray(data?.items)) return data.items;
    if (Array.isArray(data?.results)) return data.results;
    return [];
  }

  async function searchStocksApi(query) {
    const data = await fetchJson(`/search-stock?q=${encodeURIComponent(query)}`);
    return extractItems(data);
  }

  async function fetchJson(path) {
    const response = await fetch(`${API_BASE}${path}`);
    if (!response.ok) {
      const message = await response.text().catch(() => "");
      throw new Error(`${path} ${response.status} ${message}`.trim());
    }
    return response.json();
  }

  async function fetchMarket() {
    try {
      const data = await fetchJson("/market");
      setMarketItems(extractItems(data));
      return true;
    } catch (error) {
      console.error("fetchMarket", error);
      setMarketItems([]);
      return false;
    }
  }

  async function fetchDefaultStocksFromBackend() {
    try {
      const data = await fetchJson("/stocks");
      const items = normalizeSavedItems(extractItems(data));
      if (items.length) {
        setStockItems((prev) => {
          if (prev.length) return prev;
          persistItems(DEFAULT_STOCKS_KEY, items);
          return items;
        });
      }
      return true;
    } catch (error) {
      console.error("fetchDefaultStocksFromBackend", error);
      return false;
    }
  }

  async function fetchNews() {
    try {
      const data = await fetchJson("/news");
      setNewsItems(extractItems(data));
      return true;
    } catch (error) {
      console.error("fetchNews", error);
      setNewsItems([]);
      return false;
    }
  }

  async function fetchSingleStockQuote(ticker) {
    try {
      return await fetchJson(`/quote/${encodeURIComponent(ticker)}`);
    } catch (error) {
      console.error("fetchSingleStockQuote", ticker, error);
      return null;
    }
  }

  async function refreshList(items, setter, storageKey) {
    if (!items.length) return true;

    const updated = await Promise.all(
      items.map(async (item) => {
        const fresh = await fetchSingleStockQuote(item.ticker);
        if (fresh && fresh.price != null) {
          return {
            ...item,
            ...fresh,
            ticker: item.ticker,
            name: fresh.name || item.name,
          };
        }
        return item;
      })
    );

    if (storageKey === DEFAULT_STOCKS_KEY) stockItemsRef.current = updated;
    if (storageKey === EXTRA_STOCKS_KEY) extraStocksRef.current = updated;
    setter(updated);
    persistItems(storageKey, updated);
    return true;
  }

  async function refreshHoldings() {
    if (Date.now() < holdRefreshPauseUntilRef.current) return true;

    const items = holdingsRef.current;
    if (!items.length) return true;

    const requestId = ++refreshSeqRef.current.holdings;

    const updated = await Promise.all(
      items.map(async (item) => {
        const fresh = await fetchSingleStockQuote(item.ticker);
        if (fresh && fresh.price != null) {
          return {
            ...item,
            name: fresh.name || item.name,
            price: fresh.price,
            change: fresh.change,
            changePercent: fresh.changePercent,
          };
        }
        return item;
      })
    );

    if (requestId !== refreshSeqRef.current.holdings) return true;
    if (Date.now() < holdRefreshPauseUntilRef.current) return true;

    holdingsRef.current = updated;
    setHoldings(updated);
    persistItems(HOLDINGS_KEY, updated);
    return true;
  }

  async function refreshDefaultStocks() {
    await refreshList(stockItemsRef.current, setStockItems, DEFAULT_STOCKS_KEY);
  }

  async function refreshExtraStocks() {
    await refreshList(extraStocksRef.current, setExtraStocks, EXTRA_STOCKS_KEY);
  }

  async function refreshAll() {
    setErrorText("");

    const okMarket = await fetchMarket();
    const okNews = await fetchNews();
    const okDefault = await refreshDefaultStocks();
    const okExtra = await refreshExtraStocks();
    const okHoldings = await refreshHoldings();

    const successes = [okMarket, okNews, okDefault, okExtra, okHoldings].filter(Boolean).length;
    if (successes === 0) {
      setErrorText("데이터 연결 실패");
    }

    setLastUpdated(
      new Date().toLocaleTimeString("ko-KR", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    );
  }

  useEffect(() => {
    fetchDefaultStocksFromBackend();
  }, []);

  useEffect(() => {
    refreshAll();
    const timer = window.setInterval(refreshAll, REFRESH_MS);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    window.clearTimeout(searchTimerRef.current);

    if (!searchKeyword.trim()) {
      setSearchResults([]);
      setIsSearching(false);
      return;
    }

    searchTimerRef.current = window.setTimeout(async () => {
      try {
        setIsSearching(true);
        const items = await searchStocksApi(searchKeyword.trim());
        setSearchResults(items);
      } catch (error) {
        console.error("search-stock", error);
        setSearchResults([]);
      } finally {
        setIsSearching(false);
      }
    }, 250);

    return () => window.clearTimeout(searchTimerRef.current);
  }, [searchKeyword]);

  useEffect(() => {
    window.clearTimeout(investSearchTimerRef.current);

    if (!investKeyword.trim()) {
      setInvestResults([]);
      setIsInvestSearching(false);
      return;
    }

    investSearchTimerRef.current = window.setTimeout(async () => {
      try {
        setIsInvestSearching(true);
        const items = await searchStocksApi(investKeyword.trim());
        setInvestResults(items);
      } catch (error) {
        console.error("invest search-stock", error);
        setInvestResults([]);
      } finally {
        setIsInvestSearching(false);
      }
    }, 250);

    return () => window.clearTimeout(investSearchTimerRef.current);
  }, [investKeyword]);

  async function fetchChart(ticker, period = chartPeriod) {
    if (!ticker) return;
    try {
      setChartLoading(true);
      const data = await fetchJson(`/chart/${encodeURIComponent(ticker)}?period=${encodeURIComponent(period)}`);
      setChartItems(Array.isArray(data?.items) ? data.items : []);
    } catch (error) {
      console.error("chart", error);
      setChartItems([]);
    } finally {
      setChartLoading(false);
    }
  }

  useEffect(() => {
    if (selectedStock?.ticker) {
      fetchChart(selectedStock.ticker, chartPeriod);
    }
  }, [chartPeriod]);

  function stockExistsAnywhere(ticker) {
    return stockItemsRef.current.some((stock) => stock.ticker === ticker) || extraStocksRef.current.some((stock) => stock.ticker === ticker);
  }

  async function addStockTo(target, item) {
    const ticker = normalizeTicker(item?.ticker || searchKeyword);
    if (!ticker || stockExistsAnywhere(ticker)) {
      setSearchKeyword("");
      setSearchResults([]);
      return;
    }

    const fresh = await fetchSingleStockQuote(ticker);
    const newItem = {
      ticker,
      name: fresh?.name || item?.name || ticker,
      price: fresh?.price ?? null,
      change: fresh?.change ?? null,
      changePercent: fresh?.changePercent ?? null,
    };

    if (target === "default") {
      setStockItems((prev) => {
        const next = [...prev, newItem];
        stockItemsRef.current = next;
        persistItems(DEFAULT_STOCKS_KEY, next);
        return next;
      });
    } else {
      setExtraStocks((prev) => {
        const next = [...prev, newItem];
        extraStocksRef.current = next;
        persistItems(EXTRA_STOCKS_KEY, next);
        return next;
      });
    }

    setSearchKeyword("");
    setSearchResults([]);
  }

  function handleRemoveDefaultStock(ticker) {
    setStockItems((prev) => {
      const next = prev.filter((item) => item.ticker !== ticker);
      stockItemsRef.current = next;
      persistItems(DEFAULT_STOCKS_KEY, next);
      return next;
    });
    if (selectedStock?.ticker === ticker) {
      setSelectedStock(null);
      setChartItems([]);
    }
  }

  function handleRemoveExtraStock(ticker) {
    setExtraStocks((prev) => {
      const next = prev.filter((item) => item.ticker !== ticker);
      extraStocksRef.current = next;
      persistItems(EXTRA_STOCKS_KEY, next);
      return next;
    });
    if (selectedStock?.ticker === ticker) {
      setSelectedStock(null);
      setChartItems([]);
    }
  }

  function handleSelectHolding(item) {
    setSelectedInvestStock(item);
    setHoldingForm({
      ticker: item.ticker,
      name: item.name,
      quantity: "",
      avgPrice: "",
    });
    setInvestKeyword(item.name || item.ticker);
    setInvestResults([]);
  }

  async function handleAddHolding() {
    const quantity = toNumber(holdingForm.quantity);
    const avgPrice = toNumber(holdingForm.avgPrice);

    if (!quantity || quantity <= 0 || !avgPrice || avgPrice <= 0) {
      alert("보유수량, 평균단가를 확인해 주세요.");
      return;
    }

    let picked = selectedInvestStock;

    if (!picked) {
      const query = investKeyword.trim() || holdingForm.ticker.trim() || holdingForm.name.trim();
      if (!query) {
        alert("종목을 먼저 선택하세요.");
        return;
      }

      const results = await searchStocksApi(query);
      const norm = normalizeTicker(query);
      picked =
        results.find((item) =>
          normalizeTicker(item.ticker) === norm ||
          String(item.name || "").trim() === query
        ) ||
        results[0] ||
        null;
    }

    if (!picked?.ticker) {
      alert("종목을 먼저 선택하세요.");
      return;
    }

    const ticker = normalizeTicker(picked.ticker);
    const fresh = await fetchSingleStockQuote(ticker);
    const newItem = {
      ticker,
      name: fresh?.name || picked.name || ticker,
      quantity,
      avgPrice,
      price: fresh?.price ?? null,
      change: fresh?.change ?? null,
      changePercent: fresh?.changePercent ?? null,
    };

    refreshSeqRef.current.holdings += 1;
    holdRefreshPauseUntilRef.current = Date.now() + 4000;

    setHoldings((prev) => {
      const existingIndex = prev.findIndex((item) => item.ticker === ticker);
      const next = [...prev];
      if (existingIndex >= 0) next[existingIndex] = newItem;
      else next.unshift(newItem);
      holdingsRef.current = next;
      persistItems(HOLDINGS_KEY, next);
      return next;
    });

    setSelectedInvestStock(null);
    setHoldingForm({ ticker: "", name: "", quantity: "", avgPrice: "" });
    setInvestKeyword("");
    setInvestResults([]);
  }

  function handleRemoveHolding(ticker) {
    refreshSeqRef.current.holdings += 1;
    holdRefreshPauseUntilRef.current = Date.now() + 1500;

    setHoldings((prev) => {
      const next = prev.filter((item) => item.ticker !== ticker);
      holdingsRef.current = next;
      persistItems(HOLDINGS_KEY, next);
      return next;
    });
  }

  async function handleSelectStock(item) {
    const same = selectedStock?.ticker === item.ticker;
    if (same) {
      setSelectedStock(null);
      setChartItems([]);
      return;
    }
    setSelectedStock(item);
    await fetchChart(item.ticker, chartPeriod);
  }

  const futureIndexes = useMemo(
    () => marketItems.filter((item) => ["DOW_FUT", "NASDAQ_FUT"].includes(item.key)),
    [marketItems]
  );

  const mainIndexes = useMemo(
    () => marketItems.filter((item) => ["KOSPI", "KOSDAQ", "DOW", "NASDAQ"].includes(item.key)),
    [marketItems]
  );

  const commodityItems = useMemo(
    () => marketItems.filter((item) => ["GOLD", "WTI", "BRENT"].includes(item.key)),
    [marketItems]
  );

  const fxItems = useMemo(
    () => marketItems.filter((item) => ["USDKRW", "JPYKRW"].includes(item.key)),
    [marketItems]
  );

  const directAddCandidate = useMemo(() => {
    const text = normalizeTicker(searchKeyword);
    if (!text) return null;
    if (stockExistsAnywhere(text)) return null;
    const simpleTicker = /^[A-Z][A-Z0-9.\-]{0,9}$/.test(text);
    const krCode = /^\d{6}$/.test(text);
    if (!simpleTicker && !krCode) return null;
    return {
      ticker: text,
      name: `${text} 직접추가`,
    };
  }, [searchKeyword, stockItems, extraStocks]);

  const holdingSummary = useMemo(() => {
    const buyTotal = holdings.reduce((sum, item) => {
      const quantity = Number(item.quantity);
      const avgPrice = Number(item.avgPrice);
      if (!Number.isFinite(quantity) || !Number.isFinite(avgPrice)) return sum;
      return sum + quantity * avgPrice;
    }, 0);
    const evalTotal = holdings.reduce((sum, item) => {
      const quantity = Number(item.quantity);
      const price = Number(item.price);
      if (!Number.isFinite(quantity) || !Number.isFinite(price)) return sum;
      return sum + quantity * price;
    }, 0);
    const returnRate = buyTotal > 0 ? ((evalTotal - buyTotal) / buyTotal) * 100 : null;
    return { buyTotal, evalTotal, returnRate };
  }, [holdings]);

  return (
    <div className="app-shell">
      <div className="app-container">
        <header className="app-header">
          <h1>증권 대시보드</h1>
          <p className="header-desc">지수와 종목을 5초마다 자동으로 갱신합니다.</p>
          <div className="status-box">
            <div>마지막 업데이트</div>
            <strong>{lastUpdated || "불러오는 중"}</strong>
          </div>
          {errorText ? <div className="error-box">{errorText}</div> : null}
        </header>

        <nav className="top-tabs four-tabs">
          <button type="button" className={activeTab === "indexes" ? "tab active" : "tab"} onClick={() => setActiveTab("indexes")}>지수</button>
          <button type="button" className={activeTab === "stocks" ? "tab active" : "tab"} onClick={() => setActiveTab("stocks")}>종목</button>
          <button type="button" className={activeTab === "invest" ? "tab active" : "tab"} onClick={() => setActiveTab("invest")}>투자</button>
          <button type="button" className={activeTab === "news" ? "tab active" : "tab"} onClick={() => setActiveTab("news")}>뉴스</button>
        </nav>

        <main className="tab-body">
          {activeTab === "indexes" && (
            <section>
              <h2 className="section-title">미국 선물</h2>
              <div className="card-grid">
                {futureIndexes.length ? futureIndexes.map((item) => (
                  <PriceCard key={item.key} item={item} />
                )) : <div className="empty-box">표시할 선물 데이터가 없습니다.</div>}
              </div>

              <h2 className="section-title">주요 지수</h2>
              <div className="card-grid">
                {mainIndexes.length ? mainIndexes.map((item) => (
                  <PriceCard key={item.key} item={item} />
                )) : <div className="empty-box">표시할 지수 데이터가 없습니다.</div>}
              </div>

              <h2 className="section-title">원자재</h2>
              <div className="card-grid">
                {commodityItems.length ? commodityItems.map((item) => (
                  <PriceCard key={item.key} item={item} />
                )) : <div className="empty-box">표시할 원자재 데이터가 없습니다.</div>}
              </div>

              <h2 className="section-title">환율</h2>
              <div className="card-grid">
                {fxItems.length ? fxItems.map((item) => (
                  <PriceCard key={item.key} item={item} />
                )) : <div className="empty-box">표시할 환율 데이터가 없습니다.</div>}
              </div>
            </section>
          )}

          {activeTab === "stocks" && (
            <section>
              <div className="card-head" style={{ margin: "6px 0 12px" }}>
                <div>
                  <div className="section-title" style={{ margin: 0 }}>종목 관리</div>
                  <div className="card-subtitle">기본 종목 편집을 켜면 검색 결과를 기본 종목으로 넣습니다.</div>
                </div>
                <button type="button" className="mini-btn" onClick={() => setDefaultEditMode((prev) => !prev)}>
                  {defaultEditMode ? "편집완료" : "기본종목 편집"}
                </button>
              </div>

              <div className="search-box">
                <input
                  type="text"
                  value={searchKeyword}
                  placeholder={defaultEditMode ? "편집중: 검색 결과를 기본 종목으로 추가" : "종목명, 미국티커, 6자리 종목코드 검색"}
                  onChange={(e) => setSearchKeyword(e.target.value)}
                />
              </div>

              {(searchKeyword.trim() || isSearching || searchResults.length > 0) && (
                <div className="search-results">
                  {directAddCandidate ? (
                    <button
                      type="button"
                      className="search-row direct-row"
                      onClick={() => addStockTo(defaultEditMode ? "default" : "extra", directAddCandidate)}
                    >
                      <span>{directAddCandidate.name}</span>
                      <span className="muted">{defaultEditMode ? "기본종목 추가" : "추가종목 추가"}</span>
                    </button>
                  ) : null}
                  {isSearching ? (
                    <div className="search-row">검색 중...</div>
                  ) : searchResults.length ? (
                    searchResults.map((item) => (
                      <button
                        type="button"
                        className="search-row"
                        key={`${item.ticker}-${item.name}`}
                        onClick={() => addStockTo(defaultEditMode ? "default" : "extra", item)}
                      >
                        <span>{item.name}</span>
                        <span className="muted">{defaultEditMode ? "기본종목 추가" : item.ticker}</span>
                      </button>
                    ))
                  ) : searchKeyword.trim() ? (
                    <div className="search-row">검색 결과가 없으면 직접추가를 눌러도 됩니다.</div>
                  ) : null}
                </div>
              )}

              <div className="card-head" style={{ marginTop: 18, marginBottom: 10 }}>
                <h2 className="section-title" style={{ margin: 0 }}>기본 종목</h2>
                {defaultEditMode ? <div className="card-subtitle">삭제 버튼으로 기본 종목을 정리할 수 있습니다.</div> : null}
              </div>
              <div className="card-grid">
                {stockItems.length ? (
                  stockItems.map((item) => (
                    <PriceCard
                      key={item.ticker}
                      item={item}
                      removable={defaultEditMode}
                      onRemove={() => handleRemoveDefaultStock(item.ticker)}
                      onSelect={handleSelectStock}
                      selected={selectedStock?.ticker === item.ticker}
                    />
                  ))
                ) : (
                  <div className="empty-box">기본 종목이 없습니다. 편집 모드에서 종목을 추가해 보세요.</div>
                )}
              </div>

              <h2 className="section-title">추가 종목</h2>
              <div className="card-grid">
                {extraStocks.length ? (
                  extraStocks.map((item) => (
                    <PriceCard
                      key={item.ticker}
                      item={item}
                      removable
                      onRemove={() => handleRemoveExtraStock(item.ticker)}
                      onSelect={handleSelectStock}
                      selected={selectedStock?.ticker === item.ticker}
                    />
                  ))
                ) : (
                  <div className="empty-box">추가한 종목이 없습니다.</div>
                )}
              </div>

              {selectedStock ? (
                <section className="chart-panel">
                  <div className="chart-panel-head">
                    <div>
                      <div className="chart-title">{selectedStock.name} 차트</div>
                      <div className="chart-subtitle">{selectedStock.ticker}</div>
                    </div>
                    <div className="period-tabs">
                      {[
                        ["1mo", "1M"],
                        ["3mo", "3M"],
                        ["6mo", "6M"],
                      ].map(([value, label]) => (
                        <button
                          key={value}
                          type="button"
                          className={chartPeriod === value ? "period-btn active" : "period-btn"}
                          onClick={() => setChartPeriod(value)}
                        >
                          {label}
                        </button>
                      ))}
                    </div>
                  </div>
                  {chartLoading ? <div className="chart-empty">차트 불러오는 중...</div> : <LineChart data={chartItems} />}
                </section>
              ) : null}
            </section>
          )}

          {activeTab === "invest" && (
            <section>
              <div className="card-head" style={{ margin: "6px 0 12px" }}>
                <div>
                  <div className="section-title" style={{ margin: 0 }}>투자</div>
                  <div className="card-subtitle">종목, 보유수량, 평균단가를 입력하면 현재주가와 수익률을 자동 계산합니다.</div>
                </div>
              </div>

              <section className="invest-form-panel">
                <div className="invest-form-grid">
                  <div className="invest-field invest-field-wide">
                    <label>종목 검색</label>
                    <input
                      type="text"
                      value={investKeyword}
                      placeholder="종목명, 미국티커, 6자리 종목코드 검색"
                      onChange={(e) => {
                        setInvestKeyword(e.target.value);
                        setSelectedInvestStock(null);
                        setHoldingForm((prev) => ({ ...prev, ticker: "", name: "" }));
                      }}
                    />
                  </div>

                  <div className="invest-field">
                    <label>보유수량</label>
                    <input
                      type="number"
                      inputMode="decimal"
                      value={holdingForm.quantity}
                      placeholder="예: 10"
                      onChange={(e) => setHoldingForm((prev) => ({ ...prev, quantity: e.target.value }))}
                    />
                  </div>

                  <div className="invest-field">
                    <label>평균단가</label>
                    <input
                      type="number"
                      inputMode="decimal"
                      value={holdingForm.avgPrice}
                      placeholder="예: 178000"
                      onChange={(e) => setHoldingForm((prev) => ({ ...prev, avgPrice: e.target.value }))}
                    />
                  </div>
                </div>

                {(investKeyword.trim() || isInvestSearching || investResults.length > 0) && (
                  <div className="search-results invest-results">
                    {isInvestSearching ? (
                      <div className="search-row">검색 중...</div>
                    ) : investResults.length ? (
                      investResults.map((item) => (
                        <button
                          type="button"
                          className="search-row"
                          key={`invest-${item.ticker}-${item.name}`}
                          onClick={() => {
                            handleSelectHolding(item);
                          }}
                        >
                          <span>{item.name}</span>
                          <span className="muted">{item.ticker}</span>
                        </button>
                      ))
                    ) : investKeyword.trim() ? (
                      <div className="search-row">검색 결과가 없습니다.</div>
                    ) : null}
                  </div>
                )}

                <div className="invest-selected-box">
                  <div className="muted">
                    선택된 종목:{" "}
                    <strong>{selectedInvestStock ? `${selectedInvestStock.name} (${selectedInvestStock.ticker})` : "아직 없음"}</strong>
                  </div>
                  <button type="button" className="mini-btn" onClick={handleAddHolding}>
                    보유종목 저장
                  </button>
                </div>
              </section>

              <section className="invest-summary-panel">
                <div className="summary-item">
                  <div className="holding-label">매수총액</div>
                  <div className="summary-value">{formatPrice(holdingSummary.buyTotal)}</div>
                </div>
                <div className="summary-item">
                  <div className="holding-label">평가총액</div>
                  <div className="summary-value">{formatPrice(holdingSummary.evalTotal)}</div>
                </div>
                <div className="summary-item">
                  <div className="holding-label">전체 수익률</div>
                  <div className={`summary-value ${Number(holdingSummary.returnRate ?? 0) > 0 ? "up" : Number(holdingSummary.returnRate ?? 0) < 0 ? "down" : ""}`}>
                    {holdingSummary.returnRate == null ? "..." : `${formatSigned(holdingSummary.returnRate)}%`}
                  </div>
                </div>
              </section>

              <div className="holding-list">
                {holdings.length ? (
                  holdings.map((item) => (
                    <HoldingCard key={item.ticker} item={item} onRemove={() => handleRemoveHolding(item.ticker)} />
                  ))
                ) : (
                  <div className="empty-box">보유 종목이 없습니다. 종목을 선택하고 보유수량과 평균단가를 입력해 보세요.</div>
                )}
              </div>
            </section>
          )}

          {activeTab === "news" && (
            <section>
              <div className="news-list">
                {newsItems.length ? (
                  newsItems.map((item, idx) => <NewsCard key={`${item.link || idx}-${idx}`} item={item} />)
                ) : (
                  <div className="empty-box">표시할 뉴스가 없습니다.</div>
                )}
              </div>
            </section>
          )}
        </main>
      </div>
    </div>
  );
}
