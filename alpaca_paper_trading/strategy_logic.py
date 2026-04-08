import math
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr


START_DATE = "2006-01-01"
DATA_TICKERS = ["SPY", "SHY", "DBC", "DBB", "DBA"]
TRADE_TICKERS = ["SPY", "SHY"]
LIVE_BUY_BUFFER = 1.001
FRED_RETRY_COUNT = 3
FRED_RETRY_DELAY_SECONDS = 2


@dataclass
class StrategyData:
    opens: pd.DataFrame
    closes: pd.DataFrame
    dgs2: pd.Series


@dataclass
class LiveSignal:
    as_of_date: pd.Timestamp
    next_trade_date: pd.Timestamp
    score: int
    target_spy_weight: float
    target_shy_weight: float
    action: str
    stop_price: Optional[float]
    account_value: float
    ret_DBC: float
    ret_DBB: float
    ret_DBA: float
    chg_DGS2: float
    spy_above_ma50: bool

    def to_frame(self) -> pd.DataFrame:
        row = asdict(self)
        return pd.DataFrame([row])


def next_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    return ts + pd.offsets.BDay(1)


def use_completed_session_only(
    data: StrategyData,
    current_timestamp: pd.Timestamp,
    market_is_open: bool,
) -> StrategyData:
    if data.closes.empty:
        raise ValueError("No price history available.")

    current_session_date = pd.Timestamp(current_timestamp).tz_localize(None).normalize()
    latest_data_date = pd.Timestamp(data.closes.index[-1]).normalize()

    if market_is_open and latest_data_date >= current_session_date:
        trimmed_opens = data.opens.iloc[:-1].copy()
        trimmed_closes = data.closes.iloc[:-1].copy()
        trimmed_dgs2 = data.dgs2.reindex(trimmed_closes.index)
        if len(trimmed_closes.index) < 60:
            raise ValueError(
                "Insufficient completed-session history after dropping the in-progress trading day."
            )
        return StrategyData(opens=trimmed_opens, closes=trimmed_closes, dgs2=trimmed_dgs2)

    return data


def load_dgs2_with_retry_and_cache(
    closes_index: pd.DatetimeIndex,
    start_date: str,
    end_date: str,
    cache_dir: Optional[Path] = None,
) -> pd.Series:
    cache_path = None
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / "dgs2_cache.csv"

    last_error: Optional[Exception] = None
    for attempt in range(1, FRED_RETRY_COUNT + 1):
        try:
            dgs2 = pdr.DataReader("DGS2", "fred", start_date, end_date)["DGS2"]
            dgs2 = pd.to_numeric(dgs2, errors="coerce")
            if cache_path is not None:
                dgs2.to_frame(name="DGS2").to_csv(cache_path)
            return dgs2.reindex(closes_index).ffill()
        except Exception as exc:
            last_error = exc
            if attempt < FRED_RETRY_COUNT:
                time.sleep(FRED_RETRY_DELAY_SECONDS)

    if cache_path is not None and cache_path.exists():
        cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)["DGS2"]
        cached = pd.to_numeric(cached, errors="coerce")
        return cached.reindex(closes_index).ffill()

    raise RuntimeError(
        f"Failed to download DGS2 from FRED after {FRED_RETRY_COUNT} attempts and no cache was available."
    ) from last_error


def fetch_strategy_data(
    start_date: str = START_DATE,
    end_date: Optional[str] = None,
    cache_dir: Optional[Path] = None,
) -> StrategyData:
    end_date = end_date or date.today().isoformat()
    prices = yf.download(
        tickers=DATA_TICKERS,
        start=start_date,
        end=(pd.Timestamp(end_date) + pd.Timedelta(days=1)).date().isoformat(),
        auto_adjust=False,
        progress=False,
        actions=False,
        threads=False,
    )
    if prices.empty:
        raise ValueError("No Yahoo Finance price data returned.")
    if not isinstance(prices.columns, pd.MultiIndex):
        raise ValueError("Unexpected Yahoo Finance column layout.")

    opens = prices["Open"].reindex(columns=DATA_TICKERS).sort_index()
    closes = prices["Close"].reindex(columns=DATA_TICKERS).sort_index()
    dgs2 = load_dgs2_with_retry_and_cache(
        closes_index=closes.index,
        start_date=start_date,
        end_date=end_date,
        cache_dir=cache_dir,
    )

    return StrategyData(opens=opens, closes=closes, dgs2=dgs2)


def build_live_signal(
    data: StrategyData,
    account_value: float,
    current_spy_shares: int = 0,
    current_spy_avg_entry_price: Optional[float] = None,
) -> LiveSignal:
    closes = data.closes
    if len(closes.index) < 60:
        raise ValueError("Insufficient history to compute the live signal.")

    latest_date = closes.index[-1]
    ret_dbc = closes["DBC"].pct_change(20).iloc[-1]
    ret_dbb = closes["DBB"].pct_change(20).iloc[-1]
    ret_dba = closes["DBA"].pct_change(20).iloc[-1]
    chg_dgs2 = (data.dgs2 - data.dgs2.shift(20)).iloc[-1]
    ma50_spy = closes["SPY"].rolling(50).mean().iloc[-1]
    spy_close = closes["SPY"].iloc[-1]

    values = [ret_dbc, ret_dbb, ret_dba, chg_dgs2, ma50_spy, spy_close]
    if any(pd.isna(v) for v in values):
        raise ValueError("Latest observation does not have all required inputs.")

    score = int(ret_dbc > 0) + int(ret_dbb > 0) + int(ret_dba < 0) + int(chg_dgs2 < 0)
    spy_above_ma50 = bool(spy_close > ma50_spy)

    if score == 4 and spy_above_ma50:
        target_spy_weight = 1.0
        target_shy_weight = 0.0
    elif score == 3 and spy_above_ma50:
        target_spy_weight = 0.5
        target_shy_weight = 0.5
    else:
        target_spy_weight = 0.0
        target_shy_weight = 1.0

    if ret_dbc <= -0.05:
        target_spy_weight = 0.0
        target_shy_weight = 1.0

    if current_spy_shares > 0 and current_spy_avg_entry_price is not None:
        stop_price = current_spy_avg_entry_price * 0.93
    else:
        stop_price = None

    if current_spy_shares <= 0 and target_spy_weight > 0:
        action = "BUY"
    elif current_spy_shares > 0 and target_spy_weight == 0:
        action = "EXIT"
    elif current_spy_shares > 0 and target_spy_weight == 0.5:
        action = "REDUCE"
    elif current_spy_shares <= 0 and target_spy_weight == 0:
        action = "STAY_DEFENSIVE"
    else:
        action = "HOLD"
    return LiveSignal(
        as_of_date=latest_date,
        next_trade_date=next_business_day(latest_date),
        score=score,
        target_spy_weight=target_spy_weight,
        target_shy_weight=target_shy_weight,
        action=action,
        stop_price=stop_price,
        account_value=account_value,
        ret_DBC=float(ret_dbc),
        ret_DBB=float(ret_dbb),
        ret_DBA=float(ret_dba),
        chg_DGS2=float(chg_dgs2),
        spy_above_ma50=spy_above_ma50,
    )


def latest_reference_prices(data: StrategyData) -> Dict[str, float]:
    closes = data.closes.iloc[-1]
    return {ticker: float(closes[ticker]) for ticker in TRADE_TICKERS}


def compute_target_shares(
    equity: float,
    target_spy_weight: float,
    target_shy_weight: float,
    reference_prices: Dict[str, float],
) -> Dict[str, int]:
    target_weights = {
        "SPY": target_spy_weight,
        "SHY": target_shy_weight,
    }
    target_shares: Dict[str, int] = {}
    for ticker, target_weight in target_weights.items():
        price = reference_prices[ticker]
        if price <= 0:
            raise ValueError(f"Invalid reference price for {ticker}: {price}")
        buffered_price = price * LIVE_BUY_BUFFER
        target_dollars = equity * target_weight
        target_shares[ticker] = math.floor(target_dollars / buffered_price)
    return target_shares


def signal_summary_dict(signal: LiveSignal) -> Dict[str, object]:
    return {
        "as_of_date": signal.as_of_date.date().isoformat(),
        "next_trade_date": signal.next_trade_date.date().isoformat(),
        "score": signal.score,
        "target_spy_weight": signal.target_spy_weight,
        "target_shy_weight": signal.target_shy_weight,
        "action": signal.action,
        "stop_price": signal.stop_price,
        "account_value": signal.account_value,
        "ret_DBC": signal.ret_DBC,
        "ret_DBB": signal.ret_DBB,
        "ret_DBA": signal.ret_DBA,
        "chg_DGS2": signal.chg_DGS2,
        "spy_above_ma50": signal.spy_above_ma50,
    }
