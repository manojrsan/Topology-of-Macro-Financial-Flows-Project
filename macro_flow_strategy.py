import argparse
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr


SLIPPAGE_BPS = 5
SLIPPAGE_RATE = SLIPPAGE_BPS / 10_000
COMMISSION = 0.0
INITIAL_CAPITAL = 100_000.0
TICKERS = ["SPY", "SHY", "DBC", "DBB", "DBA"]
TRADE_TICKERS = ["SPY", "SHY"]
START_DATE = "2006-01-01"


@dataclass
class StrategyData:
    opens: pd.DataFrame
    closes: pd.DataFrame
    dgs2: pd.Series


@dataclass
class BacktestResult:
    signals: pd.DataFrame
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    summary: Dict[str, float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Macro financial flows strategy backtest and live signal generator."
    )
    parser.add_argument(
        "--mode",
        choices=["backtest", "live"],
        default="backtest",
        help="Run a historical backtest or emit the latest live signal.",
    )
    parser.add_argument(
        "--start-date",
        default=START_DATE,
        help="History start date for data pulls. Default: 2006-01-01.",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="History end date for data pulls. Default: today.",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=INITIAL_CAPITAL,
        help="Initial capital for backtest mode.",
    )
    parser.add_argument(
        "--account-value",
        type=float,
        default=None,
        help="Required in live mode. Current account value used for sizing context.",
    )
    parser.add_argument(
        "--current-spy-shares",
        type=int,
        default=0,
        help="Optional live-mode input for current SPY shares.",
    )
    parser.add_argument(
        "--current-spy-avg-entry-price",
        type=float,
        default=None,
        help="Optional live-mode input for current SPY average entry price.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory for CSV and chart outputs.",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=SLIPPAGE_BPS,
        help="Per-side slippage in basis points. Default: 5.",
    )
    return parser.parse_args()


def fetch_data(start_date: str, end_date: str) -> StrategyData:
    prices = yf.download(
        tickers=TICKERS,
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

    opens = prices["Open"].reindex(columns=TICKERS).sort_index()
    closes = prices["Close"].reindex(columns=TICKERS).sort_index()
    if opens.empty or closes.empty:
        raise ValueError("Price history is missing open/close data.")

    dgs2 = pdr.DataReader("DGS2", "fred", start_date, end_date)["DGS2"]
    dgs2 = pd.to_numeric(dgs2, errors="coerce")
    dgs2 = dgs2.reindex(closes.index).ffill()

    return StrategyData(opens=opens, closes=closes, dgs2=dgs2)


def compute_month_end_dates(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    month_end_dates = index.to_series().groupby(index.to_period("M")).max()
    return pd.DatetimeIndex(month_end_dates.values)


def determine_action(target_spy_weight: float, previous_target_spy_weight: float) -> str:
    if target_spy_weight > previous_target_spy_weight:
        return "BUY"
    if target_spy_weight < previous_target_spy_weight:
        return "EXIT" if target_spy_weight == 0 else "REDUCE"
    return "STAY_DEFENSIVE" if target_spy_weight == 0 else "HOLD"


def build_signals(data: StrategyData, slippage_rate: float) -> pd.DataFrame:
    closes = data.closes
    dgs2 = data.dgs2
    month_end_dates = compute_month_end_dates(closes.index)

    ret_dbc = closes["DBC"].pct_change(20)
    ret_dbb = closes["DBB"].pct_change(20)
    ret_dba = closes["DBA"].pct_change(20)
    chg_dgs2 = dgs2 - dgs2.shift(20)
    ma50_spy = closes["SPY"].rolling(50).mean()

    rows: List[Dict[str, object]] = []
    previous_target_spy_weight = 0.0

    for signal_date in month_end_dates:
        idx = closes.index.get_loc(signal_date)
        if idx < 59:
            continue
        if idx >= len(closes.index) - 1:
            continue

        metrics = {
            "ret_DBC": ret_dbc.loc[signal_date],
            "ret_DBB": ret_dbb.loc[signal_date],
            "ret_DBA": ret_dba.loc[signal_date],
            "chg_DGS2": chg_dgs2.loc[signal_date],
            "ma50_SPY": ma50_spy.loc[signal_date],
            "SPY_close": closes.at[signal_date, "SPY"],
        }
        if any(pd.isna(value) for value in metrics.values()):
            continue

        execution_date = closes.index[idx + 1]
        next_open_spy = data.opens.at[execution_date, "SPY"]
        next_open_shy = data.opens.at[execution_date, "SHY"]
        if pd.isna(next_open_spy) or pd.isna(next_open_shy):
            continue

        score = 0
        if metrics["ret_DBC"] > 0:
            score += 1
        if metrics["ret_DBB"] > 0:
            score += 1
        if metrics["ret_DBA"] < 0:
            score += 1
        if metrics["chg_DGS2"] < 0:
            score += 1

        spy_above_ma50 = bool(metrics["SPY_close"] > metrics["ma50_SPY"])
        if score == 4 and spy_above_ma50:
            target_spy_weight = 1.0
            target_shy_weight = 0.0
        elif score == 3 and spy_above_ma50:
            target_spy_weight = 0.5
            target_shy_weight = 0.5
        else:
            target_spy_weight = 0.0
            target_shy_weight = 1.0

        if metrics["ret_DBC"] <= -0.05:
            target_spy_weight = 0.0
            target_shy_weight = 1.0

        action = determine_action(target_spy_weight, previous_target_spy_weight)
        previous_target_spy_weight = target_spy_weight

        rows.append(
            {
                "signal_date": signal_date,
                "execution_date": execution_date,
                "ret_DBC": metrics["ret_DBC"],
                "ret_DBB": metrics["ret_DBB"],
                "ret_DBA": metrics["ret_DBA"],
                "chg_DGS2": metrics["chg_DGS2"],
                "spy_above_ma50": spy_above_ma50,
                "score": score,
                "target_spy_weight": target_spy_weight,
                "target_shy_weight": target_shy_weight,
                "action": action,
                "slippage_bps": slippage_rate * 10_000,
            }
        )

    signals = pd.DataFrame(rows)
    if signals.empty:
        raise ValueError("No valid month-end signals were generated.")
    return signals


def effective_fill_price(raw_open_price: float, side: str, slippage_rate: float) -> float:
    if side == "BUY":
        return raw_open_price * (1 + slippage_rate)
    if side == "SELL":
        return raw_open_price * (1 - slippage_rate)
    raise ValueError(f"Unsupported side: {side}")


def execute_trade(
    trade_date: pd.Timestamp,
    ticker: str,
    side: str,
    shares: int,
    raw_open_price: float,
    slippage_rate: float,
    reason: str,
) -> Dict[str, object]:
    fill_price = effective_fill_price(raw_open_price, side, slippage_rate)
    slippage_cost = abs(fill_price - raw_open_price) * shares
    gross_cash_flow = shares * fill_price
    commission_cost = COMMISSION
    if side == "BUY":
        net_cash_flow = -(gross_cash_flow + commission_cost)
    else:
        net_cash_flow = gross_cash_flow - commission_cost

    return {
        "trade_date": trade_date,
        "ticker": ticker,
        "side": side,
        "shares": int(shares),
        "raw_open_price": raw_open_price,
        "fill_price": fill_price,
        "slippage_cost": slippage_cost,
        "commission_cost": commission_cost,
        "net_cash_flow": net_cash_flow,
        "reason": reason,
    }


def next_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    return ts + pd.offsets.BDay(1)


def build_live_signal(
    data: StrategyData,
    slippage_rate: float,
    account_value: float,
    current_spy_shares: int,
    current_spy_avg_entry_price: Optional[float],
) -> pd.DataFrame:
    closes = data.closes
    latest_date = closes.index[-1]
    idx = len(closes.index) - 1
    if idx < 59:
        raise ValueError("Insufficient history to compute live signal.")

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
        stop_price = np.nan

    action = "STAY_DEFENSIVE" if target_spy_weight == 0 else "BUY"
    recommendation = pd.DataFrame(
        [
            {
                "as_of_date": latest_date,
                "next_trade_date": next_business_day(latest_date),
                "score": score,
                "target_spy_weight": target_spy_weight,
                "target_shy_weight": target_shy_weight,
                "action": action,
                "stop_price": stop_price,
                "account_value": account_value,
                "ret_DBC": ret_dbc,
                "ret_DBB": ret_dbb,
                "ret_DBA": ret_dba,
                "chg_DGS2": chg_dgs2,
                "spy_above_ma50": spy_above_ma50,
                "slippage_bps": slippage_rate * 10_000,
            }
        ]
    )
    return recommendation


def run_backtest(
    data: StrategyData,
    signals: pd.DataFrame,
    initial_capital: float,
    slippage_rate: float,
) -> BacktestResult:
    backtest_start_date = pd.Timestamp(signals["signal_date"].iloc[0])
    opens = data.opens.loc[backtest_start_date:, TRADE_TICKERS].copy()
    closes = data.closes.loc[backtest_start_date:, TRADE_TICKERS].copy()
    calendar = closes.index

    signal_map = signals.set_index("execution_date").sort_index()
    month_end_signal_dates = set(pd.to_datetime(signals["signal_date"]))

    cash = float(initial_capital)
    spy_shares = 0
    shy_shares = 0
    avg_entry_price: Optional[float] = None
    stopped_out = False
    pending_stop_execution: Optional[pd.Timestamp] = None

    trades: List[Dict[str, object]] = []
    equity_records: List[Dict[str, object]] = []
    realized_spy_trade_pnls: List[float] = []

    previous_portfolio_value: Optional[float] = None
    running_peak = initial_capital

    for i, current_date in enumerate(calendar):
        open_prices = opens.loc[current_date]
        close_prices = closes.loc[current_date]

        has_monthly_rebalance = current_date in signal_map.index
        if has_monthly_rebalance and pending_stop_execution == current_date:
            pending_stop_execution = None

        if pending_stop_execution == current_date and spy_shares > 0:
            raw_open_spy = float(open_prices["SPY"])
            sell_trade = execute_trade(
                trade_date=current_date,
                ticker="SPY",
                side="SELL",
                shares=spy_shares,
                raw_open_price=raw_open_spy,
                slippage_rate=slippage_rate,
                reason="stop_loss",
            )
            cash += sell_trade["net_cash_flow"]
            trades.append(sell_trade)
            if avg_entry_price is not None:
                realized_spy_trade_pnls.append((sell_trade["fill_price"] - avg_entry_price) * spy_shares)
            spy_shares = 0
            avg_entry_price = None

            raw_open_shy = float(open_prices["SHY"])
            fill_price_shy = effective_fill_price(raw_open_shy, "BUY", slippage_rate)
            max_buyable_shy = math.floor((cash - COMMISSION) / fill_price_shy)
            if max_buyable_shy > 0:
                buy_trade = execute_trade(
                    trade_date=current_date,
                    ticker="SHY",
                    side="BUY",
                    shares=max_buyable_shy,
                    raw_open_price=raw_open_shy,
                    slippage_rate=slippage_rate,
                    reason="stop_loss",
                )
                cash += buy_trade["net_cash_flow"]
                shy_shares += max_buyable_shy
                trades.append(buy_trade)

            pending_stop_execution = None
            stopped_out = True

        if has_monthly_rebalance:
            signal = signal_map.loc[current_date]
            if isinstance(signal, pd.DataFrame):
                signal = signal.iloc[-1]

            portfolio_open_value = (
                cash
                + spy_shares * float(open_prices["SPY"])
                + shy_shares * float(open_prices["SHY"])
            )
            target_weights = {
                "SPY": float(signal["target_spy_weight"]),
                "SHY": float(signal["target_shy_weight"]),
            }
            target_shares: Dict[str, int] = {}
            for ticker in TRADE_TICKERS:
                target_dollars = portfolio_open_value * target_weights[ticker]
                target_buy_fill = effective_fill_price(float(open_prices[ticker]), "BUY", slippage_rate)
                target_shares[ticker] = math.floor(target_dollars / target_buy_fill)

            current_shares = {"SPY": spy_shares, "SHY": shy_shares}

            for ticker in TRADE_TICKERS:
                shares_to_sell = max(0, current_shares[ticker] - target_shares[ticker])
                if shares_to_sell == 0:
                    continue
                raw_open_price = float(open_prices[ticker])
                sell_trade = execute_trade(
                    trade_date=current_date,
                    ticker=ticker,
                    side="SELL",
                    shares=shares_to_sell,
                    raw_open_price=raw_open_price,
                    slippage_rate=slippage_rate,
                    reason="monthly_rebalance",
                )
                cash += sell_trade["net_cash_flow"]
                trades.append(sell_trade)

                if ticker == "SPY":
                    if avg_entry_price is not None:
                        realized_spy_trade_pnls.append((sell_trade["fill_price"] - avg_entry_price) * shares_to_sell)
                    spy_shares -= shares_to_sell
                    if spy_shares == 0:
                        avg_entry_price = None
                else:
                    shy_shares -= shares_to_sell

            for ticker in TRADE_TICKERS:
                current_position = spy_shares if ticker == "SPY" else shy_shares
                desired_shares = target_shares[ticker]
                shares_to_buy = max(0, desired_shares - current_position)
                if shares_to_buy == 0:
                    continue

                raw_open_price = float(open_prices[ticker])
                fill_price = effective_fill_price(raw_open_price, "BUY", slippage_rate)
                affordable_shares = math.floor((cash - COMMISSION) / fill_price)
                shares_to_buy = min(shares_to_buy, max(0, affordable_shares))
                if shares_to_buy == 0:
                    continue

                buy_trade = execute_trade(
                    trade_date=current_date,
                    ticker=ticker,
                    side="BUY",
                    shares=shares_to_buy,
                    raw_open_price=raw_open_price,
                    slippage_rate=slippage_rate,
                    reason="monthly_rebalance",
                )
                cash += buy_trade["net_cash_flow"]
                trades.append(buy_trade)

                if ticker == "SPY":
                    previous_shares = spy_shares
                    spy_shares += shares_to_buy
                    if previous_shares == 0 or avg_entry_price is None:
                        avg_entry_price = float(buy_trade["fill_price"])
                    else:
                        avg_entry_price = (
                            (avg_entry_price * previous_shares)
                            + (float(buy_trade["fill_price"]) * shares_to_buy)
                        ) / spy_shares
                else:
                    shy_shares += shares_to_buy

            stopped_out = False
            pending_stop_execution = None

        portfolio_value = cash + spy_shares * float(close_prices["SPY"]) + shy_shares * float(close_prices["SHY"])
        running_peak = max(running_peak, portfolio_value)
        drawdown = portfolio_value / running_peak - 1
        daily_return = np.nan if previous_portfolio_value is None else portfolio_value / previous_portfolio_value - 1

        equity_records.append(
            {
                "date": current_date,
                "spy_shares": spy_shares,
                "shy_shares": shy_shares,
                "cash": cash,
                "portfolio_value": portfolio_value,
                "daily_return": daily_return,
                "drawdown": drawdown,
            }
        )
        previous_portfolio_value = portfolio_value

        is_month_end_signal_day = current_date in month_end_signal_dates
        if (
            i < len(calendar) - 1
            and spy_shares > 0
            and avg_entry_price is not None
            and not is_month_end_signal_day
            and float(close_prices["SPY"]) <= avg_entry_price * 0.93
            and not stopped_out
        ):
            pending_stop_execution = calendar[i + 1]

    equity_curve = pd.DataFrame(equity_records)
    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        trades_df = pd.DataFrame(
            columns=[
                "trade_date",
                "ticker",
                "side",
                "shares",
                "raw_open_price",
                "fill_price",
                "slippage_cost",
                "commission_cost",
                "net_cash_flow",
                "reason",
            ]
        )

    daily_returns = equity_curve["daily_return"].dropna()
    total_return = equity_curve["portfolio_value"].iloc[-1] / initial_capital - 1
    years = len(equity_curve) / 252
    cagr = np.nan if years <= 0 else (equity_curve["portfolio_value"].iloc[-1] / initial_capital) ** (1 / years) - 1
    max_drawdown = equity_curve["drawdown"].min()
    sharpe_ratio = np.nan
    if daily_returns.std(ddof=0) > 0:
        sharpe_ratio = np.sqrt(252) * daily_returns.mean() / daily_returns.std(ddof=0)
    number_of_trades = int(len(trades_df))
    win_rate = np.nan if not realized_spy_trade_pnls else float(np.mean(np.array(realized_spy_trade_pnls) > 0))

    summary = {
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "number_of_trades": number_of_trades,
        "win_rate": win_rate,
    }
    return BacktestResult(signals=signals.copy(), trades=trades_df, equity_curve=equity_curve, summary=summary)


def save_charts(equity_curve: pd.DataFrame, data: StrategyData, output_dir: Path) -> None:
    start_date = pd.Timestamp(equity_curve["date"].iloc[0])
    benchmark = data.closes.loc[start_date:, "SPY"].copy()
    benchmark = benchmark / benchmark.iloc[0] * 100
    strategy_index = equity_curve.set_index("date")["portfolio_value"]
    strategy_index = strategy_index / strategy_index.iloc[0] * 100

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(strategy_index.index, strategy_index.values, label="Strategy", linewidth=2)
    ax.plot(benchmark.index, benchmark.values, label="SPY Buy & Hold", linewidth=1.5)
    ax.set_title("Equity Curve")
    ax.set_ylabel("Indexed Value")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / "equity_curve.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(
        equity_curve["date"],
        equity_curve["drawdown"].values,
        0,
        color="firebrick",
        alpha=0.35,
    )
    ax.plot(equity_curve["date"], equity_curve["drawdown"].values, color="firebrick", linewidth=1.25)
    ax.set_title("Strategy Drawdown")
    ax.set_ylabel("Drawdown")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / "drawdown.png", dpi=150)
    plt.close(fig)


def write_backtest_outputs(result: BacktestResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    signals = result.signals.copy()
    signals["signal_date"] = pd.to_datetime(signals["signal_date"]).dt.date
    signals["execution_date"] = pd.to_datetime(signals["execution_date"]).dt.date
    signals = signals[
        [
            "signal_date",
            "execution_date",
            "ret_DBC",
            "ret_DBB",
            "ret_DBA",
            "chg_DGS2",
            "spy_above_ma50",
            "score",
            "target_spy_weight",
            "target_shy_weight",
            "action",
        ]
    ]
    signals.to_csv(output_dir / "signals.csv", index=False)

    trades = result.trades.copy()
    if not trades.empty:
        trades["trade_date"] = pd.to_datetime(trades["trade_date"]).dt.date
        trades = trades[
            [
                "trade_date",
                "ticker",
                "side",
                "shares",
                "raw_open_price",
                "fill_price",
                "slippage_cost",
                "commission_cost",
                "net_cash_flow",
                "reason",
            ]
        ]
    trades.to_csv(output_dir / "trades.csv", index=False)

    equity_curve = result.equity_curve.copy()
    equity_curve["date"] = pd.to_datetime(equity_curve["date"]).dt.date
    equity_curve.to_csv(output_dir / "equity_curve.csv", index=False)


def write_live_output(recommendation: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    formatted = recommendation.copy()
    formatted["as_of_date"] = pd.to_datetime(formatted["as_of_date"]).dt.date
    formatted["next_trade_date"] = pd.to_datetime(formatted["next_trade_date"]).dt.date
    formatted[
        [
            "as_of_date",
            "next_trade_date",
            "score",
            "target_spy_weight",
            "target_shy_weight",
            "action",
            "stop_price",
        ]
    ].to_csv(output_dir / "latest_signal.csv", index=False)


def print_summary(summary: Dict[str, float]) -> None:
    print("Summary Stats")
    print(f"Total Return  : {summary['total_return']:.2%}")
    print(f"CAGR          : {summary['cagr']:.2%}")
    print(f"Max Drawdown  : {summary['max_drawdown']:.2%}")
    print(f"Sharpe Ratio  : {summary['sharpe_ratio']:.2f}" if pd.notna(summary["sharpe_ratio"]) else "Sharpe Ratio  : nan")
    print(f"Number Trades : {summary['number_of_trades']}")
    print(f"Win Rate      : {summary['win_rate']:.2%}" if pd.notna(summary["win_rate"]) else "Win Rate      : nan")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    slippage_rate = args.slippage_bps / 10_000

    if args.mode == "live" and args.account_value is None:
        raise ValueError("--account-value is required in live mode.")

    data = fetch_data(args.start_date, args.end_date)
    signals = build_signals(data, slippage_rate)

    if args.mode == "backtest":
        result = run_backtest(
            data=data,
            signals=signals,
            initial_capital=args.initial_capital,
            slippage_rate=slippage_rate,
        )
        write_backtest_outputs(result, output_dir)
        save_charts(result.equity_curve, data, output_dir)
        print_summary(result.summary)
    else:
        recommendation = build_live_signal(
            data=data,
            slippage_rate=slippage_rate,
            account_value=args.account_value,
            current_spy_shares=args.current_spy_shares,
            current_spy_avg_entry_price=args.current_spy_avg_entry_price,
        )
        write_live_output(recommendation, output_dir)
        print(recommendation.to_string(index=False))


if __name__ == "__main__":
    main()
