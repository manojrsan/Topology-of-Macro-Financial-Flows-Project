# Macro Flow Strategy

This project implements a systematic allocation strategy that trades `SPY` and rotates into `SHY` when the macro signal weakens or a stop-loss is triggered.

The script is in [macro_flow_strategy.py](/Users/marianojrsanchez/Desktop/Trading/The%20Topology%20of%20Macro%20Financial%20Flows/Code/macro_flow_strategy.py). It supports:

-   `backtest` mode for historical simulation from `2006-01-01` through today
-   `live` mode for generating the current recommendation from the latest available data

## Data Inputs

The script pulls:

-   Yahoo Finance via `yfinance`
    -   `SPY`
    -   `SHY`
    -   `DBC`
    -   `DBB`
    -   `DBA`
-   FRED via `pandas_datareader`
    -   `DGS2` (2-year Treasury yield)

Signals are computed on month-end closes using:

-   20-trading-day return for `DBC`
-   20-trading-day return for `DBB`
-   20-trading-day return for `DBA`
-   20-trading-day change in `DGS2`
-   50-trading-day moving average of `SPY`

The backtest does not hard-code the first trading date. It begins on the first month-end where all required inputs exist and at least 60 trading days of history are available.

## Strategy Logic

At each month-end close:

1.  Score the macro inputs:
    -   `+1` if `DBC` 20-day return is positive
    -   `+1` if `DBB` 20-day return is positive
    -   `+1` if `DBA` 20-day return is negative
    -   `+1` if `DGS2` 20-day change is negative
2.  Apply the `SPY` trend filter:
    -   `SPY_close > MA50`
3.  Set target weights:
    -   Score `4` and trend filter passes: `100% SPY / 0% SHY`
    -   Score `3` and trend filter passes: `50% SPY / 50% SHY`
    -   Otherwise: `0% SPY / 100% SHY`
4.  Apply the extra defensive override:
    -   If `DBC` 20-day return is `<= -5%`, force `0% SPY / 100% SHY`
5.  Execute the rebalance at the next trading day's open.

## Stop-Loss Logic

-   While long `SPY`, maintain `avg_entry_price`
-   Stop price = `avg_entry_price * 0.93`
-   If `SPY_close <= stop_price`, exit `SPY` at the next trading day's open
-   Immediately rotate proceeds into `SHY` at that same next open
-   Do not re-enter `SPY` until the next month-end rebalance signal

## Execution Assumptions

-   Initial capital in backtest mode: `100_000`
-   No fractional shares
-   Whole-share sizing uses `floor(target_dollars / fill_price)`
-   Rebalance order of operations:
    -   sells first
    -   buys second
-   Commission: `0.00`
-   Slippage: `5 bps` per side by default
    -   buy fill = `open * (1 + 0.0005)`
    -   sell fill = `open * (1 - 0.0005)`

## Installation

Install dependencies:

``` bash
pip install -r requirements.txt
```

## Usage

### Backtest

``` bash
python3 macro_flow_strategy.py --mode backtest --output-dir outputs
```

Optional arguments:

-   `--start-date 2006-01-01`
-   `--end-date YYYY-MM-DD`
-   `--initial-capital 100000`
-   `--slippage-bps 5`

### Live Signal

``` bash
python3 macro_flow_strategy.py --mode live --account-value 250000 --output-dir outputs
```

Optional live inputs:

-   `--current-spy-shares`
-   `--current-spy-avg-entry-price`

If the live account currently holds `SPY`, providing the average entry price allows the script to report the active stop price.

## Expected Outputs

### Backtest Mode

The script writes four outputs to `--output-dir`.

#### 1. `signals.csv`

One row per month-end signal.

Columns:

-   `signal_date`
-   `execution_date`
-   `ret_DBC`
-   `ret_DBB`
-   `ret_DBA`
-   `chg_DGS2`
-   `spy_above_ma50`
-   `score`
-   `target_spy_weight`
-   `target_shy_weight`
-   `action`

`action` is one of:

-   `BUY`
-   `HOLD`
-   `REDUCE`
-   `EXIT`
-   `STAY_DEFENSIVE`

#### 2. `trades.csv`

One row per executed trade.

Columns:

-   `trade_date`
-   `ticker`
-   `side`
-   `shares`
-   `raw_open_price`
-   `fill_price`
-   `slippage_cost`
-   `commission_cost`
-   `net_cash_flow`
-   `reason`

`reason` is one of:

-   `monthly_rebalance`
-   `stop_loss`

#### 3. `equity_curve.csv`

One row per trading day in the active backtest window.

Columns:

-   `date`
-   `spy_shares`
-   `shy_shares`
-   `cash`
-   `portfolio_value`
-   `daily_return`
-   `drawdown`

#### 4. Console Summary

Printed summary statistics:

-   total return
-   CAGR
-   max drawdown
-   Sharpe ratio
-   number of trades
-   win rate

### Chart Files

Backtest mode also saves:

-   `equity_curve.png`
    -   strategy portfolio value vs. `SPY` buy-and-hold, both indexed to `100` at the backtest start
-   `drawdown.png`
    -   strategy drawdown over time

### Live Mode

The script writes:

-   `latest_signal.csv`

It also prints the current recommendation to the console.

Columns:

-   `as_of_date`
-   `next_trade_date`
-   `score`
-   `target_spy_weight`
-   `target_shy_weight`
-   `action`
-   `stop_price`

`stop_price` is populated only if current `SPY` holdings and average entry price are provided.

## Notes

-   Runtime depends on live downloads from Yahoo Finance and FRED.
-   If data sources change schema or return missing values, the script will raise an error instead of silently backfilling unsupported fields.
-   The implementation uses a common signal engine for both backtest and live mode so the decision rules stay consistent.
