# Alpaca Paper Trading Integration

This folder contains a standalone Alpaca paper-trading wrapper around the macro flow strategy.

It keeps the same core signal logic as the main Python backtest:

- signal computed from `DBC`, `DBB`, `DBA`, `DGS2`, and `SPY`
- target allocation rotates between `SPY` and `SHY`
- `SPY` stop-loss at `7%` below average entry price
- sell-first, then buy-second execution sequencing
- whole-share sizing only

## Files

- `config.py`
  - loads Alpaca credentials and runtime settings from environment variables and `.env`
- `strategy_logic.py`
  - fetches market data and computes the live macro signal
- `broker.py`
  - wraps Alpaca account, position, order, and market-clock calls
- `persistence.py`
  - manages execution state, JSONL history, and CSV ledger files
- `run_paper_trading.py`
  - main CLI for signal checks, status, stop checks, and rebalances
- `run_scheduler.py`
  - wrapper for the after-close and next-open workflow
- `.env.example`
  - example environment variable file
- `requirements.txt`
  - Python dependencies for this live trading package

## Setup

Install dependencies:

```bash
pip install -r alpaca_paper_trading/requirements.txt
```

Create a local env file:

```bash
cp alpaca_paper_trading/.env.example .env
```

Then edit `.env` and fill in your Alpaca paper credentials:

```bash
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
ALPACA_PAPER=true
ALPACA_OUTPUT_DIR=alpaca_paper_trading/outputs
```

The runner uses `python-dotenv`, so you do not need to manually `export` the variables each session if `.env` is present.

## Core Commands

Run from the project root:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode latest-signal
python3 -m alpaca_paper_trading.run_paper_trading --mode status
python3 -m alpaca_paper_trading.run_paper_trading --mode stop-check
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance
```

### Dry Run

Use `--dry-run` to inspect intended orders without submitting them:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance --dry-run
python3 -m alpaca_paper_trading.run_paper_trading --mode stop-check --dry-run
```

### Force Override

Use `--force` only when you intentionally want to bypass timing or market-hours guards:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance --force
```

## Scheduler Workflow

The wrapper script supports the intended operating rhythm.

### After Close

Use after the market close:

```bash
python3 -m alpaca_paper_trading.run_scheduler --phase after-close
```

This runs:

- `status`
- `latest-signal`
- `stop-check`

### At Open

Use during the next trading session open window:

```bash
python3 -m alpaca_paper_trading.run_scheduler --phase at-open
```

This runs:

- `rebalance`

### Cycle

For testing, you can run both wrapped actions with error capture:

```bash
python3 -m alpaca_paper_trading.run_scheduler --phase cycle --dry-run
```

## Safety Features Added

### 1. `.env` loading

- credentials can come from a local `.env`
- no need to manually export variables each session

### 2. Dry-run mode

- supported for `rebalance` and `stop-check`
- prints intended orders without touching Alpaca

### 3. Market-hours and timing guards

- `rebalance` requires the intended `next_trade_date`
- `rebalance` requires the market to be open unless `--force` is used
- `stop-check` requires the market to be closed unless `--force` is used

### 4. Persistent execution logs

The output directory now includes:

- `latest_signal.csv`
- `latest_signal.json`
- `status_snapshot.json`
- `last_rebalance.json`
- `last_stop_rotation.json`
- `execution_history.jsonl`
- `execution_ledger.csv`
- `execution_state.json`

### 5. Duplicate-run protection

- successful rebalances are keyed by `rebalance:<next_trade_date>`
- successful stop executions are keyed by `stop-check:<signal_date>`
- rerunning the same action will raise unless `--force` is passed

## Important Differences vs the Backtest

- The backtest models next-open fills and fixed slippage.
- Alpaca paper trading uses Alpaca's broker emulator and market-order handling.
- Share sizing uses latest close-based reference prices before live submission, so final fills will not exactly match the backtest.
- The stop-loss logic uses Alpaca position average entry price for the live `SPY` position.
- The script refuses to trade if open Alpaca orders already exist in `SPY` or `SHY`.

## Operational Notes

- Run `stop-check` after the market close if you want to mimic the backtest's stop evaluation logic.
- Run `rebalance` on the strategy's intended execution date, which is the next business day after the latest signal date.
- Start with a clean paper account or at least make sure `SPY` and `SHY` are not being traded by any other process.
- Use `--dry-run` first before allowing live paper submissions.
