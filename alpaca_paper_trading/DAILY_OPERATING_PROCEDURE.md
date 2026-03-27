# Daily Operating Procedure

This is the recommended manual workflow for running the Alpaca paper-trading version of the macro flow strategy.

## Goal

Each day you want to answer two questions:

1. Did the strategy generate a new signal or a stop-loss event after the close?
2. If a rebalance is required, what should be submitted on the next execution day?

## Before You Start

Make sure:

- your `.env` file is present in the project root
- your Alpaca paper credentials are valid
- you are in the project root directory

Project root:

```bash
/Users/marianojrsanchez/Desktop/Trading/The Topology of Macro Financial Flows/Code
```

## Daily Manual Workflow

### 1. After Market Close

Run:

```bash
python3 -m alpaca_paper_trading.run_scheduler --phase after-close
```

This does three things:

- checks account status
- computes the latest signal
- checks whether the `SPY` stop-loss condition has been triggered

What to review:

- `signal.score`
- `signal.target_spy_weight`
- `signal.target_shy_weight`
- `signal.action`
- current `positions`
- whether the stop check says triggered or not

Typical interpretation:

- if the signal is unchanged and no stop is triggered, do nothing
- if the strategy wants defensive positioning, the next rebalance should move capital into `SHY`
- if the strategy wants risk-on positioning, the next rebalance should move capital into `SPY` or split `SPY`/`SHY`

### 2. On the Next Execution Day, Review the Planned Rebalance

Before submitting any order, inspect the planned trade:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance --dry-run
```

What to review:

- `target_shares`
- `orders`
- whether the trade direction matches the latest signal

If the date guard blocks you because you are checking early, you can inspect with:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance --dry-run --force
```

Use `--force` only for inspection or deliberate override.

### 3. Submit the Rebalance

If the dry-run looks correct, submit the paper trade:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance
```

This will:

- verify the intended execution date
- check market-hours rules
- refuse duplicate executions unless forced
- sell down excess holdings first
- then buy the required shares

## Stop-Loss Workflow

The `SPY` stop is meant to be checked after the close.

If you want to inspect it manually:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode stop-check --dry-run
```

If the stop is truly triggered and you want to execute the paper rotation:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode stop-check
```

This will:

- sell all `SPY`
- then buy `SHY`

## Simplest Recommended Routine

If you want the shortest possible daily checklist:

After close:

```bash
python3 -m alpaca_paper_trading.run_scheduler --phase after-close
```

Next execution morning:

```bash
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance --dry-run
python3 -m alpaca_paper_trading.run_paper_trading --mode rebalance
```

## Files You Should Watch

In `alpaca_paper_trading/outputs`:

- `latest_signal.csv`
- `latest_signal.json`
- `status_snapshot.json`
- `last_rebalance.json`
- `last_stop_rotation.json`
- `execution_history.jsonl`
- `execution_ledger.csv`
- `execution_state.json`

These files give you:

- current recommendation
- last account snapshot
- last planned or executed rebalance
- last stop event
- a running audit trail

## Practical Advice

- always use `--dry-run` before a live paper rebalance
- do not use `--force` unless you understand why the guard is blocking you
- keep the paper account dedicated to this strategy if possible
- verify positions before submitting orders
- treat the output logs as your source of truth for what the script believed and did
