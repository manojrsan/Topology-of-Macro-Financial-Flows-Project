import argparse
import json
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd

from alpaca_paper_trading.broker import AlpacaPaperBroker
from alpaca_paper_trading.config import AlpacaPaperConfig
from alpaca_paper_trading.persistence import (
    append_journal,
    append_ledger_rows,
    ensure_output_dir,
    load_state,
    save_state,
)
from alpaca_paper_trading.strategy_logic import (
    LiveSignal,
    build_live_signal,
    compute_target_shares,
    fetch_strategy_data,
    latest_reference_prices,
    signal_summary_dict,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Paper-trading runner for the macro flow SPY/SHY strategy on Alpaca."
    )
    parser.add_argument(
        "--mode",
        choices=["latest-signal", "status", "rebalance", "stop-check"],
        required=True,
        help="Action to perform.",
    )
    parser.add_argument(
        "--start-date",
        default="2006-01-01",
        help="History start date used for signal inputs.",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="History end date used for signal inputs. Default: today.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional override for output directory.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Override timing and market-hours guards.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print and log intended actions without submitting orders.",
    )
    return parser.parse_args()


def write_signal_outputs(signal: LiveSignal, output_dir: Path) -> None:
    ensure_output_dir(output_dir)
    frame = signal.to_frame().copy()
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
    frame["next_trade_date"] = pd.to_datetime(frame["next_trade_date"]).dt.date
    frame.to_csv(output_dir / "latest_signal.csv", index=False)

    with (output_dir / "latest_signal.json").open("w", encoding="utf-8") as handle:
        json.dump(signal_summary_dict(signal), handle, indent=2)


def write_snapshot(payload: Dict[str, object], output_dir: Path, filename: str) -> None:
    ensure_output_dir(output_dir)
    with (output_dir / filename).open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def today_ts() -> pd.Timestamp:
    return pd.Timestamp.today().normalize()


def format_order(order: object) -> Dict[str, object]:
    return {
        "id": getattr(order, "id", None),
        "client_order_id": getattr(order, "client_order_id", None),
        "symbol": getattr(order, "symbol", None),
        "side": str(getattr(order, "side", "")),
        "qty": getattr(order, "qty", None),
        "filled_qty": getattr(order, "filled_qty", None),
        "filled_avg_price": getattr(order, "filled_avg_price", None),
        "status": str(getattr(order, "status", "")),
        "submitted_at": getattr(order, "submitted_at", None),
        "filled_at": getattr(order, "filled_at", None),
    }


def build_ledger_rows(
    event: str,
    signal: LiveSignal,
    orders: List[Dict[str, object]],
    dry_run: bool,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for order in orders:
        rows.append(
            {
                "recorded_at": pd.Timestamp.utcnow().isoformat(),
                "event": event,
                "dry_run": dry_run,
                "signal_date": signal.as_of_date.date().isoformat(),
                "next_trade_date": signal.next_trade_date.date().isoformat(),
                "score": signal.score,
                "action": signal.action,
                "symbol": order.get("symbol"),
                "side": order.get("side"),
                "qty": order.get("qty"),
                "filled_qty": order.get("filled_qty"),
                "filled_avg_price": order.get("filled_avg_price"),
                "status": order.get("status"),
                "client_order_id": order.get("client_order_id"),
            }
        )
    return rows


def assert_rebalance_window(signal: LiveSignal, clock: object, force: bool) -> None:
    if force:
        return
    if today_ts().date() != signal.next_trade_date.date():
        raise ValueError(
            "Rebalance date check failed. "
            f"Signal expects execution on {signal.next_trade_date.date().isoformat()}, "
            f"but today is {today_ts().date().isoformat()}. "
            "Pass --force to override."
        )
    if not clock.is_open:
        raise ValueError(
            "Rebalance refused because the market is currently closed. "
            "Run during market hours or pass --force."
        )


def assert_stop_check_window(clock: object, force: bool) -> None:
    if force:
        return
    if clock.is_open:
        raise ValueError(
            "Stop check refused because the market is currently open. "
            "Run after the market close or pass --force."
        )


def assert_not_duplicate(output_dir: Path, state_key: str, force: bool) -> Dict[str, str]:
    state = load_state(output_dir)
    if not force and state.get(state_key) == "done":
        raise RuntimeError(
            f"Refusing to run duplicate execution for '{state_key}'. Pass --force to override."
        )
    return state


def persist_execution(
    output_dir: Path,
    payload: Dict[str, object],
    ledger_rows: List[Dict[str, object]],
    state_key: str,
    state: Dict[str, str],
    dry_run: bool,
) -> None:
    append_journal(output_dir, payload)
    append_ledger_rows(output_dir, ledger_rows)
    if not dry_run:
        state[state_key] = "done"
        save_state(output_dir, state)


def build_runtime_context(args: argparse.Namespace) -> Dict[str, object]:
    config = AlpacaPaperConfig.from_env()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else config.output_dir.resolve()
    )
    ensure_output_dir(output_dir)

    broker = AlpacaPaperBroker(config.api_key, config.secret_key, paper=config.paper)
    account = broker.get_account_snapshot()
    positions = broker.get_positions()
    clock = broker.get_market_clock()

    spy_position = positions.get("SPY")
    current_spy_shares = int(round(spy_position.qty)) if spy_position else 0
    current_spy_avg_entry_price = spy_position.avg_entry_price if spy_position else None

    data = fetch_strategy_data(start_date=args.start_date, end_date=args.end_date)
    signal = build_live_signal(
        data=data,
        account_value=account.equity,
        current_spy_shares=current_spy_shares,
        current_spy_avg_entry_price=current_spy_avg_entry_price,
    )
    reference_prices = latest_reference_prices(data)
    write_signal_outputs(signal, output_dir)

    return {
        "config": config,
        "output_dir": output_dir,
        "broker": broker,
        "account": account,
        "positions": positions,
        "clock": clock,
        "data": data,
        "signal": signal,
        "reference_prices": reference_prices,
        "current_spy_shares": current_spy_shares,
    }


def execute_mode(args: argparse.Namespace) -> Dict[str, object]:
    ctx = build_runtime_context(args)
    output_dir = ctx["output_dir"]
    broker = ctx["broker"]
    account = ctx["account"]
    positions = ctx["positions"]
    clock = ctx["clock"]
    data = ctx["data"]
    signal = ctx["signal"]
    reference_prices = ctx["reference_prices"]
    current_spy_shares = ctx["current_spy_shares"]

    if args.mode == "latest-signal":
        payload = {
            "event": "latest_signal",
            "dry_run": args.dry_run,
            "signal": signal_summary_dict(signal),
        }
        append_journal(output_dir, payload)
        return payload

    if args.mode == "status":
        payload = {
            "event": "status",
            "dry_run": args.dry_run,
            "account": {
                "equity": account.equity,
                "cash": account.cash,
                "buying_power": account.buying_power,
            },
            "positions": {
                symbol: {
                    "qty": position.qty,
                    "avg_entry_price": position.avg_entry_price,
                    "market_value": position.market_value,
                }
                for symbol, position in positions.items()
            },
            "signal": signal_summary_dict(signal),
            "reference_prices": reference_prices,
            "market_clock": {
                "timestamp": clock.timestamp,
                "is_open": clock.is_open,
                "next_open": clock.next_open,
                "next_close": clock.next_close,
            },
        }
        write_snapshot(payload, output_dir, "status_snapshot.json")
        append_journal(output_dir, payload)
        return payload

    if args.mode == "stop-check":
        assert_stop_check_window(clock, args.force)
        if current_spy_shares <= 0 or signal.stop_price is None:
            payload = {
                "event": "stop_check",
                "dry_run": args.dry_run,
                "triggered": False,
                "message": "No active SPY position. No stop check action required.",
                "signal": signal_summary_dict(signal),
            }
            append_journal(output_dir, payload)
            return payload

        spy_close = data.closes["SPY"].iloc[-1]
        if spy_close > signal.stop_price:
            payload = {
                "event": "stop_check",
                "dry_run": args.dry_run,
                "triggered": False,
                "message": (
                    f"SPY stop not triggered. Latest close {spy_close:.2f} "
                    f"is above stop {signal.stop_price:.2f}."
                ),
                "signal": signal_summary_dict(signal),
            }
            append_journal(output_dir, payload)
            return payload

        state_key = f"stop-check:{signal.as_of_date.date().isoformat()}"
        state = assert_not_duplicate(output_dir, state_key, args.force)
        target_shares = compute_target_shares(
            equity=account.equity,
            target_spy_weight=0.0,
            target_shy_weight=1.0,
            reference_prices=reference_prices,
        )

        if args.dry_run:
            current_shy_shares = int(round(positions.get("SHY").qty)) if positions.get("SHY") else 0
            orders = [
                {"symbol": "SPY", "side": "sell", "qty": current_spy_shares, "status": "planned"},
                {
                    "symbol": "SHY",
                    "side": "buy",
                    "qty": max(0, target_shares["SHY"] - current_shy_shares),
                    "status": "planned",
                },
            ]
        else:
            orders = [
                format_order(order)
                for order in broker.rotate_spy_stop_to_shy(
                    shy_target_shares=target_shares["SHY"],
                    shy_reference_price=reference_prices["SHY"],
                    execution_tag="macro-stop",
                )
            ]

        payload = {
            "event": "stop_check",
            "dry_run": args.dry_run,
            "triggered": True,
            "spy_close": float(spy_close),
            "stop_price": signal.stop_price,
            "target_shares": target_shares,
            "orders": orders,
            "signal": signal_summary_dict(signal),
        }
        write_snapshot(payload, output_dir, "last_stop_rotation.json")
        persist_execution(
            output_dir=output_dir,
            payload=payload,
            ledger_rows=build_ledger_rows("stop_check", signal, orders, args.dry_run),
            state_key=state_key,
            state=state,
            dry_run=args.dry_run,
        )
        return payload

    if args.mode == "rebalance":
        assert_rebalance_window(signal, clock, args.force)
        state_key = f"rebalance:{signal.next_trade_date.date().isoformat()}"
        state = assert_not_duplicate(output_dir, state_key, args.force)
        target_shares = compute_target_shares(
            equity=account.equity,
            target_spy_weight=signal.target_spy_weight,
            target_shy_weight=signal.target_shy_weight,
            reference_prices=reference_prices,
        )

        current_shy_shares = int(round(positions.get("SHY").qty)) if positions.get("SHY") else 0
        if args.dry_run:
            dry_orders = []
            if current_spy_shares > target_shares["SPY"]:
                dry_orders.append(
                    {
                        "symbol": "SPY",
                        "side": "sell",
                        "qty": current_spy_shares - target_shares["SPY"],
                        "status": "planned",
                    }
                )
            if current_shy_shares > target_shares["SHY"]:
                dry_orders.append(
                    {
                        "symbol": "SHY",
                        "side": "sell",
                        "qty": current_shy_shares - target_shares["SHY"],
                        "status": "planned",
                    }
                )
            if target_shares["SPY"] > current_spy_shares:
                dry_orders.append(
                    {
                        "symbol": "SPY",
                        "side": "buy",
                        "qty": target_shares["SPY"] - current_spy_shares,
                        "status": "planned",
                    }
                )
            if target_shares["SHY"] > current_shy_shares:
                dry_orders.append(
                    {
                        "symbol": "SHY",
                        "side": "buy",
                        "qty": target_shares["SHY"] - current_shy_shares,
                        "status": "planned",
                    }
                )
            orders = dry_orders
        else:
            orders = [
                format_order(order)
                for order in broker.rebalance_to_target_shares(
                    target_shares=target_shares,
                    reference_prices=reference_prices,
                    execution_tag="macro-rebalance",
                    symbols_in_scope=["SPY", "SHY"],
                )
            ]

        payload = {
            "event": "rebalance",
            "dry_run": args.dry_run,
            "signal": signal_summary_dict(signal),
            "reference_prices": reference_prices,
            "target_shares": target_shares,
            "orders": orders,
            "market_clock": {
                "timestamp": clock.timestamp,
                "is_open": clock.is_open,
                "next_open": clock.next_open,
                "next_close": clock.next_close,
            },
        }
        write_snapshot(payload, output_dir, "last_rebalance.json")
        persist_execution(
            output_dir=output_dir,
            payload=payload,
            ledger_rows=build_ledger_rows("rebalance", signal, orders, args.dry_run),
            state_key=state_key,
            state=state,
            dry_run=args.dry_run,
        )
        return payload

    raise ValueError(f"Unsupported mode: {args.mode}")


def main() -> None:
    args = parse_args()
    payload = execute_mode(args)
    if args.mode == "latest-signal":
        print(pd.DataFrame([payload["signal"]]).to_string(index=False))
    elif args.mode == "status":
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
