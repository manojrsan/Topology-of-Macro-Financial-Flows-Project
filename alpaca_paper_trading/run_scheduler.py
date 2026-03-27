import argparse
import json

from alpaca_paper_trading.run_paper_trading import execute_mode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scheduler wrapper for the Alpaca macro flow paper-trading workflow."
    )
    parser.add_argument(
        "--phase",
        choices=["after-close", "at-open", "cycle"],
        required=True,
        help="Workflow phase to run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Pass through force override to the underlying runner.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without submitting orders.",
    )
    parser.add_argument(
        "--start-date",
        default="2006-01-01",
        help="History start date used for signal inputs.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional history end date override.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory override.",
    )
    return parser.parse_args()


def run_mode(mode: str, args: argparse.Namespace) -> dict:
    namespace = argparse.Namespace(
        mode=mode,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        force=args.force,
        dry_run=args.dry_run,
    )
    return execute_mode(namespace)


def main() -> None:
    args = parse_args()
    results = []

    if args.phase == "after-close":
        results.append(run_mode("status", args))
        results.append(run_mode("latest-signal", args))
        results.append(run_mode("stop-check", args))
    elif args.phase == "at-open":
        results.append(run_mode("rebalance", args))
    else:
        try:
            results.append(run_mode("rebalance", args))
        except Exception as exc:
            results.append({"event": "rebalance", "error": str(exc)})
        try:
            results.append(run_mode("stop-check", args))
        except Exception as exc:
            results.append({"event": "stop_check", "error": str(exc)})

    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
