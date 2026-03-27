import csv
import json
from pathlib import Path
from typing import Dict, List


STATE_FILENAME = "execution_state.json"
JOURNAL_FILENAME = "execution_history.jsonl"
LEDGER_FILENAME = "execution_ledger.csv"


def ensure_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def load_state(output_dir: Path) -> Dict[str, str]:
    ensure_output_dir(output_dir)
    path = output_dir / STATE_FILENAME
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(output_dir: Path, state: Dict[str, str]) -> None:
    ensure_output_dir(output_dir)
    path = output_dir / STATE_FILENAME
    with path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def append_journal(output_dir: Path, payload: Dict[str, object]) -> None:
    ensure_output_dir(output_dir)
    path = output_dir / JOURNAL_FILENAME
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str))
        handle.write("\n")


def append_ledger_rows(output_dir: Path, rows: List[Dict[str, object]]) -> None:
    if not rows:
        return

    ensure_output_dir(output_dir)
    path = output_dir / LEDGER_FILENAME
    fieldnames = list(rows[0].keys())
    write_header = not path.exists()

    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
