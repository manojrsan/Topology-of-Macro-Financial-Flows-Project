import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class AlpacaPaperConfig:
    api_key: str
    secret_key: str
    paper: bool
    output_dir: Path

    @classmethod
    def from_env(cls) -> "AlpacaPaperConfig":
        load_dotenv()
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        if not api_key or not secret_key:
            raise ValueError(
                "Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY."
            )

        paper = os.getenv("ALPACA_PAPER", "true").strip().lower() not in {"0", "false", "no"}
        output_dir = Path(os.getenv("ALPACA_OUTPUT_DIR", "outputs")).expanduser()
        return cls(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
            output_dir=output_dir,
        )
