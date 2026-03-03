#!/usr/bin/env python3
from __future__ import annotations

import sys

from tender_radar import run_cli


if __name__ == "__main__":
    exit_code = run_cli()
    if "ipykernel" not in sys.modules:
        raise SystemExit(exit_code)
