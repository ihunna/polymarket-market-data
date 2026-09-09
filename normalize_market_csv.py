#!/usr/bin/env python3
"""Normalize *-updown.csv market data files to the 6-column schema.

Keeps only: Time stamp, price_to_beat, final_price, lowest_up, lowest_down, outcome.
Drops late_80 / snapshot columns (price_at_*, delta_at_*).

Usage:
  python normalize_market_csv.py                  # all *-updown.csv in cwd
  python normalize_market_csv.py solana-15-updown.csv
  python normalize_market_csv.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys

MARKET_CSV_HEADER = [
    "Time stamp",
    "price_to_beat",
    "final_price",
    "lowest_up",
    "lowest_down",
    "outcome",
]

# Map normalized lowercase name -> output column
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "Time stamp": ("time stamp", "timestamp", "time_stamp"),
    "price_to_beat": ("price_to_beat", "price to beat"),
    "final_price": ("final_price", "last price", " last price"),
    "lowest_up": ("lowest_up", "lowest up"),
    "lowest_down": ("lowest_down", "lowest down"),
    "outcome": ("outcome",),
}


def _field_map(fieldnames: list[str] | None) -> dict[str, str]:
    if not fieldnames:
        return {}
    return {name.strip().lower(): name for name in fieldnames if name}


def _pick(field_map: dict[str, str], aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        if alias in field_map:
            return field_map[alias]
    return None


def normalize_file(path: str, *, dry_run: bool = False) -> tuple[int, bool]:
    """Rewrite path to MARKET_CSV_HEADER. Returns (row_count, changed)."""
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        field_map = _field_map(list(reader.fieldnames) if reader.fieldnames else None)
        if not field_map:
            print(f"  skip {path}: empty or no header")
            return 0, False

        col_keys: dict[str, str | None] = {
            out: _pick(field_map, aliases) for out, aliases in _COLUMN_ALIASES.items()
        }
        missing = [k for k, v in col_keys.items() if v is None]
        if missing:
            raise ValueError(f"{path}: missing required columns {missing}")

        existing_header = [name.strip() for name in (reader.fieldnames or []) if name]
        already_normalized = existing_header == MARKET_CSV_HEADER

        rows: list[list[str]] = []
        for row in reader:
            rows.append([str(row.get(col_keys[h]) or "").strip() for h in MARKET_CSV_HEADER])

    if already_normalized:
        print(f"  ok   {path}: already normalized ({len(rows)} rows)")
        return len(rows), False

    print(f"  fix  {path}: {len(existing_header)} cols → {len(MARKET_CSV_HEADER)} cols ({len(rows)} rows)")
    if dry_run:
        return len(rows), True

    tmp_path = f"{path}.tmp"
    with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(MARKET_CSV_HEADER)
        writer.writerows(rows)
    os.replace(tmp_path, path)
    return len(rows), True


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize market updown CSV files to 6 columns.")
    parser.add_argument(
        "files",
        nargs="*",
        help="CSV paths (default: all *-updown.csv in current directory)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing",
    )
    args = parser.parse_args()

    paths = list(args.files) if args.files else sorted(glob.glob("*-updown.csv"))
    if not paths:
        print("No *-updown.csv files found.", file=sys.stderr)
        return 1

    changed = 0
    for path in paths:
        try:
            _, did_change = normalize_file(path, dry_run=args.dry_run)
            if did_change:
                changed += 1
        except (OSError, ValueError) as exc:
            print(f"  err  {exc}", file=sys.stderr)
            return 1

    action = "would update" if args.dry_run else "updated"
    print(f"Done: {action} {changed}/{len(paths)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
