#!/usr/bin/env python3
"""Normalize *-updown.csv market data files to the 6-column schema.

Keeps only: Time stamp, price_to_beat, final_price, lowest_up, lowest_down, outcome.
Drops late_80 / snapshot columns (price_at_*, delta_at_*).

Handles mixed-width files where older rows have 6 fields under a longer header
(outcome is the last field on those rows, not column index of "outcome").

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

_CORE_ALIASES: dict[str, tuple[str, ...]] = {
    "Time stamp": ("time stamp", "timestamp", "time_stamp"),
    "price_to_beat": ("price_to_beat", "price to beat"),
    "final_price": ("final_price", "last price", " last price"),
    "lowest_up": ("lowest_up", "lowest up"),
    "lowest_down": ("lowest_down", "lowest down"),
    "outcome": ("outcome",),
}


def _norm(name: str) -> str:
    return name.strip().lower()


def _header_indices(header: list[str]) -> dict[str, int]:
    """Map output column -> index in source header (by alias)."""
    by_norm = {_norm(h): i for i, h in enumerate(header) if h}
    indices: dict[str, int] = {}
    for out, aliases in _CORE_ALIASES.items():
        for alias in aliases:
            if alias in by_norm:
                indices[out] = by_norm[alias]
                break
    return indices


def _parse_price(value: str) -> float | None:
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text or text.upper() == "N/A":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _infer_outcome(price_to_beat: str, final_price: str) -> str:
    ptb = _parse_price(price_to_beat)
    final = _parse_price(final_price)
    if ptb is None or final is None:
        return ""
    return "Up" if final >= ptb else "Down"


def _cell(row: list[str], index: int | None) -> str:
    if index is None or index < 0 or index >= len(row):
        return ""
    return str(row[index]).strip()


def _extract_row(header: list[str], indices: dict[str, int], row: list[str]) -> list[str]:
    """Extract the 6 core columns from a source row.

    Short rows (common when the header was later widened): first five fields are
    core metrics and the *last* field is outcome — not the header's outcome index.
    """
    if not row:
        return [""] * 6

    # Already short / legacy width: positional mapping.
    if len(row) <= 6:
        values = [str(c).strip() for c in row] + [""] * (6 - len(row))
        return values[:6]

    # Fewer fields than the (long) header: treat as short row padded into a long schema.
    # Example: 6 values under a 14-col header → outcome is row[-1], not header["outcome"].
    if len(row) < len(header):
        core = [str(c).strip() for c in row[:5]]
        while len(core) < 5:
            core.append("")
        outcome = str(row[-1]).strip() if row else ""
        return core + [outcome]

    # Full-width row: pick by header name.
    out = [_cell(row, indices.get(col)) for col in MARKET_CSV_HEADER]
    return out


def normalize_file(path: str, *, dry_run: bool = False, infer_missing: bool = True) -> tuple[int, bool]:
    """Rewrite path to MARKET_CSV_HEADER. Returns (row_count, changed)."""
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            print(f"  skip {path}: empty or no header")
            return 0, False
        header = [h.strip() if h else "" for h in header]
        indices = _header_indices(header)
        missing = [k for k in MARKET_CSV_HEADER if k not in indices]
        # Short-only files still need at least the first columns by position;
        # require outcome alias only when the file is wider than 6 cols.
        if len(header) > 6 and missing:
            raise ValueError(f"{path}: missing required columns {missing}")

        already_normalized = header == MARKET_CSV_HEADER
        source_rows = [list(r) for r in reader if any(str(c).strip() for c in r)]

    out_rows: list[list[str]] = []
    inferred = 0
    for row in source_rows:
        if already_normalized and len(row) >= 6:
            values = [str(c).strip() for c in row[:6]]
        else:
            values = _extract_row(header, indices, row)

        if infer_missing and values[5] not in ("Up", "Down"):
            guessed = _infer_outcome(values[1], values[2])
            if guessed:
                values[5] = guessed
                inferred += 1
        out_rows.append(values)

    # Detect whether anything would change vs current file content.
    changed = (not already_normalized) or inferred > 0
    if already_normalized and inferred == 0:
        # Still rewrite if any row had extra columns / padding differences.
        with open(path, newline="", encoding="utf-8") as f:
            existing = list(csv.reader(f))
        existing_data = existing[1:] if existing else []
        if existing_data != out_rows:
            changed = True

    if not changed:
        print(f"  ok   {path}: already normalized ({len(out_rows)} rows)")
        return len(out_rows), False

    note = f"{len(header)} cols → {len(MARKET_CSV_HEADER)} cols"
    if inferred:
        note += f"; inferred {inferred} outcome(s) from prices"
    print(f"  fix  {path}: {note} ({len(out_rows)} rows)")
    if dry_run:
        return len(out_rows), True

    tmp_path = f"{path}.tmp"
    with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(MARKET_CSV_HEADER)
        writer.writerows(out_rows)
    os.replace(tmp_path, path)
    return len(out_rows), True


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
    parser.add_argument(
        "--no-infer",
        action="store_true",
        help="Do not fill blank outcomes from final_price vs price_to_beat",
    )
    args = parser.parse_args()

    paths = list(args.files) if args.files else sorted(glob.glob("*-updown.csv"))
    if not paths:
        print("No *-updown.csv files found.", file=sys.stderr)
        return 1

    changed = 0
    for path in paths:
        try:
            _, did_change = normalize_file(
                path,
                dry_run=args.dry_run,
                infer_missing=not args.no_infer,
            )
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
