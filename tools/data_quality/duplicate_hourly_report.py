#!/usr/bin/env python3
"""Generate a duplicate-hour report for hourly weather station files.

This implementation processes each file independently to keep memory usage stable
on GitHub-hosted runners.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass
class FileStats:
    path: Path
    station: str
    rows: int
    valid_timestamp_rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan weather CSV files and report duplicate timestamps where row values are "
            "either identical or conflicting."
        )
    )
    parser.add_argument("--data-dir", default="data", help="Root data directory to scan.")
    parser.add_argument(
        "--output-dir",
        default="reports/data_quality",
        help="Directory to write report artifacts.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit of files for quick dry runs.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="Log progress every N scanned files.",
    )
    return parser.parse_args()


def iter_station_csv_files(data_dir: Path) -> Iterable[Path]:
    for csv_path in sorted(data_dir.glob("*/*.csv")):
        name = csv_path.name.lower()
        if name.endswith("_daily.csv") or name.endswith("_monthly.csv"):
            continue
        if name == "desktop.ini":
            continue
        yield csv_path


def normalize_values(df: pd.DataFrame, value_columns: list[str]) -> pd.Series:
    if not value_columns:
        return pd.Series([""] * len(df), index=df.index)

    # Build a stable row signature without expensive row-wise Python joins.
    normalized = df[value_columns].fillna("").astype(str)
    normalized = normalized.apply(lambda col: col.str.strip())
    return pd.util.hash_pandas_object(normalized, index=False).astype(str)


def analyze_file(csv_path: Path) -> tuple[FileStats, list[dict], list[dict]]:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    timestamp_col = df.columns[0]

    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    valid = df[df[timestamp_col].notna()].copy()

    stats = FileStats(
        path=csv_path,
        station=csv_path.parent.name,
        rows=len(df),
        valid_timestamp_rows=len(valid),
    )

    if valid.empty:
        return stats, [], []

    # Keep hourly records only.
    valid = valid[valid[timestamp_col].dt.floor("h") == valid[timestamp_col]].copy()
    if valid.empty:
        return stats, [], []

    value_columns = [col for col in valid.columns if col != timestamp_col]
    valid["value_signature"] = normalize_values(valid, value_columns)

    dup_rows = valid[valid.duplicated(subset=[timestamp_col], keep=False)]
    if dup_rows.empty:
        return stats, [], []

    grouped = (
        dup_rows.groupby(timestamp_col, as_index=False)
        .agg(duplicate_rows=("value_signature", "size"), unique_signatures=("value_signature", "nunique"))
    )

    base = {
        "station": csv_path.parent.name,
        "source_file": csv_path.as_posix(),
    }

    identical_rows: list[dict] = []
    conflicting_rows: list[dict] = []

    for _, row in grouped.iterrows():
        record = {
            **base,
            "date": row[timestamp_col].strftime("%Y-%m-%d"),
            "timestamp": row[timestamp_col].strftime("%Y-%m-%d %H:%M:%S"),
            "duplicate_rows": int(row["duplicate_rows"]),
        }
        if int(row["unique_signatures"]) == 1:
            identical_rows.append(record)
        else:
            conflicting_rows.append(record)

    return stats, identical_rows, conflicting_rows


def write_csv(path: Path, rows: list[dict]) -> None:
    columns = ["station", "date", "timestamp", "duplicate_rows", "source_file"]
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(path, index=False, encoding="utf-8")


def write_markdown_summary(
    out_path: Path,
    scanned_files: int,
    file_stats: list[FileStats],
    station_identical: Counter,
    station_conflicting: Counter,
    identical_count: int,
    conflicting_count: int,
) -> None:
    total_rows = sum(s.rows for s in file_stats)
    valid_rows = sum(s.valid_timestamp_rows for s in file_stats)

    lines = [
        "# Hourly Duplicate Timestamp Report",
        "",
        "## Summary",
        f"- Scanned files: {scanned_files}",
        f"- Total rows read: {total_rows}",
        f"- Rows with parseable timestamps: {valid_rows}",
        f"- Duplicate groups with identical values: {identical_count}",
        f"- Duplicate groups with conflicting values: {conflicting_count}",
        "",
        "## Duplicate groups (identical values)",
    ]

    if not station_identical:
        lines.append("- No identical-value duplicate groups found.")
    else:
        lines.append("- Top stations by identical-value duplicate group count:")
        for station, count in station_identical.most_common(20):
            lines.append(f"  - {station}: {count}")

    lines += ["", "## Duplicate groups (conflicting values)"]
    if not station_conflicting:
        lines.append("- No conflicting-value duplicate groups found.")
    else:
        lines.append("- Top stations by conflicting duplicate group count:")
        for station, count in station_conflicting.most_common(20):
            lines.append(f"  - {station}: {count}")

    lines += [
        "",
        "## Artifacts",
        "- `duplicate_identical.csv`: timestamp duplicates where all values are identical.",
        "- `duplicate_conflicting.csv`: timestamp duplicates where values differ.",
    ]

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = list(iter_station_csv_files(data_dir))
    if args.max_files is not None:
        files = files[: args.max_files]

    file_stats: list[FileStats] = []
    identical_rows: list[dict] = []
    conflicting_rows: list[dict] = []
    station_identical: Counter = Counter()
    station_conflicting: Counter = Counter()

    for i, csv_path in enumerate(files, start=1):
        try:
            stats, file_identical, file_conflicting = analyze_file(csv_path)
            file_stats.append(stats)
            identical_rows.extend(file_identical)
            conflicting_rows.extend(file_conflicting)
            if file_identical:
                station_identical[stats.station] += len(file_identical)
            if file_conflicting:
                station_conflicting[stats.station] += len(file_conflicting)
        except Exception as exc:  # pragma: no cover - defensive logging for data irregularities.
            print(f"[WARN] failed to parse {csv_path}: {exc}")

        if args.progress_every > 0 and i % args.progress_every == 0:
            print(
                f"[INFO] processed {i}/{len(files)} files | "
                f"identical={len(identical_rows)} conflicting={len(conflicting_rows)}"
            )

    identical_path = output_dir / "duplicate_identical.csv"
    conflicting_path = output_dir / "duplicate_conflicting.csv"
    markdown_path = output_dir / "duplicate_report.md"

    write_csv(identical_path, identical_rows)
    write_csv(conflicting_path, conflicting_rows)
    write_markdown_summary(
        out_path=markdown_path,
        scanned_files=len(files),
        file_stats=file_stats,
        station_identical=station_identical,
        station_conflicting=station_conflicting,
        identical_count=len(identical_rows),
        conflicting_count=len(conflicting_rows),
    )

    print(f"[INFO] wrote {markdown_path}")
    print(f"[INFO] wrote {identical_path} ({len(identical_rows)} rows)")
    print(f"[INFO] wrote {conflicting_path} ({len(conflicting_rows)} rows)")


if __name__ == "__main__":
    main()
