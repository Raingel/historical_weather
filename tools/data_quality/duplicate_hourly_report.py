#!/usr/bin/env python3
"""Generate a duplicate-hour report for hourly weather station files."""

from __future__ import annotations

import argparse
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
    normalized = (
        df[value_columns]
        .replace({pd.NA: "", float("nan"): ""})
        .fillna("")
        .astype(str)
        .apply(lambda col: col.str.strip())
    )
    return normalized.apply(lambda row: "|".join(row.values.tolist()), axis=1)


def load_hourly_data(csv_path: Path) -> tuple[pd.DataFrame, FileStats]:
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
        return valid, stats

    # Keep hourly data only.
    valid = valid[valid[timestamp_col].dt.floor("h") == valid[timestamp_col]].copy()
    valid["timestamp"] = valid[timestamp_col]
    valid["date"] = valid["timestamp"].dt.date
    value_columns = [col for col in valid.columns if col not in {timestamp_col, "timestamp", "date"}]
    valid["value_signature"] = normalize_values(valid, value_columns)
    valid["station"] = csv_path.parent.name
    valid["source_file"] = csv_path.as_posix()
    return valid[["station", "date", "timestamp", "value_signature", "source_file"]], stats


def build_report_rows(all_hourly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if all_hourly.empty:
        base_cols = ["station", "date", "timestamp", "duplicate_rows", "source_file"]
        return pd.DataFrame(columns=base_cols), pd.DataFrame(columns=base_cols)

    grouped = (
        all_hourly.groupby(["station", "date", "timestamp", "source_file"], as_index=False)
        .agg(duplicate_rows=("value_signature", "size"), unique_signatures=("value_signature", "nunique"))
    )
    duplicate_only = grouped[grouped["duplicate_rows"] > 1].copy()

    identical = duplicate_only[duplicate_only["unique_signatures"] == 1].copy()
    conflicting = duplicate_only[duplicate_only["unique_signatures"] > 1].copy()

    for frame in (identical, conflicting):
        frame["timestamp"] = frame["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        frame["date"] = frame["date"].astype(str)

    keep_cols = ["station", "date", "timestamp", "duplicate_rows", "source_file"]
    return identical[keep_cols], conflicting[keep_cols]


def write_markdown_summary(
    out_path: Path,
    scanned_files: int,
    file_stats: list[FileStats],
    identical: pd.DataFrame,
    conflicting: pd.DataFrame,
) -> None:
    total_rows = sum(s.rows for s in file_stats)
    valid_rows = sum(s.valid_timestamp_rows for s in file_stats)

    station_identical = identical.groupby("station").size().sort_values(ascending=False)
    station_conflicting = conflicting.groupby("station").size().sort_values(ascending=False)

    lines = [
        "# Hourly Duplicate Timestamp Report",
        "",
        "## Summary",
        f"- Scanned files: {scanned_files}",
        f"- Total rows read: {total_rows}",
        f"- Rows with parseable timestamps: {valid_rows}",
        f"- Duplicate groups with identical values: {len(identical)}",
        f"- Duplicate groups with conflicting values: {len(conflicting)}",
        "",
        "## Duplicate groups (identical values)",
    ]

    if station_identical.empty:
        lines.append("- No identical-value duplicate groups found.")
    else:
        lines.append("- Top stations by identical-value duplicate group count:")
        for station, count in station_identical.head(20).items():
            lines.append(f"  - {station}: {count}")

    lines += ["", "## Duplicate groups (conflicting values)"]
    if station_conflicting.empty:
        lines.append("- No conflicting-value duplicate groups found.")
    else:
        lines.append("- Top stations by conflicting duplicate group count:")
        for station, count in station_conflicting.head(20).items():
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
    hourly_frames: list[pd.DataFrame] = []

    for csv_path in files:
        try:
            hourly_df, stats = load_hourly_data(csv_path)
            file_stats.append(stats)
            if not hourly_df.empty:
                hourly_frames.append(hourly_df)
        except Exception as exc:  # pragma: no cover - defensive logging for data irregularities.
            print(f"[WARN] failed to parse {csv_path}: {exc}")

    all_hourly = pd.concat(hourly_frames, ignore_index=True) if hourly_frames else pd.DataFrame()
    identical, conflicting = build_report_rows(all_hourly)

    identical_path = output_dir / "duplicate_identical.csv"
    conflicting_path = output_dir / "duplicate_conflicting.csv"
    markdown_path = output_dir / "duplicate_report.md"

    identical.to_csv(identical_path, index=False, encoding="utf-8")
    conflicting.to_csv(conflicting_path, index=False, encoding="utf-8")
    write_markdown_summary(markdown_path, len(files), file_stats, identical, conflicting)

    print(f"[INFO] wrote {markdown_path}")
    print(f"[INFO] wrote {identical_path} ({len(identical)} rows)")
    print(f"[INFO] wrote {conflicting_path} ({len(conflicting)} rows)")


if __name__ == "__main__":
    main()
