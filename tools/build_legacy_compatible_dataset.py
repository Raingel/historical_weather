#!/usr/bin/env python3
"""Build a legacy-compatible dataset from the raw CODIS rebuild output.

The transformer keeps the raw CODIS rebuild untouched and writes a second
dataset where:
1. Legacy columns come first, with the exact legacy column names/order.
2. Extra CODIS columns are appended after the legacy-compatible prefix.
3. Legacy-only columns without a raw CODIS source fall back to old dataset
   values when the same timestamp exists.
4. Old files with no rebuilt counterpart are copied forward as-is so the final
   dataset remains as compatible as possible with downstream consumers.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


FILE_SUFFIX = {
    "hourly": "",
    "daily": "_daily",
    "monthly": "_monthly",
}

LEGACY_DERIVED_COLUMNS: Dict[str, Dict[str, Sequence[str]]] = {
    "hourly": {},
    "daily": {
        "PrecpHrMax": ("Precipitation.HourlyMaximum", "Precipitation.HourlyMaximumf"),
        "PrecpHrMaxTime": ("Precipitation.HourlyMaximumTime", "Precipitation.HourlyMaximumTimef"),
    },
    "monthly": {
        "PrecpDay": ("Precipitation.PrecipitationDays", "Precipitation.PrecipitationDaysf"),
        "PrecpHrMax": ("Precipitation.HourlyMaximum", "Precipitation.HourlyMaximumf"),
        "PrecpHrMaxTime": ("Precipitation.HourlyMaximumTime", "Precipitation.HourlyMaximumTimef"),
        "Precp1DayMax": ("Precipitation.DailyMaximum", "Precipitation.DailyMaximumf"),
        "Precp1DayMaxTime": ("Precipitation.DailyMaximumDate", "Precipitation.DailyMaximumDatef"),
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a legacy-compatible dataset from raw CODIS outputs.")
    parser.add_argument("--raw-root", default="data_codis_rebuild_full")
    parser.add_argument("--legacy-root", default="data")
    parser.add_argument("--output-root", default="data_codis_legacy_compatible")
    parser.add_argument("--report-path", default="reports/codis_full_rebuild_notebook/legacy_compat_report.json")
    parser.add_argument("--station", action="append", dest="stations")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def detect_granularity(station_id: str, path: Path) -> Tuple[str, int]:
    name = path.stem
    if name.endswith("_daily"):
        return "daily", int(name[len(station_id) + 1 : -6])
    if name.endswith("_monthly"):
        return "monthly", int(name[len(station_id) + 1 : -8])
    return "hourly", int(name[len(station_id) + 1 :])


def output_path_for(output_root: Path, station_id: str, granularity: str, year: int) -> Path:
    station_dir = output_root / station_id
    station_dir.mkdir(parents=True, exist_ok=True)
    return station_dir / f"{station_id}_{year}{FILE_SUFFIX[granularity]}.csv"


def read_csv_rows(path: Path) -> Tuple[List[str], List[List[str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return [], []
        rows = [row for row in reader]
    return header, rows


def first_non_empty(row: Dict[str, str], candidates: Iterable[str]) -> str:
    for column in candidates:
        value = row.get(column, "")
        if value not in ("", None):
            return value
    return ""


def time_column_name(header: Sequence[str]) -> Optional[str]:
    if not header:
        return None
    if "timestamp" in header:
        return "timestamp"
    return header[0]


def build_row_dicts(header: Sequence[str], rows: Sequence[Sequence[str]]) -> List[Dict[str, str]]:
    row_dicts: List[Dict[str, str]] = []
    for row in rows:
        padded = list(row) + [""] * max(0, len(header) - len(row))
        row_dicts.append(dict(zip(header, padded)))
    return row_dicts


def build_old_lookup(header: Sequence[str], rows: Sequence[Sequence[str]]) -> Dict[str, Dict[str, str]]:
    time_col = time_column_name(header)
    if time_col is None:
        return {}
    lookup: Dict[str, Dict[str, str]] = {}
    for row_dict in build_row_dicts(header, rows):
        timestamp = row_dict.get(time_col, "")
        if timestamp and timestamp not in lookup:
            lookup[timestamp] = row_dict
    return lookup


def parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def timestamp_in_window(granularity: str, timestamp: str, start_day: date, end_day: date) -> bool:
    ts = datetime.fromisoformat(timestamp)
    if granularity == "hourly":
        window_start = datetime.combine(start_day, datetime.min.time()) + timedelta(hours=1)
        window_end = datetime.combine(end_day + timedelta(days=1), datetime.min.time())
        return window_start <= ts <= window_end
    if granularity == "daily":
        window_start = datetime.combine(start_day, datetime.min.time())
        window_end = datetime.combine(end_day, datetime.min.time())
        return window_start <= ts <= window_end
    month_start = start_day.replace(day=1)
    month_end = end_day.replace(day=1)
    ts_month = ts.date().replace(day=1)
    return month_start <= ts_month <= month_end


def merge_window_rows(
    granularity: str,
    target_header: Sequence[str],
    output_rows: Sequence[Sequence[str]],
    base_header: Sequence[str],
    base_rows: Sequence[Sequence[str]],
    start_day: Optional[date],
    end_day: Optional[date],
) -> Tuple[List[str], List[List[str]]]:
    if not start_day or not end_day or not base_header:
        return list(target_header), list(output_rows)

    final_header = list(target_header)
    for column in base_header:
        if column not in final_header:
            final_header.append(column)

    base_time_col = time_column_name(base_header)
    target_time_col = time_column_name(target_header)
    if base_time_col is None or target_time_col is None:
        return final_header, [list(row) + [""] * max(0, len(final_header) - len(row)) for row in output_rows]

    merged: Dict[str, Dict[str, str]] = {}
    for row_dict in build_row_dicts(base_header, base_rows):
        timestamp = row_dict.get(base_time_col, "")
        if not timestamp:
            continue
        merged[timestamp] = {column: row_dict.get(column, "") for column in final_header}

    for row_dict in build_row_dicts(target_header, output_rows):
        timestamp = row_dict.get(target_time_col, "")
        if not timestamp:
            continue
        merged[timestamp] = {column: row_dict.get(column, "") for column in final_header}

    ordered = sorted(merged.items(), key=lambda item: datetime.fromisoformat(item[0]))
    return final_header, [[row_dict.get(column, "") for column in final_header] for _, row_dict in ordered]


def should_process(station_id: str, year: int, args: argparse.Namespace) -> bool:
    if args.stations and station_id not in set(args.stations):
        return False
    if args.start_year is not None and year < args.start_year:
        return False
    if args.end_year is not None and year > args.end_year:
        return False
    return True


def compatible_header(legacy_header: Sequence[str], new_header: Sequence[str]) -> List[str]:
    if not legacy_header:
        return list(new_header)
    extras = []
    for column in new_header:
        if column == "timestamp" and legacy_header and legacy_header[0] == "":
            continue
        if column not in legacy_header:
            extras.append(column)
    return list(legacy_header) + extras


def build_compatible_rows(
    granularity: str,
    legacy_header: Sequence[str],
    new_header: Sequence[str],
    new_rows: Sequence[Sequence[str]],
    old_lookup: Dict[str, Dict[str, str]],
) -> List[List[str]]:
    if not new_header:
        return []

    target_header = compatible_header(legacy_header, new_header)
    new_time_col = time_column_name(new_header) or "timestamp"
    derived = LEGACY_DERIVED_COLUMNS.get(granularity, {})

    output_rows: List[List[str]] = []
    for new_row in build_row_dicts(new_header, new_rows):
        timestamp = new_row.get(new_time_col, "")
        old_row = old_lookup.get(timestamp, {})
        out: Dict[str, str] = {}

        for column in target_header:
            value = ""
            if column == "" and legacy_header and legacy_header[0] == "":
                value = timestamp
            elif column in new_row:
                value = new_row.get(column, "")
            elif column in derived:
                value = first_non_empty(new_row, derived[column])
            elif column in old_row:
                value = old_row.get(column, "")
            out[column] = value

        output_rows.append([out.get(column, "") for column in target_header])
    return output_rows


def copy_if_missing(src: Path, dest: Path, overwrite: bool) -> bool:
    if dest.exists() and not overwrite:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return True


def main() -> int:
    args = parse_args()
    raw_root = Path(args.raw_root)
    legacy_root = Path(args.legacy_root)
    output_root = Path(args.output_root)
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    selected_stations = set(args.stations or [])
    raw_files: Dict[Tuple[str, str, int], Path] = {}
    legacy_files: Dict[Tuple[str, str, int], Path] = {}

    for station_dir in raw_root.iterdir():
        if not station_dir.is_dir():
            continue
        for path in station_dir.glob("*.csv"):
            station_id = station_dir.name
            granularity, year = detect_granularity(station_id, path)
            if should_process(station_id, year, args):
                raw_files[(station_id, granularity, year)] = path

    if legacy_root.exists():
        for station_dir in legacy_root.iterdir():
            if not station_dir.is_dir():
                continue
            for path in station_dir.glob("*.csv"):
                station_id = station_dir.name
                granularity, year = detect_granularity(station_id, path)
                if should_process(station_id, year, args):
                    legacy_files[(station_id, granularity, year)] = path

    transformed = 0
    copied_old_only = 0
    passthrough_new_only = 0
    fallback_cells = 0
    derived_cells = 0
    processed_examples: List[Dict[str, object]] = []

    all_keys = sorted(set(raw_files) | set(legacy_files))
    for key in all_keys:
        station_id, granularity, year = key
        out_path = output_path_for(output_root, station_id, granularity, year)
        if out_path.exists() and not args.overwrite:
            continue

        raw_path = raw_files.get(key)
        legacy_path = legacy_files.get(key)

        if raw_path is None and legacy_path is not None:
            if copy_if_missing(legacy_path, out_path, overwrite=True):
                copied_old_only += 1
            continue

        if raw_path is None:
            continue

        new_header, new_rows = read_csv_rows(raw_path)
        legacy_header: List[str] = []
        legacy_rows: List[List[str]] = []
        if legacy_path is not None:
            legacy_header, legacy_rows = read_csv_rows(legacy_path)

        old_lookup = build_old_lookup(legacy_header, legacy_rows)
        output_rows = build_compatible_rows(granularity, legacy_header, new_header, new_rows, old_lookup)
        target_header = compatible_header(legacy_header, new_header)

        base_header = legacy_header
        base_rows = legacy_rows
        if not base_header and out_path.exists():
            base_header, base_rows = read_csv_rows(out_path)
        start_day = parse_iso_date(args.start_date)
        end_day = parse_iso_date(args.end_date)
        target_header, output_rows = merge_window_rows(
            granularity,
            target_header,
            output_rows,
            base_header,
            base_rows,
            start_day,
            end_day,
        )

        derived_map = LEGACY_DERIVED_COLUMNS.get(granularity, {})
        if legacy_header:
            new_row_dicts = build_row_dicts(new_header, new_rows)
            new_time_col = time_column_name(new_header) or "timestamp"
            for new_row in new_row_dicts:
                timestamp = new_row.get(new_time_col, "")
                old_row = old_lookup.get(timestamp, {})
                for column in legacy_header:
                    if column == "" and legacy_header[0] == "":
                        continue
                    if column not in new_row:
                        if column in derived_map and first_non_empty(new_row, derived_map[column]) not in ("", None):
                            derived_cells += 1
                        elif old_row.get(column, "") not in ("", None):
                            fallback_cells += 1

        with out_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(target_header)
            writer.writerows(output_rows)

        transformed += 1
        if legacy_path is None:
            passthrough_new_only += 1
        if args.verbose and len(processed_examples) < 20:
            processed_examples.append(
                {
                    "station_id": station_id,
                    "granularity": granularity,
                    "year": year,
                    "legacy_columns": len(legacy_header),
                    "new_columns": len(new_header),
                    "output_columns": len(target_header),
                    "rows": len(output_rows),
                    "legacy_source": str(legacy_path) if legacy_path else "",
                    "raw_source": str(raw_path),
                }
            )

    report = {
        "raw_root": str(raw_root.resolve()),
        "legacy_root": str(legacy_root.resolve()),
        "output_root": str(output_root.resolve()),
        "selected_stations": sorted(selected_stations),
        "start_date": args.start_date or "",
        "end_date": args.end_date or "",
        "raw_file_count": len(raw_files),
        "legacy_file_count": len(legacy_files),
        "transformed_files": transformed,
        "copied_old_only_files": copied_old_only,
        "passthrough_new_only_files": passthrough_new_only,
        "derived_legacy_cells": derived_cells,
        "fallback_legacy_cells_from_old": fallback_cells,
        "processed_examples": processed_examples,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
