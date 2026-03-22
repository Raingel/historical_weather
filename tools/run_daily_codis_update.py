#!/usr/bin/env python3
"""Daily updater for the CODIS raw + legacy-compatible datasets.

Pipeline:
1. Refresh a recent CODIS window into data_codis_rebuild_full.
2. Rebuild matching legacy-compatible files into data_codis_legacy_compatible.
3. Sync compatible files back into data/ so existing downstream code keeps working.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the daily CODIS update pipeline.')
    parser.add_argument('--raw-root', default='data_codis_rebuild_full')
    parser.add_argument('--compat-root', default='data_codis_legacy_compatible')
    parser.add_argument('--data-root', default='data')
    parser.add_argument('--report-dir', default='reports/codis_daily_update')
    parser.add_argument('--start-year', type=int)
    parser.add_argument('--end-year', type=int)
    parser.add_argument('--start-date')
    parser.add_argument('--end-date')
    parser.add_argument('--lookback-days', type=int, default=60)
    parser.add_argument('--station', action='append', dest='stations')
    parser.add_argument('--granularity', nargs='+', choices=['hourly', 'daily', 'monthly'], default=['hourly', 'daily', 'monthly'])
    parser.add_argument('--skip-sync-data', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    if args.lookback_days < 1:
        parser.error('--lookback-days must be at least 1')
    return args


def target_date_range(args: argparse.Namespace) -> Tuple[date, date]:
    if args.start_date and args.end_date:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date)
    elif args.start_year is not None and args.end_year is not None:
        start = date(args.start_year, 1, 1)
        end = date(args.end_year, 12, 31)
    else:
        end = datetime.now().date()
        start = end - timedelta(days=args.lookback_days - 1)
    if start > end:
        raise ValueError('start date must not be later than end date')
    return start, end


def target_years(args: argparse.Namespace) -> List[int]:
    start, end = target_date_range(args)
    return list(range(start.year, end.year + 1))


def run_command(command: List[str], cwd: Path) -> None:
    print('RUN', ' '.join(command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def sync_compat_to_data(compat_root: Path, data_root: Path, years: List[int], stations: List[str]) -> int:
    copied = 0
    station_filter = set(stations)
    for station_dir in compat_root.iterdir():
        if not station_dir.is_dir():
            continue
        station_id = station_dir.name
        if station_filter and station_id not in station_filter:
            continue
        for path in station_dir.glob('*.csv'):
            name = path.stem
            if name.endswith('_daily'):
                year = int(name[len(station_id) + 1 : -6])
            elif name.endswith('_monthly'):
                year = int(name[len(station_id) + 1 : -8])
            else:
                year = int(name[len(station_id) + 1 :])
            if year not in years:
                continue
            dest = data_root / station_id / path.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            copied += 1
    return copied


def main() -> int:
    args = parse_args()
    cwd = Path.cwd()
    report_dir = cwd / args.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    start_date, end_date = target_date_range(args)
    years = target_years(args)
    stations = args.stations or []
    start_year = min(years)
    end_year = max(years)

    raw_cmd = [
        sys.executable,
        'tools/rebuild_codis_database.py',
        '--output-root',
        args.raw_root,
        '--report-dir',
        str(report_dir / 'raw'),
        '--start-year',
        str(start_year),
        '--end-year',
        str(end_year),
        '--start-date',
        start_date.isoformat(),
        '--end-date',
        end_date.isoformat(),
        '--overwrite',
    ]
    if args.verbose:
        raw_cmd.append('--verbose')
    raw_cmd.append('--granularity')
    raw_cmd.extend(args.granularity)
    for station in stations:
        raw_cmd.extend(['--station', station])
    run_command(raw_cmd, cwd)

    compat_cmd = [
        sys.executable,
        'tools/build_legacy_compatible_dataset.py',
        '--raw-root',
        args.raw_root,
        '--legacy-root',
        args.data_root,
        '--output-root',
        args.compat_root,
        '--report-path',
        str(report_dir / 'legacy_compat_report.json'),
        '--start-year',
        str(start_year),
        '--end-year',
        str(end_year),
        '--start-date',
        start_date.isoformat(),
        '--end-date',
        end_date.isoformat(),
        '--overwrite',
    ]
    if args.verbose:
        compat_cmd.append('--verbose')
    for station in stations:
        compat_cmd.extend(['--station', station])
    run_command(compat_cmd, cwd)

    copied = 0
    if not args.skip_sync_data:
        copied = sync_compat_to_data(cwd / args.compat_root, cwd / args.data_root, years, stations)
        print(f'Synced {copied} compatible files into {args.data_root}', flush=True)

    summary = {
        'timestamp': datetime.now().isoformat(timespec='seconds'),
        'years': years,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'lookback_days': args.lookback_days,
        'stations': stations,
        'granularity': args.granularity,
        'raw_root': str((cwd / args.raw_root).resolve()),
        'compat_root': str((cwd / args.compat_root).resolve()),
        'data_root': str((cwd / args.data_root).resolve()),
        'synced_to_data_files': copied,
        'skip_sync_data': args.skip_sync_data,
    }
    (report_dir / 'daily_update_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
