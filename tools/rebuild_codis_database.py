# %%
#!/usr/bin/env python3
"""Rebuild Taiwan historical weather files from the official CODIS StationData API.

This tool is intentionally isolated from the legacy update scripts so we can
verify a CODIS-only rebuild before replacing the current database.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import requests
import time


STATION_LIST_URL = "https://codis.cwa.gov.tw/api/station_list"
DEFAULT_REFERENCE_STATION_LIST = (
    "https://raw.githubusercontent.com/Raingel/weather_station_list/refs/heads/main/data/weather_sta_list.csv"
)
DEFAULT_USER_AGENT = "historical-weather-codis-rebuild/1.0"


REQUEST_HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://codis.cwa.gov.tw/StationData",
    "user-agent": DEFAULT_USER_AGENT,
}


LEGACY_COLUMN_ALIASES: Dict[str, Dict[str, str]] = {
    "hourly": {
        "StationPressure.Instantaneous": "StnPres",
        "SeaLevelPressure.Instantaneous": "SeaPres",
        "AirTemperature.Instantaneous": "Tx",
        "DewPointTemperature.Instantaneous": "Td",
        "RelativeHumidity.Instantaneous": "RH",
        "WindSpeed.Mean": "WS",
        "WindDirection.Mean": "WD",
        "PeakGust.Maximum": "WSGust",
        "PeakGust.Direction": "WDGust",
        "Precipitation.Accumulation": "Precp",
        "PrecipitationDuration.Total": "PrecpHour",
        "SunshineDuration.Total": "SunShine",
        "GlobalSolarRadiation.Accumulation": "GloblRad",
        "Visibility.Instantaneous": "Visb",
        "UVIndex.Accumulation": "UVI",
        "TotalCloudAmount.Instantaneous": "CloudAmount",
        "SoilTemperatureAt0cm.Instantaneous": "TxSoil0cm",
        "SoilTemperatureAt5cm.Instantaneous": "TxSoil5cm",
        "SoilTemperatureAt10cm.Instantaneous": "TxSoil10cm",
        "SoilTemperatureAt20cm.Instantaneous": "TxSoil20cm",
        "SoilTemperatureAt30cm.Instantaneous": "TxSoil30cm",
        "SoilTemperatureAt50cm.Instantaneous": "TxSoil50cm",
        "SoilTemperatureAt100cm.Instantaneous": "TxSoil100cm",
        "SoilTemperatureAt200cm.Instantaneous": "TxSoil200cm",
    },
    "daily": {
        "StationPressure.Mean": "StnPres",
        "SeaLevelPressure.Mean": "SeaPres",
        "StationPressure.Maximum": "StnPresMax",
        "StationPressure.MaximumTime": "StnPresMaxTime",
        "StationPressure.Minimum": "StnPresMin",
        "StationPressure.MinimumTime": "StnPresMinTime",
        "AirTemperature.Mean": "Tx",
        "AirTemperature.Maximum": "TxMaxAbs",
        "AirTemperature.MaximumTime": "TxMaxAbsTime",
        "AirTemperature.Minimum": "TxMinAbs",
        "AirTemperature.MinimumTime": "TxMinAbsTime",
        "AirTemperature.DailyRange": "TxRange",
        "DewPointTemperature.Mean": "Td",
        "RelativeHumidity.Mean": "RH",
        "RelativeHumidity.Minimum": "RHMin",
        "RelativeHumidity.MinimumTime": "RHMinTime",
        "WindSpeed.Mean": "WS",
        "WindDirection.Prevailing": "WD",
        "PeakGust.Maximum": "WSGust",
        "PeakGust.Direction": "WDGust",
        "PeakGust.MaximumTime": "WGustTime",
        "Precipitation.Accumulation": "Precp",
        "PrecipitationDuration.Total": "PrecpHour",
        "Precipitation.TenMinutelyMaximum": "PrecpMax10",
        "Precipitation.TenMinutelyMaximumTime": "PrecpMax10Time",
        "Precipitation.SixtyMinutelyMaximum": "PrecpMax60",
        "Precipitation.SixtyMinutelyMaximumTime": "PrecpMax60Time",
        "SunshineDuration.Total": "SunShine",
        "SunshineDuration.Rate": "SunShineRate",
        "GlobalSolarRadiation.Accumulation": "GloblRad",
        "GlobalSolarRadiation.HourlyMaximum": "GloblRadMax",
        "GlobalSolarRadiation.HourlyMaximumTime": "GloblRadMaxTime",
        "Visibility.Mean": "VisbMean",
        "Visibility.AutoMean": "VisbAutoMean",
        "UVIndex.Maximum": "UVIMax",
        "UVIndex.MaximumTime": "UVIMaxTime",
        "TotalCloudAmount.Mean": "CloudAmount",
        "TotalCloudAmount.SatRetrievedMean": "CloudAmountSat",
        "EvaporationClassAPan.Accumulation": "EvapA",
        "SoilTemperatureAt0cm.Mean": "TxSoil0cm",
        "SoilTemperatureAt5cm.Mean": "TxSoil5cm",
        "SoilTemperatureAt10cm.Mean": "TxSoil10cm",
        "SoilTemperatureAt20cm.Mean": "TxSoil20cm",
        "SoilTemperatureAt30cm.Mean": "TxSoil30cm",
        "SoilTemperatureAt50cm.Mean": "TxSoil50cm",
        "SoilTemperatureAt100cm.Mean": "TxSoil100cm",
    },
    "monthly": {
        "StationPressure.Mean": "StnPres",
        "SeaLevelPressure.Mean": "SeaPres",
        "StationPressure.Maximum": "StnPresMax",
        "StationPressure.MaximumTime": "StnPresMaxTime",
        "StationPressure.Minimum": "StnPresMin",
        "StationPressure.MinimumTime": "StnPresMinTime",
        "AirTemperature.Mean": "Tx",
        "AirTemperature.Maximum": "TxMaxAbs",
        "AirTemperature.MaximumTime": "TxMaxAbsTime",
        "AirTemperature.Minimum": "TxMinAbs",
        "AirTemperature.MinimumTime": "TxMinAbsTime",
        "AirTemperature.DailyRange": "TxRange",
        "DewPointTemperature.Mean": "Td",
        "RelativeHumidity.Mean": "RH",
        "RelativeHumidity.Minimum": "RHMin",
        "RelativeHumidity.MinimumTime": "RHMinTime",
        "WindSpeed.Mean": "WS",
        "WindDirection.Prevailing": "WD",
        "PeakGust.Maximum": "WSGust",
        "PeakGust.Direction": "WDGust",
        "PeakGust.MaximumTime": "WGustTime",
        "Precipitation.Accumulation": "Precp",
        "PrecipitationDuration.Total": "PrecpHour",
        "Precipitation.TenMinutelyMaximum": "PrecpMax10",
        "Precipitation.TenMinutelyMaximumTime": "PrecpMax10Time",
        "Precipitation.SixtyMinutelyMaximum": "PrecpMax60",
        "Precipitation.SixtyMinutelyMaximumTime": "PrecpMax60Time",
        "SunshineDuration.Total": "SunShine",
        "SunshineDuration.Rate": "SunShineRate",
        "GlobalSolarRadiation.Accumulation": "GloblRad",
        "GlobalSolarRadiation.HourlyMaximum": "GloblRadMax",
        "GlobalSolarRadiation.HourlyMaximumTime": "GloblRadMaxTime",
        "Visibility.Mean": "VisbMean",
        "Visibility.AutoMean": "VisbAutoMean",
        "UVIndex.Maximum": "UVIMax",
        "UVIndex.MaximumTime": "UVIMaxTime",
        "TotalCloudAmount.Mean": "CloudAmount",
        "TotalCloudAmount.SatRetrievedMean": "CloudAmountSat",
        "EvaporationClassAPan.Accumulation": "EvapA",
        "SoilTemperatureAt0cm.Mean": "TxSoil0cm",
        "SoilTemperatureAt5cm.Mean": "TxSoil5cm",
        "SoilTemperatureAt10cm.Mean": "TxSoil10cm",
        "SoilTemperatureAt20cm.Mean": "TxSoil20cm",
        "SoilTemperatureAt30cm.Mean": "TxSoil30cm",
        "SoilTemperatureAt50cm.Mean": "TxSoil50cm",
        "SoilTemperatureAt100cm.Mean": "TxSoil100cm",
    },
}


REQUEST_TYPES = {
    "hourly": "report_date",
    "daily": "report_month",
    "monthly": "report_year",
}

TIMESTAMP_KEYS = {
    "hourly": "DataTime",
    "daily": "DataDate",
    "monthly": "DataYearMonth",
}

FILE_SUFFIX = {
    "hourly": "",
    "daily": "_daily",
    "monthly": "_monthly",
}


@dataclass(frozen=True)
class StationPlan:
    station_id: str
    stn_type: str
    station_name: str
    planned_start: date
    planned_end: date
    codis_station_start: Optional[date]
    codis_station_end: Optional[date]
    codis_data_start: Optional[date]
    codis_data_end: Optional[date]
    ref_data_start: Optional[date]
    ref_station_end: Optional[date]
    station_before: str
    station_after: str
    note: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download and normalize StationData observations from CODIS into the "
            "existing station/year CSV layout."
        )
    )
    parser.add_argument("--output-root", default="data_codis_rebuild")
    parser.add_argument("--report-dir", default="reports/codis_rebuild")
    parser.add_argument("--station", action="append", dest="stations")
    parser.add_argument(
        "--granularity",
        nargs="+",
        choices=["hourly", "daily", "monthly"],
        default=["hourly", "daily", "monthly"],
    )
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--max-stations", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--skip-failed-chunks", action="store_true", default=True)
    parser.add_argument("--strict-failed-chunks", action="store_false", dest="skip_failed_chunks")
    parser.add_argument("--pause-seconds", type=float, default=0.0)
    parser.add_argument(
        "--trace-precipitation-mode",
        choices=["raw", "legacy_0.09"],
        default="raw",
    )
    parser.add_argument(
        "--reference-station-list",
        default=DEFAULT_REFERENCE_STATION_LIST,
    )
    parser.add_argument("--write-plan-only", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def parse_date(value: object) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace("/", "-")
    if len(text) == 7:
        text = f"{text}-01"
    try:
        return pd.to_datetime(text).date()
    except Exception:
        return None


def parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def coalesce_earliest(values: Sequence[Optional[date]]) -> Optional[date]:
    valid = [value for value in values if value is not None]
    return min(valid) if valid else None


def coalesce_latest(values: Sequence[Optional[date]]) -> Optional[date]:
    valid = [value for value in values if value is not None]
    return max(valid) if valid else None


def month_end(value: date) -> date:
    next_month = (value.replace(day=28) + timedelta(days=4)).replace(day=1)
    return next_month - timedelta(days=1)


def shift_2359_to_midnight(value: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(value):
        return value
    if value.strftime('%H:%M') == '23:59':
        return value + pd.Timedelta(minutes=1)
    return value


def iter_month_ranges(start_day: date, end_day: date) -> Iterator[Tuple[date, date]]:
    current = start_day
    while current <= end_day:
        current_end = min(month_end(current), end_day)
        yield current, current_end
        current = current_end + timedelta(days=1)


def flatten_dict(prefix: str, value: object, out: Dict[str, object]) -> None:
    if isinstance(value, dict):
        for key, nested_value in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else key
            flatten_dict(next_prefix, nested_value, out)
        return
    out[prefix] = value


def normalize_column_name(granularity: str, raw_name: str) -> str:
    alias = LEGACY_COLUMN_ALIASES.get(granularity, {}).get(raw_name)
    if alias:
        return alias
    return raw_name.replace(" ", "_").replace("/", "_")


def normalize_station_type(station_id: str, raw_type: str) -> str:
    if raw_type == 'auto':
        if station_id.startswith('C0'):
            return 'auto_C0'
        if station_id.startswith('C1'):
            return 'auto_C1'
        return 'autotypeA'
    return raw_type


def normalize_value(column_name: str, value: object, trace_mode: str) -> object:
    if value is None:
        return value
    if trace_mode == "legacy_0.09" and column_name == "Precp" and value == -9.8:
        return 0.09
    return value


class CodisClient:
    def __init__(self, timeout: int, max_retries: int, retry_backoff_seconds: float, verbose: bool = False) -> None:
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def get_station_catalog(self) -> pd.DataFrame:
        response = self.session.get(STATION_LIST_URL, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        rows: List[Dict[str, object]] = []
        for group in payload.get("data", []):
            stn_type = group.get("stationAttribute", "")
            for item in group.get("item", []):
                row = dict(item)
                row["stn_type"] = stn_type
                rows.append(row)
        df = pd.DataFrame(rows)
        if df.empty:
            raise RuntimeError("CODIS station_list returned no stations.")
        return df

    def fetch_observations(
        self,
        station_id: str,
        stn_type: str,
        granularity: str,
        start_day: date,
        end_day: date,
    ) -> Dict[str, object]:
        payload = {
            "date": f"{start_day.isoformat()}T00:00:00.000+08:00",
            "type": REQUEST_TYPES[granularity],
            "stn_ID": station_id,
            "stn_type": stn_type,
            "start": f"{start_day.isoformat()}T00:00:00",
            "end": f"{end_day.isoformat()}T23:59:59" if granularity == "hourly" else f"{end_day.isoformat()}T00:00:00",
        }
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    "https://codis.cwa.gov.tw/api/station",
                    data=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                wait_seconds = self.retry_backoff_seconds * attempt
                if self.verbose:
                    print(
                        f"[WARN] CODIS request failed station={station_id} granularity={granularity} "
                        f"range={start_day.isoformat()}..{end_day.isoformat()} "
                        f"attempt={attempt}/{self.max_retries} wait={wait_seconds:.1f}s error={exc}"
                    )
                if wait_seconds > 0:
                    time.sleep(wait_seconds)

        assert last_error is not None
        raise last_error


def load_reference_station_list(source: str) -> pd.DataFrame:
    if not source:
        return pd.DataFrame()
    df = pd.read_csv(source, encoding="utf-8-sig")
    renamed = df.rename(
        columns={
            "站號": "stationID",
            "資料起始日期": "ref_data_start",
            "撤站日期": "ref_station_end",
            "站名": "ref_station_name",
            "原站號": "ref_station_before",
            "新站號": "ref_station_after",
        }
    )
    if "stationID" not in renamed.columns:
        raise ValueError("Reference station list is missing the stationID/站號 column.")
    renamed["stationID"] = renamed["stationID"].astype(str)
    return renamed


def build_station_plans(
    catalog_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    args: argparse.Namespace,
) -> List[StationPlan]:
    ref_lookup = (
        reference_df.set_index("stationID", drop=False).to_dict("index")
        if not reference_df.empty
        else {}
    )
    selected_ids = set(args.stations or [])
    today_local = datetime.now().date()

    plans: List[StationPlan] = []
    for _, row in catalog_df.sort_values(["stationID", "stn_type"]).iterrows():
        station_id = str(row["stationID"])
        if selected_ids and station_id not in selected_ids:
            continue

        ref_row = ref_lookup.get(station_id, {})
        codis_station_start = parse_date(row.get("stationStartDate"))
        codis_station_end = parse_date(row.get("stationEndDate"))
        codis_data_start = parse_date(row.get("dataStartDate"))
        codis_data_end = parse_date(row.get("dataEndDate"))
        ref_data_start = parse_date(ref_row.get("ref_data_start"))
        ref_station_end = parse_date(ref_row.get("ref_station_end"))

        planned_start = coalesce_earliest([codis_data_start, codis_station_start, ref_data_start])
        if planned_start is None:
            continue

        planned_end = coalesce_latest([codis_data_end, codis_station_end, ref_station_end]) or today_local
        if args.start_year is not None:
            planned_start = max(planned_start, date(args.start_year, 1, 1))
        if args.end_year is not None:
            planned_end = min(planned_end, date(args.end_year, 12, 31))
        if args.start_date:
            planned_start = max(planned_start, parse_iso_date(args.start_date))
        if args.end_date:
            planned_end = min(planned_end, parse_iso_date(args.end_date))
        if planned_start > planned_end:
            continue

        note_parts = []
        if row.get("webRemark"):
            note_parts.append(str(row.get("webRemark")).strip())
        if row.get("remark"):
            note_parts.append(str(row.get("remark")).strip())

        plans.append(
            StationPlan(
                station_id=station_id,
                stn_type=normalize_station_type(station_id, str(row.get("stn_type", ""))),
                station_name=str(row.get("stationName", "")),
                planned_start=planned_start,
                planned_end=planned_end,
                codis_station_start=codis_station_start,
                codis_station_end=codis_station_end,
                codis_data_start=codis_data_start,
                codis_data_end=codis_data_end,
                ref_data_start=ref_data_start,
                ref_station_end=ref_station_end,
                station_before=str(row.get("stationIDBefore", "") or ref_row.get("ref_station_before", "") or ""),
                station_after=str(row.get("stationIDAfter", "") or ref_row.get("ref_station_after", "") or ""),
                note=" | ".join([part for part in note_parts if part]),
            )
        )

    if args.max_stations is not None:
        plans = plans[: args.max_stations]
    return plans


def rows_to_dataframe(
    payload: Dict[str, object],
    granularity: str,
    trace_mode: str,
) -> pd.DataFrame:
    data = payload.get("data") or []
    if not data:
        return pd.DataFrame()
    entries = data[0].get("dts") or []
    if not entries:
        return pd.DataFrame()

    timestamp_key = TIMESTAMP_KEYS[granularity]
    normalized_rows: List[Dict[str, object]] = []
    for entry in entries:
        if timestamp_key not in entry:
            continue
        row: Dict[str, object] = {"timestamp": entry[timestamp_key]}
        for key, value in entry.items():
            if key == timestamp_key:
                continue
            if isinstance(value, dict):
                flattened: Dict[str, object] = {}
                flatten_dict(key, value, flattened)
                for raw_name, raw_value in flattened.items():
                    column_name = normalize_column_name(granularity, raw_name)
                    row[column_name] = normalize_value(column_name, raw_value, trace_mode)
            else:
                row[normalize_column_name(granularity, key)] = value
        normalized_rows.append(row)

    df = pd.DataFrame(normalized_rows)
    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if granularity == 'hourly':
        df["timestamp"] = df["timestamp"].apply(shift_2359_to_midnight)
    df = df[df["timestamp"].notna()].copy()
    if df.empty:
        return df

    df.sort_values("timestamp", inplace=True)
    df.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)
    ordered_columns = ["timestamp"] + sorted([col for col in df.columns if col != "timestamp"])
    return df.loc[:, ordered_columns]


def combine_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    valid = [frame for frame in frames if not frame.empty]
    if not valid:
        return pd.DataFrame()
    combined = pd.concat(valid, ignore_index=True, sort=False)
    combined.sort_values("timestamp", inplace=True)
    combined.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)
    ordered_columns = ["timestamp"] + sorted([col for col in combined.columns if col != "timestamp"])
    return combined.loc[:, ordered_columns]


def write_station_plan_report(plans: Sequence[StationPlan], report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "station_plan.csv"
    rows = [
        {
            "station_id": plan.station_id,
            "station_name": plan.station_name,
            "stn_type": plan.stn_type,
            "planned_start": plan.planned_start.isoformat(),
            "planned_end": plan.planned_end.isoformat(),
            "codis_station_start": plan.codis_station_start.isoformat() if plan.codis_station_start else "",
            "codis_station_end": plan.codis_station_end.isoformat() if plan.codis_station_end else "",
            "codis_data_start": plan.codis_data_start.isoformat() if plan.codis_data_start else "",
            "codis_data_end": plan.codis_data_end.isoformat() if plan.codis_data_end else "",
            "reference_data_start": plan.ref_data_start.isoformat() if plan.ref_data_start else "",
            "reference_station_end": plan.ref_station_end.isoformat() if plan.ref_station_end else "",
            "station_before": plan.station_before,
            "station_after": plan.station_after,
            "note": plan.note,
        }
        for plan in plans
    ]
    pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def year_output_path(output_root: Path, station_id: str, granularity: str, year: int) -> Path:
    station_dir = output_root / station_id
    station_dir.mkdir(parents=True, exist_ok=True)
    suffix = FILE_SUFFIX[granularity]
    return station_dir / f"{station_id}_{year}{suffix}.csv"


def frames_for_station_year(
    client: CodisClient,
    plan: StationPlan,
    granularity: str,
    year: int,
    args: argparse.Namespace,
) -> pd.DataFrame:
    start_day = max(plan.planned_start, date(year, 1, 1))
    end_day = min(plan.planned_end, date(year, 12, 31))
    if start_day > end_day:
        return pd.DataFrame()

    chunk_ranges = iter_month_ranges(start_day, end_day) if granularity in {"hourly", "daily"} else ((start_day, end_day),)
    frames: List[pd.DataFrame] = []
    for chunk_start, chunk_end in chunk_ranges:
        if args.verbose:
            print(f"[INFO] fetch {granularity} station={plan.station_id} {chunk_start.isoformat()}..{chunk_end.isoformat()}")
        try:
            payload = client.fetch_observations(plan.station_id, plan.stn_type, granularity, chunk_start, chunk_end)
            frames.append(rows_to_dataframe(payload, granularity, args.trace_precipitation_mode))
        except requests.RequestException as exc:
            message = (
                f"[WARN] skip chunk station={plan.station_id} granularity={granularity} "
                f"range={chunk_start.isoformat()}..{chunk_end.isoformat()} error={exc}"
            )
            if args.skip_failed_chunks:
                print(message)
                continue
            raise RuntimeError(message) from exc
        if args.pause_seconds > 0:
            time.sleep(args.pause_seconds)

    combined = combine_frames(frames)
    if combined.empty:
        return combined

    combined["timestamp"] = pd.to_datetime(combined["timestamp"], errors="coerce")
    combined = combined[combined["timestamp"].notna()].copy()
    if granularity == "hourly":
        start_ts = pd.Timestamp(start_day) + pd.Timedelta(hours=1)
        end_ts = pd.Timestamp(end_day) + pd.Timedelta(days=1)
        combined = combined[(combined["timestamp"] >= start_ts) & (combined["timestamp"] <= end_ts)].copy()
    elif granularity == "daily":
        start_ts = pd.Timestamp(start_day)
        end_ts = pd.Timestamp(end_day)
        combined = combined[(combined["timestamp"] >= start_ts) & (combined["timestamp"] <= end_ts)].copy()
    else:
        combined = combined[combined["timestamp"].dt.year == year].copy()
    combined.sort_values("timestamp", inplace=True)
    return combined


def format_timestamp_for_csv(granularity: str, series: pd.Series) -> pd.Series:
    if granularity == "monthly":
        return series.dt.strftime("%Y-%m-01")
    if granularity == "hourly":
        return series.dt.strftime("%Y-%m-%d %H:%M:%S")
    return series.dt.strftime("%Y-%m-%d")


def timestamp_window(
    granularity: str,
    start_day: date,
    end_day: date,
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = pd.Timestamp(start_day)
    if granularity == "hourly":
        return start_ts + pd.Timedelta(hours=1), pd.Timestamp(end_day) + pd.Timedelta(days=1)
    if granularity == "daily":
        return start_ts, pd.Timestamp(end_day)
    start_month = pd.Timestamp(start_day).to_period("M").to_timestamp()
    end_month = pd.Timestamp(end_day).to_period("M").to_timestamp()
    return start_month, end_month


def read_existing_year_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig")
    if "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"].notna()].copy()
    if df.empty:
        return df
    df.sort_values("timestamp", inplace=True)
    df.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
    ordered_columns = ["timestamp"] + [col for col in df.columns if col != "timestamp"]
    return df.loc[:, ordered_columns]


def merge_window_into_year(
    existing_df: pd.DataFrame,
    window_df: pd.DataFrame,
    granularity: str,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    if existing_df.empty:
        merged = window_df.copy()
    elif window_df.empty:
        merged = existing_df.copy()
    else:
        start_ts, end_ts = timestamp_window(granularity, start_day, end_day)
        if granularity == "monthly":
            existing_mask = existing_df["timestamp"].dt.to_period("M").between(
                start_ts.to_period("M"),
                end_ts.to_period("M"),
            )
        else:
            existing_mask = existing_df["timestamp"].between(start_ts, end_ts)
        kept_existing = existing_df.loc[~existing_mask].copy()
        merged = combine_frames([kept_existing, window_df])
    if merged.empty:
        return merged
    merged.sort_values("timestamp", inplace=True)
    merged.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
    ordered_columns = ["timestamp"] + sorted([col for col in merged.columns if col != "timestamp"])
    return merged.loc[:, ordered_columns]


def rebuild_station_data(
    client: CodisClient,
    plans: Sequence[StationPlan],
    output_root: Path,
    report_dir: Path,
    args: argparse.Namespace,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    span_rows: List[Dict[str, object]] = []

    for plan in plans:
        for granularity in args.granularity:
            for year in range(plan.planned_start.year, plan.planned_end.year + 1):
                out_path = year_output_path(output_root, plan.station_id, granularity, year)
                partial_window = bool(args.start_date or args.end_date)
                if out_path.exists() and not args.overwrite and not partial_window:
                    continue

                year_start = max(plan.planned_start, date(year, 1, 1))
                year_end = min(plan.planned_end, date(year, 12, 31))
                df = frames_for_station_year(client, plan, granularity, year, args)
                existing_df = read_existing_year_file(out_path) if partial_window else pd.DataFrame()
                final_df = df
                status = "written"
                if partial_window:
                    final_df = merge_window_into_year(existing_df, df, granularity, year_start, year_end)
                    if df.empty and not existing_df.empty:
                        status = "retained_existing"

                if final_df.empty:
                    span_rows.append(
                        {
                            "station_id": plan.station_id,
                            "station_name": plan.station_name,
                            "stn_type": plan.stn_type,
                            "granularity": granularity,
                            "year": year,
                            "row_count": 0,
                            "observed_start": "",
                            "observed_end": "",
                            "planned_start": year_start.isoformat(),
                            "planned_end": year_end.isoformat(),
                            "output_path": str(out_path),
                            "status": "empty",
                        }
                    )
                    continue

                csv_df = final_df.copy()
                csv_df["timestamp"] = format_timestamp_for_csv(granularity, csv_df["timestamp"])
                csv_df.to_csv(out_path, index=False, encoding="utf-8-sig")

                observed_start = final_df["timestamp"].min()
                observed_end = final_df["timestamp"].max()
                span_rows.append(
                    {
                        "station_id": plan.station_id,
                        "station_name": plan.station_name,
                        "stn_type": plan.stn_type,
                        "granularity": granularity,
                        "year": year,
                        "row_count": int(len(final_df)),
                        "observed_start": observed_start.isoformat() if pd.notna(observed_start) else "",
                        "observed_end": observed_end.isoformat() if pd.notna(observed_end) else "",
                        "planned_start": year_start.isoformat(),
                        "planned_end": year_end.isoformat(),
                        "output_path": str(out_path),
                        "status": status,
                    }
                )

    span_path = report_dir / "download_span_report.csv"
    pd.DataFrame(span_rows).to_csv(span_path, index=False, encoding="utf-8-sig")
    return span_path


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_root)
    report_dir = Path(args.report_dir)

    client = CodisClient(
        timeout=args.timeout,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        verbose=args.verbose,
    )
    catalog_df = client.get_station_catalog()
    reference_df = load_reference_station_list(args.reference_station_list)
    plans = build_station_plans(catalog_df, reference_df, args)

    if not plans:
        raise SystemExit("No stations selected for rebuild.")

    plan_path = write_station_plan_report(plans, report_dir)
    print(f"[INFO] wrote {plan_path}")

    if args.write_plan_only:
        print("[INFO] write-plan-only enabled; skipping observation downloads.")
        return

    span_path = rebuild_station_data(client, plans, output_root, report_dir, args)
    print(f"[INFO] wrote {span_path}")


if __name__ == "__main__":
    main()
# %%
