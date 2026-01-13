"""Refetch missing daily/hourly data for station-years with monthly data."""

from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import datetime
import io
import os
import re
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests
from dateutil.parser import parse
from requests import get, post


class NAGR:
    def __init__(self) -> None:
        self.my_headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest",
        }

    @staticmethod
    def dict_to_get(params: Dict[str, str]) -> str:
        return "&".join(["{}={}".format(k, v) for k, v in params.items()])

    def agr_get_items(self, station: str = "466910", data_type: str = "hourly") -> OrderedDict:
        if data_type == "daily":
            get_items_uri = "https://agr.cwa.gov.tw/NAGR/history/station_day/get_items"
        elif data_type == "hourly":
            get_items_uri = "https://agr.cwa.gov.tw/NAGR/history/station_hour/get_items"
        else:
            raise ValueError(f"Unsupported data_type: {data_type}")
        response = post(get_items_uri, data={"station": station}, headers=self.my_headers)
        payload = response.json()
        items = {entry["item"]: entry["cname"] for entry in payload.get("items", [])}
        if not items:
            return OrderedDict()
        ordered = OrderedDict()
        for column in payload.get("columns", []):
            ordered[items[column]] = column
        return ordered

    @staticmethod
    def replace_list_by_dict(columns: Iterable[str], mapping: Dict[str, str]) -> List[str]:
        mapping = dict(mapping)
        mapping["觀測時間"] = "date"
        return [mapping.get(column, column) for column in columns]

    @staticmethod
    def add_2359(value: pd.Timestamp) -> pd.Timestamp:
        if value.strftime("%H%M") == "2359":
            return value + pd.Timedelta(minutes=1)
        return value

    def get_data_by_csv_api(
        self,
        station_id: str,
        start_time: str,
        end_time: str,
        data_type: str,
        save_path: str,
    ) -> pd.DataFrame:
        if data_type == "hourly":
            uri = "https://agr.cwa.gov.tw/NAGR/history/station_hour/create_report"
        elif data_type == "daily":
            uri = "https://agr.cwa.gov.tw/NAGR/history/station_day/create_report"
        else:
            raise ValueError(f"Unsupported data_type: {data_type}")
        items = self.agr_get_items(station_id, data_type)
        data = {
            "station": station_id,
            "start_time": parse(start_time).strftime("%Y-%m-%d"),
            "end_time": parse(end_time).strftime("%Y-%m-%d"),
            "items": ",".join(items.values()),
            "report_type": "csv_time",
            "level": "自動站",
        }
        if station_id[0:2] not in ["46", "C0", "C1"]:
            data["level"] = ""
        try:
            response = get(uri + "?" + self.dict_to_get(data))
            response.encoding = "big5"
            raw_io = io.StringIO(response.text)
            df = pd.read_csv(raw_io, encoding="big5", skiprows=[0], on_bad_lines="skip", index_col=False)
            df.columns = self.replace_list_by_dict(df.columns, items)
            df.drop(["測站代碼"], axis=1, inplace=True)
            df = df[df["date"].isna() == False]
            df["date"] = pd.to_datetime(df["date"])
            df["date"] = df["date"].apply(self.add_2359)
            df.index = df["date"].to_list()
            df.drop(["date"], axis=1, inplace=True)
        except Exception as exc:  # noqa: BLE001
            print(f"Error during parsing file for station {station_id}: {exc}")
            return pd.DataFrame()
        if save_path:
            df.to_csv(save_path)
            print(f"Saved to {save_path}")
        return df


class CODIS:
    def _stations_fetch(self) -> Tuple[str, Dict[str, str]]:
        return (
            "https://codis.cwa.gov.tw/api/station_list",
            {
                "headers": {
                    "accept": "*/*",
                    "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
                    "sec-ch-ua": '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "x-requested-with": "XMLHttpRequest",
                },
                "referrer": "https://codis.cwa.gov.tw/StationData",
                "referrerPolicy": "strict-origin-when-cross-origin",
                "body": "",
                "method": "GET",
                "mode": "cors",
                "credentials": "include",
            },
        )

    def _daily_fetch(
        self,
        sta_id: str = "467490",
        stn_type: str = "cwb",
        start: datetime = datetime(2022, 8, 16, 0, 0, 0),
        end: datetime = datetime(2022, 9, 13, 0, 0, 0),
    ) -> Tuple[str, Dict[str, str], Dict[str, str]]:
        return (
            "https://codis.cwa.gov.tw/api/station?",
            {
                "headers": {
                    "accept": "application/json, text/javascript, */*; q=0.01",
                    "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
                    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "sec-ch-ua": '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "x-requested-with": "XMLHttpRequest",
                },
                "referrer": "https://codis.cwa.gov.tw/StationData",
                "referrerPolicy": "strict-origin-when-cross-origin",
                "body": "",
                "method": "POST",
                "mode": "cors",
                "credentials": "include",
            },
            {
                "date": "2022-08-16T00%3A00%3A00.000%2B08%3A00",
                "type": "report_month",
                "stn_ID": sta_id,
                "stn_type": stn_type,
                "start": start.strftime("%Y-%m-%dT00:00:00"),
                "end": end.strftime("%Y-%m-%dT00:00:00"),
            },
        )

    def _hourly_fetch(
        self,
        sta_id: str = "467490",
        stn_type: str = "cwb",
        start: datetime = datetime(2022, 8, 16, 0, 0, 0),
        end: datetime = datetime(2022, 9, 13, 0, 0, 0),
    ) -> Tuple[str, Dict[str, str], Dict[str, str]]:
        return (
            "https://codis.cwa.gov.tw/api/station?",
            {
                "headers": {
                    "accept": "application/json, text/javascript, */*; q=0.01",
                    "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
                    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "sec-ch-ua": '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "x-requested-with": "XMLHttpRequest",
                },
                "referrer": "https://codis.cwa.gov.tw/StationData",
                "referrerPolicy": "strict-origin-when-cross-origin",
                "body": "",
                "method": "POST",
                "mode": "cors",
                "credentials": "include",
            },
            {
                "date": "2022-08-16T00%3A00%3A00.000%2B08%3A00",
                "type": "report_month",
                "stn_ID": sta_id,
                "stn_type": stn_type,
                "start": start.strftime("%Y-%m-%dT00:00:00"),
                "end": end.strftime("%Y-%m-%dT00:00:00"),
            },
        )

    @staticmethod
    def fetcher(url: str, params: Dict[str, str], data: Dict[str, str] | str = "") -> Dict:
        if params["method"] == "GET":
            return requests.get(url, params=params, data=data).json()
        if params["method"] == "POST":
            return requests.post(url, params=params, data=data).json()
        raise ValueError(f"Unsupported method: {params['method']}")

    def get_stations_df(self) -> pd.DataFrame:
        stations_raw = self.fetcher(*self._stations_fetch())
        stations_df = pd.DataFrame()
        for station in stations_raw.get("data", []):
            df_temp = pd.DataFrame(station["item"])
            df_temp["stn_type"] = station["stationAttribute"]
            stations_df = pd.concat([stations_df, df_temp], axis=0)
        return stations_df

    @staticmethod
    def daily_json_parser(wea_data: Dict) -> pd.DataFrame:
        output_df = pd.DataFrame()
        for entry in wea_data["data"][0]["dts"]:
            try:
                output_df.loc[entry["DataDate"], "StnPres"] = entry["StationPressure"]["Mean"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "SeaPres"] = entry["SeaLevelPressure"]["Mean"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "StnPresMax"] = entry["StationPressure"]["Maximum"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "StnPresMaxTime"] = entry["StationPressure"]["MaximumTime"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "StnPresMin"] = entry["StationPressure"]["Minimum"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "StnPresMinTime"] = entry["StationPressure"]["MinimumTime"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "Tx"] = entry["AirTemperature"]["Mean"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "TxMaxAbs"] = entry["AirTemperature"]["Maximum"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "TxMaxAbsTime"] = entry["AirTemperature"]["MaximumTime"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "TxMinAbs"] = entry["AirTemperature"]["Minimum"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "TxMinAbsTime"] = entry["AirTemperature"]["MinimumTime"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "Td"] = entry["DewPointTemperature"]["Mean"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "RH"] = entry["RelativeHumidity"]["Mean"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "WS"] = entry["WindSpeed"]["Mean"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "WD"] = entry["WindDirection"]["Prevailing"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "WSMax"] = entry["WindSpeed"]["Maximum"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "WSMaxTime"] = entry["WindSpeed"]["MaximumTime"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "Precp"] = entry["Precipitation"]["Accumulation"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[entry["DataDate"], "PrecpHour"] = entry["Precipitation"]["Accumulation_hour"]
            except Exception:  # noqa: BLE001
                pass
        output_df.fillna(-99.8, inplace=True)
        output_df.index = pd.to_datetime(output_df.index)
        output_df.index.name = ""
        output_df.sort_index(inplace=True)
        return output_df

    @staticmethod
    def hourly_json_parser(wea_data: Dict) -> pd.DataFrame:
        output_df = pd.DataFrame()
        for entry in wea_data["data"][0]["dts"]:
            data_time = entry["DataTime"]
            try:
                output_df.loc[data_time, "StnPres"] = entry["StationPressure"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "SeaPres"] = entry["SeaLevelPressure"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "Tx"] = entry["AirTemperature"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "Td"] = entry["DewPointTemperature"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "RH"] = entry["RelativeHumidity"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "WS"] = entry["WindSpeed"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "WD"] = entry["WindDirection"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "WSGust"] = entry["WindSpeed"]["Gust"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "WDGust"] = entry["WindDirection"]["Gust"]
            except Exception:  # noqa: BLE001
                pass
            try:
                output_df.loc[data_time, "Precp"] = entry["Precipitation"]["Instantaneous"]
            except Exception:  # noqa: BLE001
                pass
        output_df.fillna(-99.8, inplace=True)
        output_df.index = pd.to_datetime(output_df.index)
        output_df["DataTime_temp"] = output_df.index
        output_df.loc[
            output_df["DataTime_temp"].dt.strftime("%H:%M:%S") == "23:59:00", "DataTime_temp"
        ] = output_df.loc[
            output_df["DataTime_temp"].dt.strftime("%H:%M:%S") == "23:59:00", "DataTime_temp"
        ] + pd.Timedelta(minutes=1)
        output_df.index = output_df["DataTime_temp"]
        output_df.index.name = ""
        output_df.drop("DataTime_temp", axis=1, inplace=True)
        output_df.sort_index(inplace=True)
        return output_df

    def get_full_year_daily(self, sta_id: str, stn_type: str, year: int) -> pd.DataFrame:
        output_df = pd.DataFrame()
        start = datetime(year, 1, 1, 0, 0, 0)
        terminate = min(datetime(year + 1, 1, 1, 0, 0, 0), datetime.now())
        while start < terminate:
            end = start + pd.Timedelta(days=60)
            if end > datetime(year + 1, 1, 1, 0, 0, 0):
                end = datetime(year + 1, 1, 1, 0, 0, 0)
            raw_data = self.fetcher(*self._daily_fetch(sta_id=sta_id, stn_type=stn_type, start=start, end=end))
            try:
                output_df = pd.concat([output_df, self.daily_json_parser(raw_data)])
                print(f"  Success to process station: {sta_id} for {start} {end}")
            except Exception:  # noqa: BLE001
                print(f"  Failed to process station: {sta_id} for {start} {end}")
            start = end
        return output_df

    def get_full_year_hourly(self, sta_id: str, stn_type: str, year: int) -> pd.DataFrame:
        output_df = pd.DataFrame()
        start = datetime(year, 1, 1, 0, 0, 0)
        terminate = min(datetime(year + 1, 1, 1, 0, 0, 0), datetime.now())
        while start < terminate:
            end = start + pd.Timedelta(days=30)
            if end > datetime(year + 1, 1, 1, 0, 0, 0):
                end = datetime(year + 1, 1, 1, 0, 0, 0)
            raw_data = self.fetcher(*self._hourly_fetch(sta_id=sta_id, stn_type=stn_type, start=start, end=end))
            try:
                output_df = pd.concat([output_df, self.hourly_json_parser(raw_data)])
                print(f"  Success to process station: {sta_id} for {start} {end}")
            except Exception:  # noqa: BLE001
                print(f"  Failed to process station: {sta_id} for {start} {end}")
            start = end
        return output_df


def normalize_station_type(station_id: str, station_type: str) -> Optional[str]:
    if station_type == "agr":
        return "agr"
    if station_type == "cwb":
        return "cwb"
    if station_type == "auto":
        if station_id.startswith("C0"):
            return "auto_C0"
        if station_id.startswith("C1"):
            return "auto_C1"
        return "auto"
    return None


def build_station_type_map(codis: CODIS) -> Dict[str, str]:
    stations_df = codis.get_stations_df()
    stations_df = stations_df[stations_df["stationEndDate"] == ""]
    stations_df.reset_index(inplace=True, drop=True)
    station_type_map = {}
    for _, row in stations_df.iterrows():
        station_id = row["stationID"]
        stn_type = normalize_station_type(station_id, row["stn_type"])
        if stn_type:
            station_type_map[station_id] = stn_type
    return station_type_map


def find_missing_station_years(data_dir: str) -> Dict[str, Set[int]]:
    missing: Dict[str, Set[int]] = {}
    monthly_pattern = re.compile(r"^(?P<station>.+)_(?P<year>\d{4})_monthly\.csv$")
    for station_id in os.listdir(data_dir):
        station_path = os.path.join(data_dir, station_id)
        if not os.path.isdir(station_path):
            continue
        for filename in os.listdir(station_path):
            match = monthly_pattern.match(filename)
            if not match:
                continue
            year = int(match.group("year"))
            daily_path = os.path.join(station_path, f"{station_id}_{year}_daily.csv")
            hourly_path = os.path.join(station_path, f"{station_id}_{year}.csv")
            if not (os.path.exists(daily_path) and os.path.exists(hourly_path)):
                missing.setdefault(station_id, set()).add(year)
    return missing


def fetch_missing_data(
    data_dir: str,
    station_type_map: Dict[str, str],
    station_years: Dict[str, Set[int]],
) -> None:
    codis = CODIS()
    nagr = NAGR()
    for station_id, years in sorted(station_years.items()):
        station_type = station_type_map.get(station_id)
        if station_type is None:
            print(f"Skip {station_id}: unable to determine station type")
            continue
        for year in sorted(years):
            station_path = os.path.join(data_dir, station_id)
            os.makedirs(station_path, exist_ok=True)
            daily_path = os.path.join(station_path, f"{station_id}_{year}_daily.csv")
            hourly_path = os.path.join(station_path, f"{station_id}_{year}.csv")
            needs_daily = not os.path.exists(daily_path)
            needs_hourly = not os.path.exists(hourly_path)
            if not needs_daily and not needs_hourly:
                continue
            print(f"Processing station {station_id} for year {year}")
            if needs_daily:
                print(f"  Fetching daily data -> {daily_path}")
                if station_type == "agr":
                    df_daily = nagr.get_data_by_csv_api(
                        station_id=station_id,
                        start_time=f"{year}-01-01",
                        end_time=f"{year + 1}-01-01",
                        data_type="daily",
                        save_path=daily_path,
                    )
                else:
                    df_daily = codis.get_full_year_daily(sta_id=station_id, stn_type=station_type, year=year)
                    if not df_daily.empty:
                        df_daily.to_csv(daily_path)
                if df_daily.empty:
                    print(f"  Daily data fetch failed for {station_id} {year}")
            if needs_hourly:
                print(f"  Fetching hourly data -> {hourly_path}")
                if station_type == "agr":
                    df_hourly = nagr.get_data_by_csv_api(
                        station_id=station_id,
                        start_time=f"{year}-01-01",
                        end_time=f"{year + 1}-01-01",
                        data_type="hourly",
                        save_path=hourly_path,
                    )
                else:
                    df_hourly = codis.get_full_year_hourly(sta_id=station_id, stn_type=station_type, year=year)
                    if not df_hourly.empty:
                        df_hourly.to_csv(hourly_path)
                if df_hourly.empty:
                    print(f"  Hourly data fetch failed for {station_id} {year}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refetch daily/hourly data when monthly data exists but daily/hourly is missing.",
    )
    parser.add_argument("--data-dir", default="data", help="Path to data directory (default: data)")
    args = parser.parse_args()

    data_dir = args.data_dir
    if not os.path.isdir(data_dir):
        raise SystemExit(f"Data directory not found: {data_dir}")

    codis = CODIS()
    station_type_map = build_station_type_map(codis)
    missing_station_years = find_missing_station_years(data_dir)

    if not missing_station_years:
        print("No station-years found with missing daily/hourly data.")
        return

    fetch_missing_data(data_dir, station_type_map, missing_station_years)


if __name__ == "__main__":
    main()
