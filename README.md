# Taiwan Historical Weather Database

Taiwan historical weather observations collected from official station datasets.

This repository now contains both:
- a raw CODIS rebuild generated from the official CWA StationData system
- a legacy-compatible dataset that preserves the old CSV interface for downstream users

## Which folder should I use?

Use one of the following folders depending on your purpose:

- `data`
  - The previous online dataset kept for reference and backward comparison.
- `data_codis_rebuild_full`
  - The new raw rebuild from official CODIS data.
  - Best if you want the most official/raw representation.
  - Column names are not legacy-compatible.
- `data_codis_legacy_compatible`
  - The recommended folder for most users.
  - Built from the new CODIS rebuild, but old columns are preserved at the front with the same names/order as before.
  - Extra official CODIS columns are appended after the legacy columns.

If you have existing code that reads the old `data` folder, you should migrate to `data_codis_legacy_compatible` first.

## Data sources

The rebuilt dataset is based on official CWA CODIS StationData downloads:
- CODIS StationData: <https://codis.cwa.gov.tw/StationData>
- CODIS station list API: <https://codis.cwa.gov.tw/api/station_list>

Station metadata reference used during rebuild:
- <https://github.com/Raingel/weather_station_list>

Historically, this repository also included data collected from earlier CWA / agrometeorological sources. The new rebuild unifies the main data source through CODIS whenever possible.

## 2026 rebuild update

### Why the rebuild was done

Two major issues were reported in the older dataset:
- some wind direction values were questioned because of `.5` decimal values
- duplicate rows were found in some hourly files, especially around `00:00`

The new rebuild was done to:
- re-download station data from official CODIS
- reduce source inconsistency across station types
- remove duplicate timestamps in rebuilt files
- keep a legacy-compatible dataset for downstream scripts

### What changed

The repository now has a two-layer update flow:

1. `tools/rebuild_codis_database.py`
   - downloads raw station/year CSVs from official CODIS
   - output: `data_codis_rebuild_full`

2. `tools/build_legacy_compatible_dataset.py`
   - converts the raw CODIS rebuild into a legacy-compatible superset
   - output: `data_codis_legacy_compatible`

### What stayed mostly the same

For the overlapping core weather variables, the rebuilt dataset is intended to stay very close to the old dataset.

Core examples include:
- `StnPres`
- `SeaPres`
- `Tx`
- `Td`
- `RH`
- `WS`
- `WD`
- `WSGust`
- `WDGust`
- `Precp`

In spot checks and rebuild verification, these core values were mostly consistent with the old database.

Intentional differences remain in a few areas:
- duplicate timestamps were removed from rebuilt files
- some old files contained cross-year carry-over rows that were not kept in the raw CODIS rebuild
- official CODIS formatting may keep values such as wind direction decimals when that is how CODIS represents them
- some legacy-only columns that do not exist in CODIS were carried forward from the old dataset only for compatibility

## Compatibility policy

The final usable dataset in `data_codis_legacy_compatible` follows this rule:

- old columns are kept first
- old column names are kept exactly the same
- extra CODIS columns are appended after the old columns
- if an old column can be derived from CODIS raw columns, it is rebuilt from CODIS
- if an old column has no CODIS source, the old value is retained only for compatibility
- if a file exists only in the old dataset and not in the raw CODIS rebuild, the old file is copied forward so downstream users do not suddenly lose files

This means `data_codis_legacy_compatible` is a compatibility layer, not a pure raw CODIS dump.

## Quality control summary

### Raw CODIS rebuild

Key checks on `data_codis_rebuild_full`:
- rebuilt station directories: `1214`
- rebuilt raw files: `58292`
- duplicate timestamp files found: `0`
- new-only station directories compared with old dataset: `53`
- old-only station directories compared with raw rebuild: `9`

### Legacy-compatible dataset

Key checks on `data_codis_legacy_compatible`:
- output files: `62064`
- output file count exactly matches `(old dataset union raw CODIS rebuild)`
- missing output files after compatibility build: `0`
- extra unexpected output files: `0`
- old files whose leading columns exactly match legacy headers: `61053`
- legacy prefix mismatches: `0`
- raw-only files passed through as-is: `1011`
- old-only files copied forward for compatibility: `3772`

Compatibility rebuild statistics:
- cells derived from CODIS raw columns into old legacy columns: `132225`
- legacy-only cells filled from old data because CODIS had no source column: `3243514`

QC reports are stored here:
- `reports/codis_full_rebuild_notebook/post_rebuild_audit.json`
- `reports/codis_full_rebuild_notebook/legacy_compat_report.json`
- `reports/codis_full_rebuild_notebook/legacy_compat_audit.json`

## Data layout

All datasets are organized as:

- first folder level: station ID
- file name: station-year CSV

Examples:
- `data_codis_legacy_compatible/466920/466920_1996.csv`
- `data_codis_legacy_compatible/466920/466920_1996_daily.csv`
- `data_codis_legacy_compatible/466920/466920_1996_monthly.csv`

Naming rules:
- hourly: `{station_id}_{year}.csv`
- daily: `{station_id}_{year}_daily.csv`
- monthly: `{station_id}_{year}_monthly.csv`

## Column guide

### General notes

There are two kinds of columns in the final compatibility dataset:

1. Legacy columns
- these are the old columns used by downstream scripts
- they always appear first
- names are preserved exactly

2. Extra CODIS columns
- these are official/raw CODIS fields kept after the legacy columns
- examples: `Precipitation.HourlyMaximum`, `RelativeHumidity.Maximum`, `WindDirection.CountForCode00`, `WindSpeed.TotalForCode00`
- these fields are useful for advanced users but are not required by older scripts

### Time column

In the old dataset, the first column header was often blank.
In the compatibility dataset this behavior is preserved where the old file existed.

So for legacy-compatible files:
- the first column may appear as an empty header
- that first column is the timestamp/date/month key

For raw-only new files without an old counterpart:
- the first column is `timestamp`

### Hourly legacy columns

Common hourly legacy columns:

- first column: timestamp (`YYYY-MM-DD HH:MM:SS`)
- `StnPres`: station pressure
- `SeaPres`: sea level pressure
- `Tx`: air temperature
- `Td`: dew point temperature
- `RH`: relative humidity
- `WS`: mean wind speed
- `WD`: mean wind direction
- `WSGust`: peak gust speed
- `WDGust`: peak gust direction
- `Precp`: precipitation accumulation
- `PrecpHour`: precipitation duration / precipitation hour summary when available
- `SunShine`: sunshine duration
- `GloblRad`: global solar radiation accumulation
- `EvapA`: Class A pan evaporation if available in the old dataset
- `Visb`: visibility if available
- `UVI`: UV index accumulation if available
- `CloudAmount`: cloud amount if available
- `TxSoil0cm`, `TxSoil5cm`, `TxSoil10cm`, `TxSoil20cm`, `TxSoil30cm`, `TxSoil50cm`, `TxSoil100cm`, `TxSoil200cm`: soil temperatures at each depth when available
- `H_VMC010` to `H_VMC120`: legacy hourly station-specific VMC fields in some files

### Daily legacy columns

Common daily legacy columns:

- first column: date (`YYYY-MM-DD`)
- `StnPres`, `SeaPres`: mean pressure values
- `StnPresMax`, `StnPresMaxTime`: daily maximum station pressure and its time
- `StnPresMin`, `StnPresMinTime`: daily minimum station pressure and its time
- `Tx`: mean temperature
- `TxMaxAbs`, `TxMaxAbsTime`: daily absolute maximum temperature and its time
- `TxMinAbs`, `TxMinAbsTime`: daily absolute minimum temperature and its time
- `TxRange`: daily temperature range when available
- `Td`: mean dew point
- `RH`: mean relative humidity
- `RHMin`, `RHMinTime`: minimum relative humidity and its time
- `WS`, `WD`: mean wind speed and prevailing wind direction
- `WSGust`, `WDGust`, `WGustTime`: maximum gust, its direction, and time
- `Precp`: daily precipitation accumulation
- `PrecpMax10`, `PrecpMax10Time`: 10-minute precipitation maximum and time
- `PrecpHrMax`, `PrecpHrMaxTime`: hourly precipitation maximum and time
- `PrecpHour`: precipitation duration when available
- `SunShine`: sunshine duration
- `GloblRad`: global solar radiation
- `EvapA`: Class A pan evaporation when available
- `VisbMean`: mean visibility when available
- `VisbAutoMean`: auto visibility mean when available
- `UVIMax`, `UVIMaxTime`: maximum UV index and time when available
- `CloudAmount`, `CloudAmountSat`: cloud amount summaries when available
- `TxSoil0cm`, `TxSoil5cm`, `TxSoil10cm`, `TxSoil20cm`, `TxSoil30cm`, `TxSoil50cm`, `TxSoil100cm`: mean soil temperatures
- `D_VMC010` to `D_VMC120`: legacy daily station-specific VMC fields in some files

### Monthly legacy columns

Common monthly legacy columns:

- first column: month key (`YYYY-MM-01` in the compatibility rebuild)
- `StnPres`, `SeaPres`: mean pressure values
- `StnPresMax`, `StnPresMaxTime`: monthly maximum station pressure and its date/time marker
- `StnPresMin`, `StnPresMinTime`: monthly minimum station pressure and its date/time marker
- `Tx`: mean air temperature
- `TxMaxAbs`, `TxMaxAbsTime`: absolute maximum temperature and date/time marker
- `TxMinAbs`, `TxMinAbsTime`: absolute minimum temperature and date/time marker
- `TxRange`: temperature range when available
- `Td`: mean dew point temperature
- `RH`: mean relative humidity
- `RHMin`, `RHMinTime`: minimum relative humidity and date/time marker
- `WS`, `WD`: mean wind speed and prevailing wind direction
- `WSGust`, `WDGust`, `WGustTime`: peak gust summary
- `Precp`: monthly precipitation accumulation
- `PrecpDay`: number of precipitation days
- `PrecpHour`: precipitation duration
- `PrecpMax10`, `PrecpMax10Time`: 10-minute precipitation maximum and time marker
- `PrecpMax60`, `PrecpMax60Time`: 60-minute precipitation maximum and time marker
- `PrecpHrMax`, `PrecpHrMaxTime`: hourly precipitation maximum and time marker
- `Precp1DayMax`, `Precp1DayMaxTime`: 1-day precipitation maximum and date marker
- `SunShine`: sunshine duration
- `SunShineRate`: sunshine rate when available
- `GloblRad`: global solar radiation
- `EvapA`: Class A pan evaporation when available
- `VisbMean`, `VisbAutoMean`: visibility summaries when available
- `UVIMax`, `UVIMaxTime`: UV index summaries when available
- `CloudAmount`, `CloudAmountSat`: cloud amount summaries when available
- `VaporPressure`: legacy vapor pressure field present in some old files
- `TxSoil0cm`, `TxSoil5cm`, `TxSoil10cm`, `TxSoil20cm`, `TxSoil30cm`, `TxSoil50cm`, `TxSoil100cm`, `TxSoil200cm`, `TxSoil300cm`, `TxSoil500cm`: monthly soil temperature summaries

## Important interpretation notes

- The compatibility dataset is designed for maximum backward compatibility, not for minimum column count.
- If your old script uses a known old column, it should continue to work with `data_codis_legacy_compatible`.
- Extra CODIS columns are appended and can be ignored by old workflows.
- Raw CODIS files may represent some values differently from the historical dataset, including missing-value style and some timing markers.
- The raw CODIS rebuild removed duplicate timestamps found in the old database.

## Tools in this repository

- `tools/rebuild_codis_database.py`
  - rebuild raw data from official CODIS
- `tools/build_legacy_compatible_dataset.py`
  - convert raw CODIS rebuild into the final legacy-compatible dataset
- `tools/data_quality/duplicate_hourly_report.py`
  - check hourly duplicate timestamps and duplicate content

Run the duplicate-hourly QC tool with:

```bash
python tools/data_quality/duplicate_hourly_report.py
```

## Station list

A station metadata list can be found here:
- <https://github.com/Raingel/weather_station_list>

## Web interface

If you are not familiar with GitHub, you can also use the web interface:
- <https://mycolab.pp.nchu.edu.tw/historical_weather/>

## Citation

Please cite as:

Ou, J.-H., Kuo, C.-H., Wu, Y.-F., Lin, G.-C., Lee, M.-H., Chen, R.-K., Chou, H.-P., Wu, H.-Y., Chu, S.-C., Lai, Q.-J., Tsai, Y.-C., Lin, C.-C., Kuo, C.-C., Liao, C.-T., Chen, Y.-N., Chu, Y.-W., Chen, C.-Y., 2023. Application-oriented deep learning model for early warning of rice blast in Taiwan. Ecological Informatics 73, 101950. <https://doi.org/10.1016/j.ecoinf.2022.101950>
