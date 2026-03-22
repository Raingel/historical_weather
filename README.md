# Taiwan Historical Weather Database / 台灣歷史氣象資料庫

Taiwan historical weather observations rebuilt from official CWA/CODIS station data, with a legacy-compatible CSV interface for downstream users.

本資料庫收錄台灣歷史氣象測站資料，並已用中央氣象署 CWA CODIS 官方資料重新建庫，同時保留舊版 CSV 介面，讓既有下游程式可以持續使用。

## Project Status / 專案目前狀態

This repository now provides three related dataset layers:

本 repository 目前提供三層資料：

- `data`
  - Production-ready dataset for downstream users.
  - Legacy-compatible and intended to replace the historical online dataset.
  - 正式提供給下游程式使用的資料夾。
  - 已做舊版欄位相容，設計上就是要取代原本線上使用的資料。
- `data_codis_legacy_compatible`
  - Explicit legacy-compatible rebuild derived from the new CODIS raw dataset.
  - Old columns are preserved first, and extra CODIS columns are appended after them.
  - 由新版 CODIS 原始資料轉出的相容版資料。
  - 舊欄位固定排在前面，新的官方欄位接在後面。
- `data_codis_rebuild_full`
  - Raw rebuild from official CODIS downloads.
  - Best for users who want the most direct official representation.
  - 官方 CODIS 原始重建資料。
  - 適合需要最貼近官方欄位與原始結構的使用者。

For most users, `data` is the correct folder to use.

對大多數使用者來說，直接使用 `data` 就是正確選擇。

## Data Sources / 資料來源

Official primary source / 主要官方來源：

- CODIS StationData: <https://codis.cwa.gov.tw/StationData>
- CODIS station list API: <https://codis.cwa.gov.tw/api/station_list>

Station metadata reference / 測站中繼資料參考：

- <https://github.com/Raingel/weather_station_list>

The rebuilt database now prefers a unified CODIS-based pipeline whenever possible.

新版資料庫已盡量統一改用 CODIS 作為主要下載來源，以減少不同來源混用造成的差異。

## 2026 Rebuild Update / 2026 年重建更新

### Why the rebuild was done / 為什麼要重建

Two issues were reported in the older database:

舊版資料庫先前有兩類問題被回報：

- Some wind direction values were questioned because decimal values such as `.5` appeared.
- Some hourly files contained duplicate timestamps, especially around `00:00`.
- 有些風向資料出現 `.5` 的數值，使用者質疑與原始資料不符。
- 有些時資料出現重複 timestamp，尤其集中在 `00:00` 左右。

The rebuild therefore aimed to:

所以這次重建的目標是：

- re-download observations from official CODIS
- unify the main upstream source
- remove duplicate timestamps in rebuilt files
- preserve backward compatibility for existing downstream code
- 重新自官方 CODIS 下載資料
- 盡量統一主要上游來源
- 去除重建資料中的重複 timestamp
- 保留對既有下游程式的相容性

### What changed / 這次更新換了什麼

The new pipeline now works in two stages:

新的建庫流程分成兩段：

1. `tools/rebuild_codis_database.py`
   - downloads and rebuilds raw CODIS station/year files
   - 下載並重建 CODIS 原始測站逐年資料
2. `tools/build_legacy_compatible_dataset.py`
   - converts raw CODIS output into a legacy-compatible superset
   - 將原始 CODIS 重建結果轉成舊版相容的 superset 資料

A daily updater was also added. It refreshes a rolling 60-day window so the workflow stays fast even near year end:

另外也新增了每日更新流程。它只會更新最近 60 天，避免年底時 workflow 因為重跑整年而變得太慢：

- `tools/run_daily_codis_update.py`
- `.github/workflows/codis_daily_update.yml`

## Compatibility Policy / 相容性策略

The final user-facing dataset follows this rule:

最後對外提供的相容資料遵循以下原則：

- old columns always stay first
- old column names stay exactly the same
- extra CODIS columns are appended after old columns
- if an old field can be derived from CODIS, it is rebuilt from CODIS
- if an old-only field has no CODIS source, the old value is retained only for compatibility
- if a file exists only in the old dataset, it is copied forward so downstream users do not suddenly lose files
- 舊欄位一定排在前面
- 舊欄位名稱完全不改
- 新的 CODIS 欄位接在舊欄位後面
- 舊欄位若能由 CODIS 推回，就用 CODIS 重建
- 舊欄位若沒有 CODIS 對應來源，才以舊值補回相容層
- 若某檔只存在舊資料庫，會保留下來，避免下游程式突然找不到檔案

This means `data` and `data_codis_legacy_compatible` are compatibility layers built from the new CODIS rebuild, not plain raw dumps.

所以 `data` 與 `data_codis_legacy_compatible` 都是建構在新版 CODIS 重建資料上的相容層，不是單純的 raw dump。

## Quality Control Summary / 品質檢查摘要

### Raw CODIS rebuild / 原始 CODIS 重建

Key checks on `data_codis_rebuild_full`:

- station directories / 測站資料夾：`1214`
- raw files / 原始檔案數：`58292`
- files with duplicate timestamps / 有重複 timestamp 的檔案：`0`
- new-only station directories vs old dataset / 相較舊庫新增站數：`53`
- old-only station directories vs raw rebuild / 舊庫獨有站數：`9`

### Legacy-compatible output / 相容版輸出

Key checks on `data_codis_legacy_compatible`:

- output files / 輸出檔數：`62064`
- missing output files / 缺漏輸出檔：`0`
- legacy header prefix mismatches / 舊欄位前綴不相符檔案：`0`
- raw-only passthrough files / 新版獨有直接保留檔：`1011`
- old-only copied files / 舊版獨有保留檔：`3772`

Compatibility rebuild statistics / 相容層統計：

- legacy cells rebuilt from CODIS / 由 CODIS 推回舊欄位的儲存格：`132225`
- legacy-only cells copied from old data / 因 CODIS 無來源而沿用舊值的儲存格：`3243514`

QC report files / QC 報告位置：

- `reports/codis_full_rebuild_notebook/post_rebuild_audit.json`
- `reports/codis_full_rebuild_notebook/legacy_compat_report.json`
- `reports/codis_full_rebuild_notebook/legacy_compat_audit.json`

## Data Layout / 資料結構

All datasets use the same layout:

所有資料夾都使用相同結構：

- first folder level = station ID / 第一層資料夾 = 站號
- file name = station-year CSV / 檔名 = 站號加年份 CSV

Examples / 例子：

- `data/466920/466920_1996.csv`
- `data/466920/466920_1996_daily.csv`
- `data/466920/466920_1996_monthly.csv`

Naming rules / 命名規則：

- hourly: `{station_id}_{year}.csv`
- daily: `{station_id}_{year}_daily.csv`
- monthly: `{station_id}_{year}_monthly.csv`

## Column Guide / 欄位說明

### General notes / 一般說明

There are two kinds of columns in the compatibility datasets:

相容資料裡的欄位可以分成兩類：

1. Legacy columns / 舊版相容欄位
- These are the columns used by downstream scripts.
- They always appear first.
- 這些是舊下游程式會直接使用的欄位。
- 它們一定排在最前面。

2. Extra CODIS columns / 額外 CODIS 官方欄位
- These are official/raw CODIS fields appended after the legacy columns.
- Older scripts can ignore them.
- 這些是附加在舊欄位後面的 CODIS 官方欄位。
- 舊程式通常可以直接忽略。

### Time column / 時間欄位

- In old files, the first column header was often blank.
- In compatibility output, that legacy interface is preserved when an old counterpart exists.
- 舊檔第一欄欄名常常是空白。
- 在相容版裡，如果有舊檔對應，這種介面會盡量保留。

### Common hourly legacy columns / 常見時資料舊欄位

- `StnPres`: station pressure / 測站氣壓
- `SeaPres`: sea level pressure / 海平面氣壓
- `Tx`: air temperature / 氣溫
- `Td`: dew point temperature / 露點溫度
- `RH`: relative humidity / 相對濕度
- `WS`: mean wind speed / 平均風速
- `WD`: mean wind direction / 平均風向
- `WSGust`: maximum gust speed / 最大陣風風速
- `WDGust`: gust direction / 最大陣風風向
- `Precp`: precipitation accumulation / 累積降水量
- `PrecpHour`: precipitation duration or hourly summary / 降水時數或相關摘要
- `SunShine`: sunshine duration / 日照時數
- `GloblRad`: global radiation / 全天空日射量
- `EvapA`: Class A pan evaporation / A 盆蒸發量
- `Visb`: visibility / 能見度
- `UVI`: UV index / 紫外線指數
- `CloudAmount`: cloud amount / 雲量
- `TxSoil0cm` to `TxSoil200cm`: soil temperature at different depths / 不同深度土壤溫度

### Common daily legacy columns / 常見日資料舊欄位

- `StnPres`, `SeaPres`: mean pressure / 平均氣壓
- `StnPresMax`, `StnPresMaxTime`: daily maximum station pressure and time / 日最大測站氣壓及時間
- `StnPresMin`, `StnPresMinTime`: daily minimum station pressure and time / 日最小測站氣壓及時間
- `Tx`: mean temperature / 平均氣溫
- `TxMaxAbs`, `TxMaxAbsTime`: absolute maximum temperature and time / 絕對最高溫及時間
- `TxMinAbs`, `TxMinAbsTime`: absolute minimum temperature and time / 絕對最低溫及時間
- `TxRange`: temperature range / 溫度日較差
- `Td`: mean dew point / 平均露點溫度
- `RH`: mean relative humidity / 平均相對濕度
- `RHMin`, `RHMinTime`: minimum relative humidity and time / 最低相對濕度及時間
- `WS`, `WD`: mean wind speed and direction / 平均風速與風向
- `WSGust`, `WDGust`, `WGustTime`: gust summary / 最大陣風摘要
- `Precp`: daily precipitation / 日累積雨量
- `PrecpMax10`, `PrecpMax10Time`: 10-minute precipitation maximum / 10 分鐘最大降雨量
- `PrecpHrMax`, `PrecpHrMaxTime`: hourly precipitation maximum / 1 小時最大降雨量
- `SunShine`: sunshine duration / 日照時數
- `GloblRad`: global radiation / 日射量
- `EvapA`: Class A pan evaporation / A 盆蒸發量
- `VisbMean`, `VisbAutoMean`: visibility summary / 能見度摘要
- `UVIMax`, `UVIMaxTime`: UV maximum and time / 紫外線指數最大值及時間
- `CloudAmount`, `CloudAmountSat`: cloud amount summary / 雲量摘要

### Common monthly legacy columns / 常見月資料舊欄位

- `StnPres`, `SeaPres`: mean pressure / 平均氣壓
- `Tx`: mean air temperature / 平均氣溫
- `TxMaxAbs`, `TxMaxAbsTime`: absolute monthly maximum temperature / 月絕對最高溫
- `TxMinAbs`, `TxMinAbsTime`: absolute monthly minimum temperature / 月絕對最低溫
- `RH`: mean relative humidity / 平均相對濕度
- `WS`, `WD`: wind summary / 風速風向摘要
- `WSGust`, `WDGust`, `WGustTime`: gust summary / 陣風摘要
- `Precp`: monthly precipitation / 月累積雨量
- `PrecpDay`: precipitation days / 降雨日數
- `PrecpHour`: precipitation duration / 降水時數
- `PrecpMax10`, `PrecpMax60`, `PrecpHrMax`, `Precp1DayMax`: precipitation extremes / 降水極值摘要
- `SunShine`, `SunShineRate`: sunshine summary / 日照摘要
- `GloblRad`: radiation summary / 日射摘要
- `EvapA`: Class A pan evaporation / A 盆蒸發量
- `VisbMean`, `VisbAutoMean`: visibility summary / 能見度摘要
- `UVIMax`, `UVIMaxTime`: UV summary / 紫外線摘要
- `CloudAmount`, `CloudAmountSat`: cloud summary / 雲量摘要
- `VaporPressure`: vapor pressure / 水氣壓
- `TxSoil0cm` to `TxSoil500cm`: monthly soil temperature summaries / 月土壤溫度摘要

## Daily Update Automation / 每日自動更新

The repository includes a GitHub Actions workflow for daily updates:

本 repository 已內建 GitHub Actions 每日自動更新流程：

- workflow: `.github/workflows/codis_daily_update.yml`
- updater: `tools/run_daily_codis_update.py`

The update flow is:

更新流程如下：

1. refresh the most recent 60 days of raw CODIS data into `data_codis_rebuild_full`
2. merge the refreshed window back into full-year raw files
3. rebuild matching compatibility files into `data_codis_legacy_compatible`
4. sync compatible files back into `data`
5. commit and push the updated results
6. 將最近 60 天的 raw CODIS 資料更新到 `data_codis_rebuild_full`
7. 再把這段更新 merge 回完整年檔
8. 重新產生對應的 `data_codis_legacy_compatible`
9. 把相容版同步回 `data`
10. commit 並 push 結果

A duplicate check workflow is also kept:

另外也保留每週重複資料檢查：

- `.github/workflows/hourly-duplicate-report.yml`

## Tools / 工具

- `tools/rebuild_codis_database.py`
  - rebuild raw CODIS data / 重建原始 CODIS 資料
- `tools/build_legacy_compatible_dataset.py`
  - build legacy-compatible output / 產生舊版相容資料
- `tools/run_daily_codis_update.py`
  - run the daily raw + compatibility + sync pipeline / 執行每日更新流程
- `tools/build_push_plan.ps1`
  - build chunked rollout plans / 產生分批推送計畫
- `tools/apply_push_batch.ps1`
  - apply one rollout batch with logs / 執行單一分批推送並保留 log
- `tools/data_quality/duplicate_hourly_report.py`
  - check duplicate hourly timestamps / 檢查時資料重複 timestamp

## Web Interface / 網頁介面

If you do not want to browse the repository directly, a web interface is available:

若不想直接瀏覽 GitHub repository，也可以使用網頁介面：

- <https://mycolab.pp.nchu.edu.tw/historical_weather/>

## Citation / 引用

Please cite / 建議引用：

Ou, J.-H., Kuo, C.-H., Wu, Y.-F., Lin, G.-C., Lee, M.-H., Chen, R.-K., Chou, H.-P., Wu, H.-Y., Chu, S.-C., Lai, Q.-J., Tsai, Y.-C., Lin, C.-C., Kuo, C.-C., Liao, C.-T., Chen, Y.-N., Chu, Y.-W., Chen, C.-Y., 2023. Application-oriented deep learning model for early warning of rice blast in Taiwan. Ecological Informatics 73, 101950. <https://doi.org/10.1016/j.ecoinf.2022.101950>
