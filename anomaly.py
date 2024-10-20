# %%
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from calendar import monthrange

# 載入站點資料
sta_URI = "https://raw.githubusercontent.com/Raingel/weather_station_list/refs/heads/main/data/weather_sta_list.csv"
stations_df = pd.read_csv(sta_URI)
stations_df.reset_index(inplace=True, drop=True)
stations_df = stations_df[['站號', '站名', '緯度', '經度']]
stations_df.columns = ['stationID', 'stationName', 'latitude', 'longitude']

# %%
# 定義變數
variables = ['Tx', 'RH', 'Precp']  # 溫度、相對溼度、降水量

# 定義氣候基準期間
climatology_start_year = 2000
climatology_end_year = 2020  # 包含2020年

# 獲取當前日期
today = datetime.now()

# 新增 rebuild 選項
rebuild = False  # 如果為 True，則重建從 2010 年至今的所有月份的距平資料

# 根據 rebuild 選項確定要計算的月份列表
if rebuild:
    # 重建從 2010 年 1 月到當前日期的所有月份
    start_year = 2010
    start_month = 1
    end_year = today.year
    end_month = today.month
    target_months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                break
            target_months.append((year, month))
else:
    # 根據當前日期確定要計算的月份列表
    if today.day >= 1 and today.day <= 5:
        # 如果今天是1~5號，就計算這個月及前個月
        target_months = [
            (today.year, today.month),
            ((today - timedelta(days=1)).year, (today - timedelta(days=1)).month)
        ]
    else:
        # 否則只計算這個月
        target_months = [(today.year, today.month)]

# 預先計算並儲存所有站點的氣候基準資料
print("正在預處理氣候基準資料...")
station_climatology = {}

for idx, row in stations_df.iterrows():
    sta_id = str(row['stationID']).strip()
    data_dir = f"./data/{sta_id}/"
    climatology_data_list = []

    for year in range(climatology_start_year, climatology_end_year + 1):
        file_path = os.path.join(data_dir, f"{sta_id}_{year}_daily.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            if not set(variables).issubset(df.columns):
                continue
            # 將小於 -9 的值替換為 NaN
            df[variables] = df[variables].where(df[variables] > -9, np.nan)
            df['md'] = df.index.strftime('%m-%d')
            climatology_data_list.append(df)
    if climatology_data_list:
        climatology_data = pd.concat(climatology_data_list)
        station_climatology[sta_id] = climatology_data
    else:
        # 如果沒有氣候基準資料，記錄為 None
        station_climatology[sta_id] = None

print("氣候基準資料預處理完成。")

# %%
for target_year, target_month in target_months:
    print(f"正在計算 {target_year} 年 {target_month} 月的距平值")
    # 判斷是過去的月份還是當前月份
    if datetime(target_year, target_month, 1) < datetime(today.year, today.month, 1):
        # 過去的月份，使用整個月的資料
        period_start = datetime(target_year, target_month, 1)
        # 獲取該月的最後一天
        last_day = monthrange(target_year, target_month)[1]
        period_end = datetime(target_year, target_month, last_day)
    else:
        # 當前月份，使用到今天的資料
        period_start = datetime(target_year, target_month, 1)
        period_end = today

    # 生成期間的月-日列表
    current_md_list = [(period_start + timedelta(days=i)).strftime('%m-%d') for i in range((period_end - period_start).days + 1)]
    results = []
    print("正在計算距平值...")
    for idx, row in stations_df.iterrows():
        sta_id = str(row['stationID']).strip()
        station_name = row['stationName']
        latitude = row['latitude']
        longitude = row['longitude']
        data_dir = f"./data/{sta_id}/"

        # 獲取該站點的氣候基準資料
        climatology_data = station_climatology.get(sta_id)
        if climatology_data is None:
            print(f"沒有 {sta_id} 的氣候基準資料")
            continue

        # 篩選出當前目標月份的氣候基準資料
        climatology_period_data = climatology_data[climatology_data['md'].isin(current_md_list)]
        if climatology_period_data.empty:
            print(f"站點 {sta_id} 在氣候基準期間內沒有資料")
            continue

        climatology_means = climatology_period_data[variables].mean()

        # 收集當前年份的資料
        file_path = os.path.join(data_dir, f"{sta_id}_{target_year}_daily.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            if not set(variables).issubset(df.columns):
                print(f"站點 {sta_id} 當前資料缺少必要變數")
                continue
            df['md'] = df.index.strftime('%m-%d')
            df_period = df[(df.index >= period_start) & (df.index <= period_end)]
            if df_period.empty:
                print(f"站點 {sta_id} 在期間內沒有當前資料")
                continue
            # 將小於 -9 的值替換為 NaN
            df_period[variables] = df_period[variables].where(df_period[variables] > -9, np.nan)
            current_data = df_period
        else:
            print(f"站點 {sta_id} 在 {target_year} 沒有當前資料檔案")
            continue

        current_means = current_data[variables].mean()

        if current_means.isnull().any() or climatology_means.isnull().any():
            print(f"站點 {sta_id} 資料缺失，跳過")
            continue

        anomalies = current_means - climatology_means

        result = {
            '站名': station_name,
            '緯度': latitude,
            '經度': longitude,
            '平均溫距平': anomalies['Tx'],
            '本年度平均溫': current_means['Tx'],
            '平均溼度距平': anomalies['RH'],
            '本年度日均相對溼度': current_means['RH'],
            '本年度日均降水量': current_means['Precp'],
            '日均降水量距平': anomalies['Precp'],
            '站號': sta_id
        }

        results.append(result)

    results_df = pd.DataFrame(results)

    if not results_df.empty:
        # 將數值四捨五入到小數點後兩位
        round_columns = ['平均溫距平', '本年度平均溫', '平均溼度距平', '本年度日均相對溼度', '本年度日均降水量', '日均降水量距平']
        results_df[round_columns] = results_df[round_columns].round(2)

        # 如果需要，也可以將緯度和經度四捨五入到指定的小數位，例如6位
        results_df['緯度'] = results_df['緯度'].round(6)
        results_df['經度'] = results_df['經度'].round(6)

        # 保存為 CSV 和 JSON
        os.makedirs('./anomaly/result/', exist_ok=True)
        csv_filename = f'./anomaly/result/{target_year}_{target_month:02d}.csv'
        json_filename = f'./anomaly/result/{target_year}_{target_month:02d}.json'

        results_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        results_df.to_json(json_filename, orient='records', force_ascii=False)

        print(f"結果已保存到 {csv_filename} 和 {json_filename}")
    else:
        print(f"在 {target_year} 年 {target_month} 月沒有可用的資料。")

# %%

