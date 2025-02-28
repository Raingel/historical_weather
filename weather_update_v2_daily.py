# %%
import requests
from datetime import datetime
import pandas as pd
from dateutil.parser import parse
import os
from collections import OrderedDict
from requests import get,post
import json
import io
import time  # 新增此行

class NAGR:
    def __init__(self):    
        self.my_headers = {    
                                "accept": "application/json, text/javascript, */*; q=0.01",
                                "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
                                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                                "sec-ch-ua": "\"Chromium\";v=\"92\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"92\"",
                                "sec-ch-ua-mobile": "?0",
                                "sec-fetch-dest": "empty",
                                "sec-fetch-mode": "cors",
                                "sec-fetch-site": "same-origin",
                                "x-requested-with": "XMLHttpRequest", #required
                            }
    def dictToGET(self, d):
        return '&'.join(['{}={}'.format(k,v) for k,v in d.items()])
    def agr_get_items (self, station = '466910', type = 'hourly'): #check available fields
        if type == 'daily':
            get_items_URI = "https://agr.cwa.gov.tw/NAGR/history/station_day/get_items"
        elif type == 'hourly':
            get_items_URI = "https://agr.cwa.gov.tw/NAGR/history/station_hour/get_items"
        r = post(get_items_URI, data = {'station': station},  headers= self.my_headers)
        i = json.loads(r.text)
        d = {t['item']: t['cname'] for t in i['items']}
        if d == {}:
            return {}
        orderedD = OrderedDict()
        for e in i['columns']:
            orderedD[d[e]] = e 
        return orderedD
    def replaceListByDict(self,l, d):
        d['觀測時間'] = 'date'
        return [d[i] if i in d else i for i in l]
    def add_2359(self, d):
        if d.strftime('%H%M') == '2359':
            return d + pd.Timedelta(minutes=1)
        else:
            return d
    def getDataByCsvAPI(self,STA = '466900', start_time='2020-08-16', end_time='2020-09-13', type='hourly', save_path = ''):
        #print("Downloading data from {} to {}".format(start_time, end_time))
        #STA = '466900'
        if type=='hourly':
            URI = 'https://agr.cwa.gov.tw/NAGR/history/station_hour/create_report'     
        elif type=='daily':
            URI = 'https://agr.cwa.gov.tw/NAGR/history/station_day/create_report'
        items = self.agr_get_items(STA, type)
        data = {
            'station': STA,
            'start_time': parse(start_time).strftime('%Y-%m-%d'),
            'end_time': parse(end_time).strftime('%Y-%m-%d'),
            'items': ','.join(items.values()),
            'report_type':'csv_time',
            'level': '自動站'
        }
        if STA[0:2] not in ['46','C0','C1']:
            data['level'] = ''
        try:
            r = get(URI+'?'+self.dictToGET(data))
            r.encoding='big5'
            rio = io.StringIO(r.text)
            df = pd.read_csv(rio,encoding='big5', skiprows=[0], on_bad_lines = 'skip', index_col=False)
            df.columns = self.replaceListByDict(df.columns, items)
            df.drop(['測站代碼'], axis=1, inplace=True)
            #discard NaN date, which is occasionally returned by the API
            df = df[df['date'].isna() == False]
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].apply(self.add_2359)
            df.index = df['date'].to_list()
            df.drop(['date'], axis=1, inplace=True)
        except Exception as e:
            print("Error during parsing file:", e)
            return pd.DataFrame()
        if save_path != '':
            df.to_csv(save_path)
            print ("Saved to {}".format(save_path))
        return df

class CODIS:
    def _stations_fetch(self):
        return ("https://codis.cwa.gov.tw/api/station_list", {
                "headers": {
                    "accept": "*/*",
                    "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
                    "sec-ch-ua": "\"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"108\", \"Google Chrome\";v=\"108\"",
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": "\"Windows\"",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "x-requested-with": "XMLHttpRequest"
                },
                "referrer": "https://codis.cwa.gov.tw/StationData",
                "referrerPolicy": "strict-origin-when-cross-origin",
                "body": "",
                "method": "GET",
                "mode": "cors",
                "credentials": "include"
                })
    def _daily_fetch(self, sta_id="467490", stn_type='cwb', start=datetime(2022,8,16,0,0,0), end=datetime(2022,9,13,0,0,0)):
        return ("https://codis.cwa.gov.tw/api/station?", {
        "headers": {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-ch-ua": "\"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"108\", \"Google Chrome\";v=\"108\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest"
        },
        "referrer": "https://codis.cwa.gov.tw/StationData",
        "referrerPolicy": "strict-origin-when-cross-origin",
        "body": "",
        "method": "POST",
        "mode": "cors",
        "credentials": "include"
        },
        {
            "date": "2022-08-16T00%3A00%3A00.000%2B08%3A00",
            "type": "report_month",
            "stn_ID": sta_id,
            "stn_type": stn_type,
            "start": start.strftime("%Y-%m-%dT00:00:00"),
            "end": end.strftime("%Y-%m-%dT00:00:00")
        })
    def fetcher(self, url, params, data=""):
        if params['method'] == 'GET':
            return requests.get(url, params=params, data=data).json()
        elif params['method'] == 'POST':
            return requests.post(url, params=params, data=data).json()
    def get_stations_df(self):
        stations_raw = self.fetcher(*self._stations_fetch())
        stations_df = pd.DataFrame()
        for i in range(len(stations_raw['data'])):
            df_temp = pd.DataFrame(stations_raw['data'][i]['item'])
            df_temp['stn_type'] = stations_raw['data'][i]['stationAttribute']
            stations_df = pd.concat([stations_df, df_temp], axis=0)
        return stations_df    
    def daily_json_parser(self, wea_data):
        output_df = pd.DataFrame()
        for v in wea_data['data'][0]['dts']:
            #Since different stations have different data, we need to check if the data is available
            #Super ugly code, but it works
            #print(v['DataTime'])
            try:
                output_df.loc[v['DataDate'], 'StnPres'] = v['StationPressure']['Mean']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'SeaPres'] = v['SeaLevelPressure']['Mean']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'StnPresMax'] = v['StationPressure']['Maximum']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'StnPresMaxTime'] = v['StationPressure']['MaximumTime']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'StnPresMin'] = v['StationPressure']['Minimum']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'StnPresMinTime'] = v['StationPressure']['MinimumTime']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'Tx'] = v['AirTemperature']['Mean']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxMaxAbs'] = v['AirTemperature']['Maximum']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxMaxAbsTime'] = v['AirTemperature']['MaximumTime']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxMinAbs'] = v['AirTemperature']['Minimum']
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxMinAbsTime'] = v['AirTemperature']['MinimumTime']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'Td'] = v['DewPointTemperature']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'RH'] = v['RelativeHumidity']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'RHMin'] = v['RelativeHumidity']['Minimum']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'RHMinTime'] = v['RelativeHumidity']['MinimumTime']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'WS'] = v['WindSpeed']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'WD'] = v['WindDirection']['Prevailing']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'WSGust'] = v['PeakGust']['Maximum']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'WDGust'] = v['PeakGust']['Direction']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'WGustTime'] = v['PeakGust']['MaximumTime']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'Precp'] = v['Precipitation']['Accumulation']  
                if output_df.loc[v['DataDate'], 'Precp'] == -9.8:
                    output_df.loc[v['DataDate'], 'Precp'] = 0.09
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'PrecpHour'] = v['PrecipitationDuration']['Total']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'PrecpMax10'] = v['Precipitation']['TenMinutelyMaximum']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'PrecpMax10Time'] = v['Precipitation']['TenMinutelyMaximumTime']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'PrecpMax60'] = v['Precipitation']['SixtyMinutelyMaximum']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'PrecpMax60Time'] = v['Precipitation']['SixtyMinutelyMaximumTime']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'SunShine'] = v['SunshineDuration']['Total']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'SunShineRate'] = v['SunshineDuration']['Rate']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'GloblRad'] = v['GlobalSolarRadiation']['Accumulation']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'EvapA'] = v['EvaporationClassAPan']['Accumulation']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil0cm'] = v['SoilTemperatureAt0cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil5cm'] = v['SoilTemperatureAt5cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil10cm'] = v['SoilTemperatureAt10cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil20cm'] = v['SoilTemperatureAt20cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil30cm'] = v['SoilTemperatureAt30cm']['Mean']
            except:
                pass
            try:  
                output_df.loc[v['DataDate'], 'TxSoil50cm'] = v['SoilTemperatureAt50cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil100cm'] = v['SoilTemperatureAt100cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil200cm'] = v['SoilTemperatureAt200cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil300cm'] = v['SoilTemperatureAt300cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'TxSoil500cm'] = v['SoilTemperatureAt500cm']['Mean']  
            except:
                pass
            try:
                output_df.loc[v['DataDate'], 'VaporPressure'] = v['VaporPressure']['Mean']  
            except:
                pass
        output_df.fillna(-99.8, inplace=True)
        output_df.index = pd.to_datetime(output_df.index)
        #For which H:M:S is 23:59:00, we need to change it to 00:00:00 by adding 1 minute
        output_df['DataTime_temp'] = output_df.index
        output_df.loc[output_df['DataTime_temp'].dt.strftime('%H:%M:%S') == '23:59:00', 'DataTime_temp'] = output_df.loc[output_df['DataTime_temp'].dt.strftime('%H:%M:%S') == '23:59:00', 'DataTime_temp'] + pd.Timedelta(minutes=1)
        output_df.index = output_df['DataTime_temp']
        output_df.index.name = ""
        output_df.drop('DataTime_temp', axis=1, inplace=True)
        #Sort by index
        output_df.sort_index(inplace=True)
        return output_df    
    def get_full_year(self, sta_id="467490", stn_type='cwb', year = 2022):
        output_df = pd.DataFrame()
        #fetcher(*_hourly_fetch(sta_id="467490", stn_type='cwb', start=datetime(2022,1,1,0,0,0), end=datetime(2022,3,2,0,0,0)))
        start = datetime(year,1,1,0,0,0)
        terminate = min(datetime(year+1,1,1,0,0,0), datetime.now())
        while start < terminate:
            #Max. duration cannot exceed 60 days
            end = start + pd.Timedelta(days=60)
            if end > datetime(year+1,1,1,0,0,0):
                end = datetime(year+1,1,1,0,0,0)
            raw_data = self.fetcher(*self._daily_fetch(sta_id=sta_id, stn_type=stn_type, start=start, end=end))
            try:
                output_df = pd.concat([output_df, self.daily_json_parser(raw_data)])
                print("  Success to process station: {} for {} {}".format(sta_id, start, end))
            except Exception as e:
                print("  Failed to process station: {} for {} {}".format(sta_id, start, end))
            start = end
        return output_df

codis = CODIS()
stations_df = codis.get_stations_df()
#Remove suspended stations (row['stationEndDate'] != "")
stations_df = stations_df[(stations_df['stationEndDate'] == "")]
nagr = NAGR()

stations_df.reset_index(inplace=True, drop=True)
stations_df[-10:]
# %%
from filelock import FileLock
import os
def safe_write_to_csv(log, filename="log.csv"):
    lock_file = f"{filename}.lock"
    with FileLock(lock_file):
        log.to_csv(filename)
def thread_pack (sta_id,stn_type,y):
    filename = "data/{}/{}_{}_daily.csv".format(sta_id, sta_id, y)
    if os.path.exists("log.csv"):
        log = pd.read_csv("log.csv", index_col='sta_id')
        if sta_id in log.index:
            if 'daily' in log.columns:
                AVOID_RE_UPDATE = False
                try:
                    if pd.to_datetime(log.loc[sta_id, 'daily']) > datetime.now() - pd.Timedelta(days=1) and AVOID_RE_UPDATE:
                        print("File {} was updated in the last 24 hours. Skipping...".format(filename))
                        return pd.DataFrame()
                except:
                    pass
    print("Processing station: {} for year {}".format(sta_id, y))  
    if stn_type == 'agr':
        #if station is agr, use NAGR API
        output_df = nagr.getDataByCsvAPI(STA = sta_id, start_time= f"{y}-01-01", end_time=f"{y+1}-01-01", type='daily', save_path = filename)
    else:
        output_df = codis.get_full_year(sta_id=sta_id, stn_type=stn_type, year = y)
    if output_df.empty:
        return pd.DataFrame()
    else:
        #將更新紀錄寫在log.csv中,縱index為站號，橫標題為daily, 值 = 更新時間
        #log.csv可能是空白的檔案，或是已經有部分資料
        print ("Updating log.csv")
        if os.path.exists("log.csv"):
            log = pd.read_csv("log.csv", index_col = 'sta_id')
        else:
            log = pd.DataFrame(columns=['sta_id', 'daily', 'hourly', 'monthly'])
            #Set sta_id as index
            log.set_index('sta_id', inplace=True)
        log.loc[sta_id, 'daily'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        safe_write_to_csv(log, "log.csv")
        output_df.to_csv(filename.format(sta_id, sta_id, y))
    return output_df
# %%
import threading
import time 
station_counter = 0 
waiting_list = []
for index, row in stations_df.iterrows():
    os.makedirs("./data/{}".format(row['stationID']), exist_ok=True)
    sta_id = row['stationID']
    stn_type = ""
    if row['stn_type'] == 'agr':
        stn_type = 'agr'
    elif row['stn_type'] == 'cwb':
        stn_type = 'cwb'
    elif row['stn_type'] == 'auto':
        if row['stationID'][0:2] == 'C0':
            stn_type = 'auto_C0'
        elif row['stationID'][0:2] == 'C1':
            stn_type = 'auto_C1'
    if stn_type == "":
        continue

    if datetime.now().month == 1:
        start_y = datetime.now().year - 1
    else:
        start_y = datetime.now().year
    end_y = datetime.now().year
    for y in range(start_y, end_y+1):
        #Start multi-threading, max. 10 threads, 1 thread for 1 station, timeout = 60 seconds
        t = threading.Thread(target=thread_pack, args=(sta_id,stn_type,y))
        t.start()
        waiting_list.append(t)
        while len(waiting_list) >= 1:
            for t in waiting_list:
                t.join(timeout=60)
                if not t.is_alive():
                    waiting_list.remove(t)
                    break
        station_counter += 1  # 增加计数器
    if station_counter % 5 == 0:
        print("暫停一下子，避免頻繁存取", station_counter)
        time.sleep(20)

# %%
