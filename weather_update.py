# %%
import sys
import argparse
ROOT = './'
import pandas as pd
import requests
from dateutil.parser import parse
import json
from datetime import datetime, timedelta
import os

parser = argparse.ArgumentParser()
parser.add_argument("-t",
                    "--type",
                    type=str,
                    default="daily",
                    help="Type of data to be retrieved: daily or hourly")

parser.add_argument("-a",
                    "--all",
                    type=str,
                    default=False,
                    help="If true, all years will be retrieved")

parser.add_argument("-o",
                    "--overwrite", 
                    type=str,
                    default=True,
                    help="If true, existing files will be overwritten")

#jupyter會傳一個-f進來，不這樣接會有錯誤
args, unknown = parser.parse_known_args()
ALL = args.all
OVERWRITE = args.overwrite
TYPE = args.type
# %%

def agr_get_sta_list(area_id=0, level_id=0):
    my_headers = {    
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
    URI = 'https://agr.cwb.gov.tw/NAGR/history/station_day/get_station_name'
    area = ['', '北', '中', '南', '東'][area_id]
    level = ['自動站', '新農業站'][level_id]
    r1 = requests.post(URI, data={'area':area, 'level':level}, headers = my_headers)
    sta_dict = json.loads(r1.text)
    df = pd.DataFrame(sta_dict)
    extract_df = df[['ID', 'Cname', 'Altitude', 'Latitude_WGS84', 'Longitude_WGS84', 'Address', 'StnBeginTime', 'stnendtime', 'stationlist_auto']]
    extract_df.columns=['站號', '站名', '海拔高度(m)', '緯度', '經度', '地址', '資料起始日期', '撤站日期', '備註']
    return extract_df
def load_weather_station_list(include_suspended = False, include_agr_sta = True):
    #load from CWB
    raw = pd.read_html('https://e-service.cwb.gov.tw/wdps/obs/state.htm')
    weather_station_list = raw[0]
    if include_suspended:
        weather_station_list =  weather_station_list.append(raw[1])
    #load from agri
    if include_agr_sta:
        weather_station_list = weather_station_list.append(agr_get_sta_list(level_id=1), ignore_index = True)
    return weather_station_list


# In[168]:


#Fetch From https://agr.cwb.gov.tw/NAGR/history/station_day
my_headers = {    
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

def agr_get_hour_data (station = '466910', start_time = '2021-08-24', end_time = '2021-08-24', items=['PrecpHour']):
    get_hour = "https://agr.cwb.gov.tw/NAGR/history/station_hour/get_station_hour"
    #Data may exist in the "Automatic Station" database
    #The database has different date processing behaviors for automatic and agricultural stations
    start_time_dt = parse(start_time)
    end_time_dt = parse(end_time)
    r1 = requests.post(get_hour, data = {'station' : station, 'start_time': start_time_dt.strftime('%Y%m%d'), 'end_time': end_time_dt.strftime('%Y%m%d'), 'items[]': items, 'level': ''}, headers = my_headers)
    r2 = requests.post(get_hour, data = {'station' : station, 'start_time': start_time_dt.strftime('%Y%m%d'), 'end_time': end_time_dt.strftime('%Y%m%d'), 'items[]': items, 'level': '自動站'}, headers = my_headers)
    #return r1.text,r2.text
    #Try parsing return data
    try:
        r1_parsed = json.loads(r1.text)['result']
    except Exception as e:
        r1_parsed=[]
        #print('r1 parse error: ',e, r1.text)
        
    try:
        r2_parsed = json.loads(r2.text)['result']
    except Exception as e:
        r2_parsed=[]
        #print('r2 parse error: ',e, r2.text)

    
    #only one of r1 and r2 will contain required data
    if r1_parsed != []:
        #print (station, "為農業站")
        return r1_parsed
    if r2_parsed != []:
        #print (station, "為自動站")
        return r2_parsed
    return []


def agr_get_items (station = '466910'): #check available fields
    get_items_URI = "https://agr.cwb.gov.tw/NAGR/history/station_day/get_items"
    r = requests.post(get_items_URI, data = {'station': station},  headers= my_headers)
    try:
        return json.loads(r.text)['columns']
    except:
        return r.text
    
def agr_parsing (f):
    try:
        rt = list(f.values())[0]
        rt = float(rt)
        if rt < -90:
            return -999
        return rt
    except Exception as e:
        return -999
    
    
def agr_fetch (station_num=467080, date='2021-08-16'):
    #Not in use
    output_columns = {
        'StaNO': '站號',
        'ObsTime': '觀測時間(hour)',
        'StnPres': '測站氣壓(hPa)',
        'SeaPres': '海平面氣壓(hPa)',
        'Tx': '氣溫(℃)',
        'Td': '露點溫度(℃)',
        'RH': '相對溼度(%)',
        'WS': '風速(m/s)',
        'WD': '風向(360degree)',
        'WSGust': '最大陣風(m/s)',
        'WDGust': '最大陣風風向(360degree)',
        'Precp': '降水量(mm)',
        'PrecpHr': '降水時數(hr)',
        'SunShine': '日照時數(hr)',
        'GloblRad': '全天空日射量(MJ/㎡)',
        'Visb': '能見度(km)',
        'UVI': '紫外線指數',
        'Cloud':'總雲量(0~10)'}
    #initialize the dataFrame
    df = pd.DataFrame(columns = output_columns.values())
    #If the items variable contains something that is not in the database, an error will occur. So you have to check the intersection first
    sta_item = set(output_columns.keys()).intersection(agr_get_items(station_num))
    d = agr_get_hour_data(station=station_num,start_time=date, end_time=date, items=sta_item)
    df['觀測時間(hour)'] = pd.date_range(date+' 01:00:00', periods=24, freq='1h')
    df['站號'] = station_num
    df.set_index('觀測時間(hour)', inplace=True)
    #if no valid data
    if (d==[]):
        return pd.DataFrame()
    for single_factor in d:
        for rec in single_factor:
            #{'StnPres': '924.4', 'ObsTime': '2020-08-15 02:00:00'}
            ObsTime = parse(rec['ObsTime'])
            #in case of 8/16 23:59:00
            if ObsTime.strftime('%H%M')=='2359':
                ObsTime += timedelta(minutes=1)
            del rec['ObsTime']
            #After deleting ObsTime, the last key left is the observation data
            col_code = list(rec)[0]
            col_name = output_columns[col_code]
            #fill the data into the table
            df.loc[ObsTime,col_name] = rec[col_code]

    df.fillna(-999, inplace=True)
    df.reset_index(inplace=True)
    #dt need to be converted to str before enter MySQL
    df['觀測時間(hour)'] = df['觀測時間(hour)'].dt.strftime('%Y-%m-%d %H:%i:%s')
    
    return df

def agr_fetch_year_full (station_num=467080, year='2021', save_path = ''):
    sta_item = agr_get_items(station_num)
    df = pd.DataFrame()
    for month in range(1,13):
        start_time = '{}/{}/{}'.format(year,month,'1')
        if month == 12:
            end_time = '{}/{}/{}'.format(year,month,'31')
        else:
            end_time = (parse('{}/{}/{}'.format(year,month + 1,'1')) - timedelta(days = 1)).strftime('%Y/%m/%d')
        data_list = agr_get_hour_data(station=station_num,start_time=start_time, end_time=end_time, items=sta_item)
        df = df.append(agr_factor_to_df(data_list))     
    if save_path != '':
        df.to_csv(save_path, encoding = 'utf-8-sig')
    print(station_num, year,'downloaded')
    return df
    
def add_1min_to_2359 (d):
    if d.strftime('%H%M')=='2359':
        d += timedelta(minutes=1)
    return d

def agr_factor_to_df (d):
    df = pd.DataFrame()
    for r in d:
        keys = list(r[0])
        keys.remove('ObsTime')
        #There will be two keys in the data, one is the observation time, and the other is the query data
        factor_key = keys[0]
        data = [row[factor_key] for row in r]
        ObsTime = [parse(row['ObsTime']) for row in r]
        ObsTime = list(map(add_1min_to_2359 , ObsTime))
        df_temp = pd.DataFrame(index = ObsTime)
        df_temp[factor_key] = data
        #Seldomly 23:59 and 00:00 are simultaneously present in the same dataset
        #which will cause duplicated keys after adding 1 minitues to 23:59
        #e.g. sta_no 72C440 2016/10/18-2016/10/20            
        df_temp = df_temp[~df_temp.index.duplicated(keep='last')]
        if df.empty:
            df = df_temp.copy()
        else: 
            df = pd.merge(df, df_temp, left_index=True, right_index=True, how='outer')
    return df
def agr_get_daily_data (station = '466910', start_time = '2021-08-16', end_time = '2021-08-24', items=['PrecpHour']):
    get_hour = "https://agr.cwb.gov.tw/NAGR/history/station_day/get_station_day"
    start_time_dt = parse(start_time)
    end_time_dt = parse(end_time)
    r1 = requests.post(get_hour, data = {'station' : station, 'start_time': start_time_dt.strftime('%Y%m%d'), 'end_time': end_time_dt.strftime('%Y%m%d'), 'items[]': items, 'level': ''}, headers = my_headers)
    r2 = requests.post(get_hour, data = {'station' : station, 'start_time': start_time_dt.strftime('%Y%m%d'), 'end_time': end_time_dt.strftime('%Y%m%d'), 'items[]': items, 'level': '自動站'}, headers = my_headers)
    #return r1.text,r2.text
    #Try parsing return data
    try:
        r1_parsed = json.loads(r1.text)['result']
    except Exception as e:
        r1_parsed=[]
        #print('r1 parse error: ',e, r1.text)
        
    try:
        r2_parsed = json.loads(r2.text)['result']
    except Exception as e:
        r2_parsed=[]
        #print('r2 parse error: ',e, r2.text)

    
    #only one of r1 and r2 will contain required data
    if r1_parsed != []:
        #print (station, "為農業站")
        return r1_parsed
    if r2_parsed != []:
        #print (station, "為自動站")
        return r2_parsed
    return []
def agr_fetch_year_daily (station_num=467080, year='2021', save_path = ''):
    sta_item = agr_get_items(station_num)
    df = pd.DataFrame()
    for month in range(1,13):
        start_time = '{}/{}/{}'.format(year,month,'1')
        if month == 12:
            end_time = '{}/{}/{}'.format(year,month,'31')
        else:
            end_time = (parse('{}/{}/{}'.format(year,month + 1,'1')) - timedelta(days = 1)).strftime('%Y/%m/%d')
        data_list = agr_get_daily_data(station=station_num,start_time=start_time, end_time=end_time, items=sta_item)
        df = df.append(agr_factor_to_df(data_list))     
    if save_path != '':
        df.to_csv(save_path, encoding = 'utf-8-sig')
    print(station_num, year,'downloaded')
    return df

#CODIS
def parse_time (date, h):
    if h != 24:
        return parse("{} {}:00".format(date, h))
    if h == 24:
        return parse("{} 00:00".format(date, h)) + timedelta(days = 1)


def download_from_CODIS (sta_no = '467080', date = '2021-08-16'):
    URI = "https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station={}&stname=%25E7%25A6%258F%25E5%25B1%25B1&datepicker={}&altitude=405m".format(sta_no, date)
    try:
        w_df = pd.read_html(URI, encoding = 'utf-8')[1]
        #w_df.columns = ['ObsTime','StnPres','SeaPres','Temperature','Td dew point',
        #'RH','WS','WD','WSGust','WDGust',
        #'Precp','PrecpHour','SunShine','GloblRad','Visb','UVI','Cloud Amount']
        w_df.columns = ['觀測時間(hour)', '測站氣壓(hPa)', '海平面氣壓(hPa)', '氣溫(℃)', '露點溫度(℃)',
       '相對溼度(%)', '風速(m/s)', '風向(360degree)', '最大陣風(m/s)', '最大陣風風向(360degree)',
       '降水量(mm)', '降水時數(hr)', '日照時數(hr)', '全天空日射量(MJ/㎡)', '能見度(km)', '紫外線指數',
       '總雲量(0~10)']
        if len(w_df) == 0:
            return pd.DataFrame()
    except:
        return pd.DataFrame()
    w_df['觀測時間(hour)'] = w_df['觀測時間(hour)'].apply(lambda x: parse_time(date, x))
    w_df.replace(['...','/','V','x','&'], float('nan'), inplace = True)
    w_df.replace('T', 0.05, inplace = True)
    return w_df

# %%
sta_list = load_weather_station_list(include_suspended = True)
print('Weather station list downloaded')
#Problematic data (sta_no='466920',start_date='2022-03-15',end_date='2022-04-20')

# %%
import concurrent.futures
with concurrent.futures.ThreadPoolExecutor() as executor:
    for index, row in sta_list[:].iterrows():
        sta_info = {}
        sta_info ['sta_no'] = row[0]
        sta_info ['sta_name'] = row[1]
        #Because the suspended weather station has been pre-downloaded
        #So no need to consider the problems of the already suspended weather station
        sta_info ['start'] = parse(row['資料起始日期']).year
        sta_info ['end'] = datetime.today().year if (str(row['撤站日期']) == 'nan' or str(row['撤站日期']) == '') else parse(row['撤站日期']).year
        DIR_PATH = os.path.join(ROOT,'data',sta_info ['sta_no'])
        os.makedirs(DIR_PATH, exist_ok=True)
        #start multi-threading download
        futures=[]
        #loop over all years
        if datetime.today().strftime('%m%d') == '0101':
            #if it is the first day of the year, update the last year
            years = [datetime.today().year - 1, datetime.today().year]
        else:
            years = [datetime.today().year]

        #First Time -- Re-download all years
        if ALL:
            years = range(sta_info['start'], sta_info['end'] + 1)
            
        for year in years:
            CSV_PATH_FULL = os.path.join(DIR_PATH,sta_info['sta_no']+'_'+str(year)+'.csv')
            CSV_PATH_DAILY = os.path.join(DIR_PATH,sta_info['sta_no']+'_'+str(year)+'_daily.csv')
            #Download full data
            try:
                if TYPE == 'hourly':
                    if not OVERWRITE and os.path.exists(CSV_PATH_FULL):
                        print('{} already exists'.format(CSV_PATH_FULL))
                        continue
                    print ('Try downloading (full/hourly)',index, sta_info['sta_no'], year)
                    future = executor.submit(
                                                agr_fetch_year_full, sta_info['sta_no'], 
                                                year, 
                                                CSV_PATH_FULL
                                            )
                elif TYPE == 'daily':
                    if not OVERWRITE and os.path.exists(CSV_PATH_DAILY):
                        print('{} already exists'.format(CSV_PATH_DAILY))
                        continue
                    print ('Try downloading (daily)',index, sta_info['sta_no'], year)
                    future = executor.submit(
                                                agr_fetch_year_daily, sta_info['sta_no'], 
                                                year, 
                                                CSV_PATH_DAILY
                                            )
                futures.append(future)
            except Exception as e:
                print(e)
        #wait for threads to finish for every 5 stations
        if index % 20 == 0:
            print('Waiting for all threads to finish')
            for future in futures:
                try:
                    future.result(timeout=300)
                except concurrent.futures.TimeoutError:
                    print('Timeout error', future)

            print('All threads finished')
            futures.clear()
            print('Clear futures')          

    #wait job to be finished
    print('Waiting for all threads to finish')
    for future in futures:
        try:
            future.result(timeout=500)
        except concurrent.futures.TimeoutError:
            print('Timeout error', future)



