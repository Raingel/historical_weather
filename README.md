# 臺灣歷史氣象觀測資料庫 
# Taiwan Historical Weather Database

## 資料來源 Sources of data

這裡的氣象資料來源包含中央氣象局建立之氣象測站(C0、C1、4開頭之測站)及農委會所屬之農業氣象站(前述站號以外的測站)。

The sources of meteorological data here include the meteorological stations established by the Central Weather Bureau (station code beginning with C0, C1, and 4) and the agricultural meteorological stations belonging to the Council of Agriculture (station code other than those mentioned above).

## 使用說明 User's Guide

由於臺灣的氣象觀測站在每日12點將前一日的觀測資料更新到資料庫，因此本系統設定每天下午一點 (GMT+8)從資料庫獲取氣象資料。
使用時可直接由本repo之data資料夾內獲取資料，第一層資料夾以站號區別，資料夾內的天氣觀測資料則依據年分存成獨立的csv格式檔案。
站號的清單可以參考這裡：https://github.com/Raingel/weather_station_list

Since the weather observation stations in Taiwan update the previous day's observation data to the database at 12:00 p.m. daily, the system is set to acquire meteorological data from the database at 1:00 p.m. (GMT+8) daily.

The data can be obtained directly from the data folder of this repo. The first layer of the folder is identified by station number, and the weather observation data in the folder is stored in separated csv format files according to the year.

The list of station numbers can be found here: https://github.com/Raingel/weather_station_list

## 網頁版使用者介面 Web-based user interface

如果你不熟悉github，也可以參考這個網頁版的介面來下載氣象資料。
https://mycolab.pp.nchu.edu.tw/historical_weather/

In case you are not familiar with github, you can also refer to this web interface to download meteorological data.
https://mycolab.pp.nchu.edu.tw/historical_weather/

## 注意事項 Note

使用此資料時，請務必註明資料來源為中央氣象局建置之氣象觀測站(https://e-service.cwb.gov.tw/HistoryDataQuery/) 或農委會之農業氣象觀測網(https://agr.cwb.gov.tw/NAGR/history/station_day/ )。

When using these data, please be sure to indicate that the source of the data is from the meteorological observation stations established by the Central Weather Bureau (CWB; https://e-service.cwb.gov.tw/HistoryDataQuery/) or the Agricultural Meteorological Observation Network (Station) of the Council of Agriculture (CoA; https://agr.cwb.gov.tw/NAGR/history/station_day/).

## 引用此資料庫 Cite this database

請引用
Ou, J.-H., Kuo, C.-H., Wu, Y.-F., Lin, G.-C., Lee, M.-H., Chen, R.-K., Chou, H.-P., Wu, H.-Y., Chu, S.-C., Lai, Q.-J., Tsai, Y.-C., Lin, C.-C., Kuo, C.-C., Liao, C.-T., Chen, Y.-N., Chu, Y.-W., Chen, C.-Y., 2022. Application-oriented deep learning model for early warning of rice blast in Taiwan. Ecological Informatics 101950. https://doi.org/10.1016/j.ecoinf.2022.101950

Please cite as:
Ou, J.-H., Kuo, C.-H., Wu, Y.-F., Lin, G.-C., Lee, M.-H., Chen, R.-K., Chou, H.-P., Wu, H.-Y., Chu, S.-C., Lai, Q.-J., Tsai, Y.-C., Lin, C.-C., Kuo, C.-C., Liao, C.-T., Chen, Y.-N., Chu, Y.-W., Chen, C.-Y., 2022. Application-oriented deep learning model for early warning of rice blast in Taiwan. Ecological Informatics 101950. https://doi.org/10.1016/j.ecoinf.2022.101950


