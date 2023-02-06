#This file is to concatenate all the data of each station together
# %%
import os
import pandas as pd

# %%
ROOT = "./data"

#%%
try:
  os.makedirs("./pooled/", exist_ok=True)
except:
  pass

# %%
for f in os.scandir(ROOT):
    if f.is_dir():
        print(f.name)
        monthly_pool = pd.DataFrame()
        daily_pool = pd.DataFrame()
        hourly_pool = pd.DataFrame()
        for csv_file in os.scandir(f.path):
            if csv_file.name.endswith("monthly.csv"):
                print("Monthly",csv_file.name)
                monthly_pool = pd.concat([monthly_pool,pd.read_csv(csv_file.path)])
            elif csv_file.name.endswith("daily.csv"):
                print("Daily",csv_file.name)
                daily_pool = pd.concat([daily_pool,pd.read_csv(csv_file.path)])
            elif csv_file.name.endswith(".csv"):
                print("Hourly",csv_file.name)
                hourly_pool = pd.concat([hourly_pool,pd.read_csv(csv_file.path)])
            else:
                print("Unknown",csv_file.name)
                continue
        #Sort by date info in ['Unnamed: 0']
        try:
            monthly_pool.sort_values(by=['Unnamed: 0'],inplace=True)
            monthly_pool.to_csv("./pooled/"+f.name+"_monthly.csv",index=False)
        except:
            pass
        try:
            daily_pool.sort_values(by=['Unnamed: 0'],inplace=True)
            daily_pool.to_csv("./pooled/"+f.name+"_daily.csv",index=False)
        except:
            pass
        try:
            hourly_pool.sort_values(by=['Unnamed: 0'],inplace=True)      
            hourly_pool.to_csv("./pooled/"+f.name+"_hourly.csv",index=False)
        except:
            pass

# %%



