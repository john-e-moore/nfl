import nfl_data_py as nfl
import pandas as pd 
import time

########################################
# Play-by-play
########################################
"""
pbp_cols = nfl.see_pbp_cols().to_list()

with open('/home/jmlaptop/nfl/pbp_cols.txt', 'w') as f:
    for i,col in enumerate(pbp_cols):
        f.write(f'({i}) {col}\n')

time.sleep(5)

df = nfl.import_pbp_data([2022]).head()

df.to_csv('/home/jmlaptop/nfl/pbp_sample.csv')
"""

########################################
# Weekly
########################################
weekly_cols = nfl.see_weekly_cols().to_list()

with open('/home/jmlaptop/nfl/data/weekly_cols.txt', 'w') as f:
    for i,col in enumerate(weekly_cols):
        f.write(f'({i}) {col}\n')

time.sleep(5)

df = nfl.import_weekly_data([2022]).head()
print(df.head())
print(df.tail())
print(df.dtypes)

df.to_csv('/home/jmlaptop/nfl/data/weekly_sample.csv')