import nfl_data_py as nfl
import pandas as pd 

pbp_cols = nfl.see_pbp_cols().to_list()

with open('/home/jmlaptop/nfl/pbp_cols.txt', 'w') as f:
    for i,col in enumerate(pbp_cols):
        f.write(f'({i}) {col}\n')

df = nfl.import_pbp_data(2022).head()

df.to_csv('/home/jmlaptop/nfl/pbp_sample.csv')