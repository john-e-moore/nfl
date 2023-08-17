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
# Weekly player data
########################################
"""
weekly_cols = nfl.see_weekly_cols().to_list()

with open('/home/jmlaptop/nfl/data/weekly_cols.txt', 'w') as f:
    for i,col in enumerate(weekly_cols):
        f.write(f'({i}) {col}\n')

time.sleep(5)
"""
years = [x for x in range(1999, 2023)]
df = nfl.import_weekly_data([2022])
df = df[df['position'] == 'QB']
print(df.head())
print(df.tail())
print(df.dtypes)

df.to_csv('/home/jmlaptop/nfl/data/qb_weekly_2018_2022.csv')

"""
########################################
# Seasonal data
########################################
df = nfl.import_seasonal_data([2022]).head()
df.to_csv('/home/jmlaptop/nfl/data/seasonal_sample.csv')
time.sleep(5)

########################################
# Rosters
########################################
df = nfl.import_rosters([2022]).head()
df.to_csv('/home/jmlaptop/nfl/data/rosters_sample.csv')
time.sleep(5)

########################################
# Win totals
########################################
df = nfl.import_win_totals([2022]).head()
df.to_csv('/home/jmlaptop/nfl/data/win_totals_sample.csv')
time.sleep(5)

########################################
# Scoring lines
########################################
df = nfl.import_sc_lines([2022]).head()
df.to_csv('/home/jmlaptop/nfl/data/scoring_lines_sample.csv')
time.sleep(5)

########################################
# Officials
########################################
df = nfl.import_officials([2022]).head()
df.to_csv('/home/jmlaptop/nfl/data/officials_sample.csv')
time.sleep(5)

########################################
# Draft picks
########################################
df = nfl.import_draft_picks([2022]).head()
df.to_csv('/home/jmlaptop/nfl/data/draft_picks_sample.csv')
time.sleep(5)

########################################
# Draft values
########################################
df = nfl.import_draft_values().head()
df.to_csv('/home/jmlaptop/nfl/data/draft_values_sample.csv')
time.sleep(5)

########################################
# Team logos/colors etc.
########################################
df = nfl.import_team_desc().head()
df.to_csv('/home/jmlaptop/nfl/data/team_desc_sample.csv')
time.sleep(5)

########################################
# Player ids
########################################
df = nfl.import_ids().head()
df.to_csv('/home/jmlaptop/nfl/data/player_ids_sample.csv')
time.sleep(5)
"""