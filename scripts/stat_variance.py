from scipy.stats import norm
from typing import List
import pandas as pd
import numpy as np
import nfl_data_py as nfl

def fetch_player_weekly(years: List[int], retries: int) -> pd.DataFrame:
    """Gets weekly player data from nfl_data_py."""
    
    for retry_count in range(retries):
        try:
            df = nfl.import_weekly_data(years=years)
        except Exception as e:
            print(f"Exception fetching data. {retries - (retry_count + 1)} retries left.")
            print(e)
    return df

df = fetch_player_weekly([2018, 2019, 2020, 2021, 2022], 1)
df.to_csv('data/player_weekly.csv')

cols = [
    'player_display_name',
    'season',
    'completions',
    'attempts',
    'passing_yards',
    'passing_tds',
    'interceptions',
    'rushing_yards',
    'rushing_tds',
    'rushing_fumbles_lost',
    'receptions',
    'targets',
    'receiving_yards',
    'receiving_tds',
    'fantasy_points',
    'fantasy_points_ppr'
]
df = df[cols]
count_df = df.groupby(['player_display_name', 'season']).count()
mask = count_df['completions'] > 10
count_df = count_df[mask]
count_df['count'] = count_df['completions']
count_df = count_df['count']
print(count_df)
df = df.set_index(['player_display_name', 'season'], drop=False)
print(df.columns)
df2 = df.join(count_df).reset_index(drop=True)
print(df2)
print(df2.columns)
pivot_df = df2.groupby(['player_display_name', 'season']).agg(['mean', 'std'])
#mask = pivot_df['count'] > 10
#pivot_df = pivot_df[mask]
pivot_df.to_csv('data/player_weekly_agg.csv')