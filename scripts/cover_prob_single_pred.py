import pandas as pd
import numpy as np
import xgboost as xgb
import pyarrow
import sys

# Load model
# Make 1-row Pandas df (list?) of dependent variables
# Apply model
# Extract cover probability

"""
Columns needed for predict_proba:
'spread_time',
 - compares posteam_spread to time elapsed
 - 2.5 (start of game) to 0 (end of game); negative if posteam_is_home = 0
'score_differential',
 - also switches +/- with posteam
'diff_time_ratio',
 - compares score_differential to time elapsed
'posteam_is_home',
 - 1 if posteam (offensive team) is home
'half_seconds_remaining',
'game_seconds_remaining',
'down',
'ydstogo',
'yardline_100',
'posteam_timeouts_remaining',
'defteam_timeouts_remaining',
'receive_2h_ko',
'is_pat',
'spread_line_differential',

stuff I'd be able to put in by hand to use the model:

# Sample
Start of game
KC @ JAX -3 (spread refers to away team)
KC receives kickoff

entry schema:
'away_team' = 'KC'
'home_team' = 'JAX'
'away_score' = 0
'home_score' = 0
'away_timeouts_remaining' = 3
'home_timeouts_remaining' = 3
'away_spread' = -3
'posteam' = 'KC'
'qtr' = 1
'qtr_clock' = '15:00'
'down' = 1
'ydstogo' = 10
'yardline_100' = 75
'receive_2h_ko' = 0
'is_pat' = 0

target schema:
x'spread_time',
 - compares posteam_spread to time elapsed
 - 2.5 (start of game) to 0 (end of game); negative if posteam_is_home = 0
x'score_differential',
 - also switches +/- with posteam
x'diff_time_ratio',
 - compares score_differential to time elapsed
x'posteam_is_home',
 - 1 if posteam (offensive team) is home
x'half_seconds_remaining',
x'game_seconds_remaining',
x'down',
x'ydstogo',
x'yardline_100',
x'posteam_timeouts_remaining',
x'defteam_timeouts_remaining',
x'receive_2h_ko',
x'is_pat',
'spread_line_differential',
"""

OUTPUT_FILEPATH = '/home/jmlaptop/nfl/models/single_cp_multiclass_pred.txt'
MODEL_FILEPATH = '/home/jmlaptop/nfl/models/cp_multiclass.model'

def clock_time_to_seconds(clock_time: str) -> int:
    """mm:ss to seconds; e.g. '15:00' -> 900"""
    split_time = clock_time.split(':')
    return int(split_time[0])*60 + int(split_time[1])

def calculate_game_time_remaining(qtr: int, clock_time: str) -> int:
    """
    qtr: current quarter
    clock_time: 'hh:mm' time remaining in current quarter

    Returns number of seconds left in game
    """
    return (4 - qtr) * 900 + clock_time_to_seconds(clock_time)

def calculate_features(df: pd.DataFrame) -> pd.DataFrame: 
    # posteam_is_home
    df['posteam_is_home'] = df['posteam'] == df['home_team']
    df['posteam_is_home'] = df['posteam_is_home'].astype(int)

    # posteam_timeouts_remaining
    df['posteam_timeouts_remaining'] = np.where(
        df['posteam_is_home'] == 1,
            df['home_timeouts_remaining'],
            df['away_timeouts_remaining']
    )

    # defteam_timeouts_remaining
    df['defteam_timeouts_remaining'] = np.where(
        df['posteam_is_home'] == 1,
            df['away_timeouts_remaining'],
            df['home_timeouts_remaining']
    )

    # posteam_spread
    df['posteam_spread'] = np.where(
        df['posteam_is_home'] == 1,
            df['away_spread'],
            -1 * df['away_spread']
    )

    # score_differential (in relation to posteam)
    df['score_differential'] = np.where(
        df['posteam_is_home'] == 1,
            df['home_score'] - df['away_score'],
            df['away_score'] - df['home_score']
    )

    # game_seconds_remaining
    df['game_seconds_remaining'] = df.apply(
        lambda x: calculate_game_time_remaining(
            x['qtr'], x['clock_time']), axis=1
    )
    # half seconds remaining
    df['half_seconds_remaining'] = np.where(
        df['qtr'] < 3,
            df['game_seconds_remaining'] - 1800,
            df['game_seconds_remaining']
    )

    # elapsed_share
    df['elapsed_share'] = (
        (3600 - df['game_seconds_remaining']) / 
        3600
    )

    # spread_time
    df['spread_time'] = df['posteam_spread'] * np.exp(-4 * df['elapsed_share'])

    # diff_time_ratio
    df['diff_time_ratio'] = df['score_differential'] / np.exp(-4 * df['elapsed_share'])

    # spread_line_differential
    # spread_line is in relation to away team; score_differential in relation to posteam
    df['spread_line_differential'] = np.where(
        df['posteam_is_home'] == 1,
            df['posteam_spread'] + df['score_differential'],
            -1 * df['posteam_spread'] + df['score_differential']
    )

    # is_half_point_spread
    df['is_half_point_spread'] = np.where(
        df['spread_line'] % 1 == 0,
            0,
            1
    )

    model_columns = [
        'spread_time',
        'score_differential',
        'diff_time_ratio',
        'posteam_is_home',
        'half_seconds_remaining',
        'game_seconds_remaining',
        'down',
        'ydstogo',
        'yardline_100',
        'posteam_timeouts_remaining',
        'defteam_timeouts_remaining',
        'receive_2h_ko',
        'is_pat',
        'spread_line_differential'
    ]

    return df[model_columns]

def apply_model(df: pd.DataFrame) -> pd.DataFrame:
    # Load model
    print("Loading XGBClassifier...")
    model = xgb.XGBClassifier()
    model.load_model(MODEL_FILEPATH)

    # Predict cover probability
    print("Predicting probability...")
    df['cover_prob'] = model.predict_proba(df)[:,1]

    return df

def main():
    
    # Sample data
    d = {
        'away_team': ['KC'],
        'home_team': ['JAX'],
        'away_score': [0],
        'home_score': [0],
        'away_timeouts_remaining': [3],
        'home_timeouts_remaining': [3],
        'away_spread': [-3],
        'posteam': ['KC'],
        'qtr': [1],
        'clock_time': ['15:00'],
        'down': [1],
        'ydstogo': [10],
        'yardline_100': [75],
        'receive_2h_ko': [0],
        'is_pat': [0],
    }

    # Sample data
    d = {
        'away_team': ['KC'],
        'home_team': ['JAX'],
        'away_score': [14],
        'home_score': [0],
        'away_timeouts_remaining': [3],
        'home_timeouts_remaining': [3],
        'away_spread': [-3],
        'posteam': ['KC'],
        'qtr': [2],
        'clock_time': ['15:00'],
        'down': [1],
        'ydstogo': [10],
        'yardline_100': [75],
        'receive_2h_ko': [0],
        'is_pat': [0],
    }

    # Sample data
    d = {
        'away_team': ['KC'],
        'home_team': ['JAX'],
        'away_score': [0],
        'home_score': [7],
        'away_timeouts_remaining': [3],
        'home_timeouts_remaining': [3],
        'away_spread': [-3],
        'posteam': ['KC'],
        'qtr': [3],
        'clock_time': ['15:00'],
        'down': [1],
        'ydstogo': [10],
        'yardline_100': [75],
        'receive_2h_ko': [0],
        'is_pat': [0],
    }

    # Create 1-row DataFrame from sample data
    df = pd.DataFrame(d)
    for i,col in enumerate(df.columns):
        print(f"({i}) {col}: {df[col].iloc[0]}")

    model_df = calculate_features(df)
    for i,col in enumerate(model_df.columns):
        print(f"({i}) {col}: {df[col].iloc[0]}")

    # Apply model
    predictions_df = apply_model(model_df)
    cover_prob = predictions_df['cover_prob'].iloc[0]
    print(f"Probability that offensive team covers: {cover_prob}")

    # Thoughts
    # How is total not part of this model? I think that is an important predictor
    # To see if the model is suitable for betting, I need to bin the results by spread (absolute value of spread?)
    # There is more room for feature engineering; this model is just an extension of a NFLFastR win prob model
    # I need to get posteam_cover, defteam_cover, tie probabilities; this model cover probs don't add to 1

if __name__ == "__main__":
    main()