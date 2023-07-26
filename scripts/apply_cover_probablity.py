import pandas as pd
import numpy as np
import xgboost as xgb
import pyarrow
import sys

OUTPUT_FILEPATH = '/home/jmlaptop/nfl/models/output.csv'
MODEL_FILEPATH = '/home/jmlaptop/nfl/models/cp.model'
PBP_FILEPATH = '/home/jmlaptop/nfl/data/week_1_pbp.parquet'
COLUMNS = [
    'play_id',
    'game_id',
    'season',
    'week',
    'away_team',
    'home_team',
    'away_score',
    'home_score',
    'season_type', # filter for reg season
    'posteam', # offensive team
    'posteam_type', # 'home' or 'away'
    'defteam', # defensive team
    'side_of_field',
    'yardline_100',
    'half_seconds_remaining',
    'game_seconds_remaining',
    'qtr', # derive 'half' from this
    'down',
    'yrdln', # 'BAL 35'
    'ydstogo',
    'weather', # use to create yes/no for rain, temperature decile
    'wind',
    'temp',
    'location',
    'surface',
    'roof',
    'spread_line',
    'total',
    'play_type',
    'game_half',
    'result',
    'score_differential',
    'posteam_timeouts_remaining',
    'defteam_timeouts_remaining'
]

def preprocess_raw_pbp(df: pd.DataFrame):
    print("Preprocessing data...")
    # Trim unnecessary columns
    pbp_df = df[COLUMNS].copy()

    ## create some new variables for the model ##
    ## most features taken directly from nflfastR ##

    ## SPREAD_LINE_DIFFERENTIAL ##
    ## instead of a point differential, use a spread line differential ##
    ## ie how close is the team to covering ##
    pbp_df['spread_line_differential'] = np.where(
        pbp_df['posteam_type'] == 'home',
        -1 * pbp_df['spread_line'] + pbp_df['score_differential'],
        np.where(
            pbp_df['posteam_type'] == 'away',
            pbp_df['spread_line'] + pbp_df['score_differential'],
            np.nan
        )
    )

    ## elapsed share, spread_time, and Diff_Time_Ratio are all custom features from nflfastR's model ##
    ## https://raw.githubusercontent.com/mrcaseb/nflfastR/master/R/helper_add_ep_wp.R ##

    ## elapsed share ##
    pbp_df['elapsed_share'] = (
        (3600 - pbp_df['game_seconds_remaining']) / 
        3600
    )

    pbp_df['posteam_spread'] = np.where(
        pbp_df['posteam_type'] == 'home',
        pbp_df['spread_line'],
        -1 * pbp_df['spread_line']
    )

    ## spread_time ##
    pbp_df['spread_time'] = pbp_df['posteam_spread'] * np.exp(-4 * pbp_df['elapsed_share'])

    ## Diff_Time_Ratio ##
    pbp_df['diff_time_ratio'] = pbp_df['score_differential'] / np.exp(-4 * pbp_df['elapsed_share'])


    ## RECEIVE_2H_KO ##
    ## determine who received the first kickoff ##
    kickoff_df = pbp_df[pbp_df['play_type'] == 'kickoff'].groupby(
        ['game_id']
    )[['game_id','posteam_type']].head(1)

    ## add back to df ##
    pbp_df = pd.merge(
        pbp_df,
        kickoff_df.rename(columns={
            'posteam_type' : 'received_first_ko'
        }),
        on=['game_id'],
        how='left'
    )

    ## create receive 2nd half ko variable ##
    pbp_df['receive_2h_ko'] = np.where(
        (pbp_df['game_half'] == 'Half1') &
        (pbp_df['posteam_type'] != pbp_df['received_first_ko']),
        1,
        0
    )


    ## IS_PAT ##
    ## denote if a play is a pat ##
    pbp_df['is_pat'] = np.where(
        pbp_df['play_type'] == 'extra_point',
        1,
        0
    )


    ## POSTEAM_IS_HOME ##
    ## turn posteam_type into a boolean ##
    pbp_df['posteam_is_home'] = np.where(
        pbp_df['posteam_type'] == 'home',
        1,
        np.where(
            pbp_df['posteam_type'] == 'away',
            0,
            np.nan
        )
    )


    ## COVER_RESULT ##
    pbp_df['cover_result'] = np.where(
        pbp_df['posteam_type'] == 'home',
        np.where(
            -1 * pbp_df['spread_line'] + pbp_df['result'] > 0,
            1,
            0
        ),
        np.where(
            pbp_df['posteam_type'] == 'away',
            np.where(
                pbp_df['spread_line'] + -1 * pbp_df['result'] > 0,
                1,
                0
            ),
            np.nan
        )
    )


    ## filter down to just the columns we need for the model ##
    ## preserving full pbp_df in case we want to use it later ##
    ## narrator: They did not, in fact, end up using it later ##
    model_df = pbp_df[[
        ## only needed for train/test split ##
        'play_id',
        'game_id',
        'season',
        ## dependent var ##
        'cover_result',
        ## independent vars from WP model ##
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
        ## new features for CP model ##
        'is_pat',
        'spread_line_differential',
    ]].copy()

    ## remove NAs ##
    model_df = model_df.dropna()

    return model_df

def apply_model(df):
    # Load model
    print("Loading XGBClassifier...")
    model = xgb.XGBClassifier()
    model.load_model(MODEL_FILEPATH)

    # Predict cover probability
    print("Predicting probability...")
    df['cover_prob'] = model.predict_proba(df.drop(columns=['cover_result', 'season']).set_index(['play_id', 'game_id']))[:,1]

    return df

def main():
    # Load data
    pbp_df = pd.read_parquet(PBP_FILEPATH, engine='pyarrow')

    # Need to join output so I can see columns like home team, away team, season, etc.
    all_cols_df = pbp_df[COLUMNS].copy().set_index(['play_id', 'game_id'])
    print(f"Length of all_cols_df: {len(all_cols_df)}")

    # Preprocess for modeling
    model_df = preprocess_raw_pbp(pbp_df)

    # Apply model
    #predictions_df = apply_model(model_df)[['cover_result', 'cover_prob']]
    predictions_df = apply_model(model_df).set_index(['play_id', 'game_id'])[['spread_line_differential', 'cover_result', 'cover_prob']]
    print(f"Length of predictions_df: {len(predictions_df)}")

    print(f"All cols df:\n {all_cols_df.dtypes}")
    print(all_cols_df.head())
    print(f"Prediction df:\n {predictions_df.dtypes}")
    print(predictions_df.head())
    #sys.exit()

    # Trim data to select 1 game
    #all_cols_df = all_cols_df[all_cols_df.index == '2022_01_BAL_NYJ']
    #predictions_df = predictions_df[predictions_df.index == '2022_01_BAL_NYJ']
    # Join
    output_df = all_cols_df.join(predictions_df, how='inner').sort_values('game_seconds_remaining', ascending=False)
    print(f"{output_df.head()}")
    for col in output_df.columns:
        print(col)
    print(f"Rows: {len(output_df)}")
    output_df.to_csv(OUTPUT_FILEPATH)

if __name__ == "__main__":
    main()