# Standard
from typing import List
import time
# External
import nfl_data_py as nfl
import pandas as pd
import pyarrow
# Internal
#from utils.logger import get_logger
#from utils.s3_utils import write_df_to_s3, check_file_exists

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, confusion_matrix
from sklearn import set_config

columns = [
    'season',
    'week',
    'away_team',
    'home_team',
    'season_type', # filter for reg season
    'away_score',
    'home_score',
    'score_differential',
    'qtr', # derive 'half' from this
    'down',
    'yrdln', # 'BAL 35'
    'ydstogo',
    'goal_to_go',
    'posteam', # offensive team
    'posteam_type', # 'home' or 'away'
    'defteam', # defensive team
    'side_of_field',
    'yardline_100',
    'half_seconds_remaining',
    'game_seconds_remaining',
    'home_timeouts_remaining',
    'away_timeouts_remaining',
    'weather', # use to create yes/no for rain, temperature decile
    'wind',
    'temp',
    'location',
    'surface',
    'roof',
    'start_time',
    'div_game',
    'spread_line',
    'result',
    'total_line',
    'total'
]

filepath = "/home/jmlaptop/nfl/data/win_prob_1999_2022.parquet"
df = pd.read_parquet(filepath, engine='pyarrow')
df = df[columns]
df = df[df['season_type'] == 'REG']
print(df.head())
for i,col in enumerate(df.columns):
    print(f"({i}) {col}")
print(f"{len(df.columns)} columns")
print(f"{len(df)} rows")
df['away_beat_spread'] = df['result'] > df['spread_line']
df['away_beat_spread'] = df['away_beat_spread'].astype(int)
df['home_beat_spread'] = df['spread_line'] > df['result']
df['home_beat_spread'] = df['home_beat_spread'].astype(int)
describe_df = df.describe(include='all').transpose()
print(describe_df)
describe_df.to_csv('/home/jmlaptop/nfl/data/describe.csv')

# calculated columns
# away_beat_spread (true if result > spread_line)


# which years have wind/temp?
#wind_by_year = df[~df['wind'].isna()].groupby('season').size()
#print(wind_by_year) 
# data missing from most years, entirely for 2021
# just not going to use wind and temp
# TODO: just try a minimal model first with game state and spread
# away_team_on_offense (1,0)
df['away_team_on_offense'] = df['posteam'] == df['away_team']
df['away_team_on_offense'] = df['away_team_on_offense'].astype(int)
print(df['away_team_on_offense'].head())
# down_str = ["_{str(x)} for x in df['down']"]
# down not numeric
spread_columns = [
    'score_differential',
    'away_team_on_offense',
    #'down', # convert to categorical
    #'ydstogo',
    #'goal_to_go',
    #'yardline_100',
    #'qtr', # convert to categorical
    'game_seconds_remaining',
    #'half_seconds_remaining',
    #'away_timeouts_remaining',
    #'home_timeouts_remaining',
    #'location', # categorical
    #'surface', # categorical
    #'roof', # categorical
    #'div_game',
    #'spread_line',
    #'total_line',
    'away_beat_spread'
]
spread_df = df[spread_columns].dropna()
print(f"spread_df length after dropping nulls: {len(spread_df)}")
"""
spread_df['down_str'] = [f"down_{str(int(x))}" for x in spread_df['down']]
print(spread_df['down_str'].head())

# Predictor columns
predictor_columns = [x for x in spread_df.columns if x != 'away_beat_spread']
print(f"Predictor columns: {predictor_columns}")

# Numerical columns

# One-hot encoding for categorical features
spread_df_encoded = pd.get_dummies(spread_df, 
                      prefix=['down_str', 'location', 'surface', 'roof'], 
                      columns = ['down_str', 'location', 'surface', 'roof'], 
                      drop_first=True)

describe_spread_df_encoded = spread_df_encoded.describe(include='all').transpose()
describe_spread_df_encoded.to_csv('/home/jmlaptop/nfl/data/describe_spread_df_encoded.csv')
"""


################################################################################################
################################################################################################
################################################################################################

set_config(display='diagram')

# Select predictor columns (feature selection can be refined further)
#predictor_cols = [x for x in spread_df.columns if x != 'away_beat_spread']
predictor_cols = ['score_differential', 'game_seconds_remaining', 'away_team_on_offense']

# Split the data into train and test sets
train, test = train_test_split(spread_df, test_size=0.2, random_state=42)

# Define preprocessing for numeric columns (scale them)
numeric_features = [
    'score_differential',
    #'ydstogo',
    #'goal_to_go',
    #'yardline_100',
    'game_seconds_remaining',
    #'half_seconds_remaining',
    #'away_timeouts_remaining',
    #'home_timeouts_remaining',
    #'spread_line',
    #'total_line'
]
numeric_transformer = Pipeline(steps=[
    #('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())])

# Define preprocessing for categorical columns (one-hot encode them)
categorical_features = [
    'away_team_on_offense',
    #'down_str',
    #'qtr',
    #'location',
    #'surface',
    #'roof'
]
categorical_transformer = Pipeline(steps=[
    #('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))])

# Combine preprocessing steps
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)])

# Define the model
model = Pipeline(steps=[('preprocessor', preprocessor),
                        ('classifier', LogisticRegression())])

# Train the model
model.fit(train[predictor_cols], train['away_beat_spread'])
print("Model trained.")

# Apply the model to predict probabilities
probabilities = model.predict_proba(test[predictor_cols])

# Add the predicted probabilities to the DataFrame
test['away_team_beats_spread_probability'] = probabilities[:, 1]  # The second column is the probability that the away team beats the spread

# Write the DataFrame to a CSV file
test.to_csv("/home/jmlaptop/nfl/data/predictions.csv", index=False)

print(test.head())



"""
set_config(display='diagram') # Allows to visualize the pipeline

# Create the binary target variables
df_new['beat_spread'] = (df_new['result'] + df_new['spread_line']) > 0
df_new['over_under'] = df_new['result'] + df_new['total']

# TODO:
# GPT predicting the wrong thing. predictors I want:
# away_score
# home_score
# down
# ydstogo
# yrdln (e.g. 'BAL 35') - transform this into yds_to_endzone
# yardline_100
# side_of_field
# defteam
# posteam
# spread_line
# total
# preseason win total
# week
## filter season_type = 'REG'
# weather like %rain%
# roof (e.g. 'outdoors')
# surface (e.g. 'fieldturf')

# Select predictor columns (feature selection can be refined further)
predictor_cols = ['away_score', 'home_score', 'spread_line', 'total', 'week', 
                  'away_team', 'home_team', 'posteam', 'defteam', 'season_type']

# Split the data into train and test sets
train, test = train_test_split(df_new, test_size=0.2, random_state=42)

# Define preprocessing for numeric columns (scale them)
numeric_features = ['away_score', 'home_score', 'spread_line', 'total', 'week']
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())])

# Define preprocessing for categorical columns (one-hot encode them)
categorical_features = ['away_team', 'home_team', 'posteam', 'defteam', 'season_type']
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))])

# Combine preprocessing steps
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)])

# Define the model
model = Pipeline(steps=[('preprocessor', preprocessor),
                        ('classifier', LogisticRegression())])

# Train the model
model.fit(train[predictor_cols], train['beat_spread'])

# Now the model is trained and ready to predict whether the away team beats the spread
model
"""