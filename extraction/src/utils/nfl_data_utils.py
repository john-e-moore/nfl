# Standard
from typing import List
# External
import pandas as pd
import nfl_data_py as nfl

def fetch_pbp(years: List[int]) -> None:
    """Gets play-by-play data from nfl_data_py and converts to JSON."""
    
    df = nfl.import_pbp_data(years=years, downcast=True, cache=False, alt_path=None)
    json_data = df.to_json(orient='records')

    return json_data