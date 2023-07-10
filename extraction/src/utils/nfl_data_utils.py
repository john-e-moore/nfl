# Standard
from typing import List
# External
import pandas as pd
import nfl_data_py as nfl
# Internal
from utils.logger import get_logger

logger = get_logger(__name__)

def fetch_pbp(years: List[int], format='json') -> None:
    """Gets play-by-play data from nfl_data_py and converts to JSON."""
    
    df = nfl.import_pbp_data(years=years, downcast=True, cache=False, alt_path=None)

    return df
    