# Standard
from typing import List
from io import BytesIO, StringIO
# External
import pandas as pd
import nfl_data_py as nfl
import pyarrow.parquet as pq
# Internal
from utils.logger import get_logger

logger = get_logger(__name__)

def fetch_pbp(years: List[int], format='json') -> None:
    """Gets play-by-play data from nfl_data_py and converts to JSON."""
    
    df = nfl.import_pbp_data(years=years, downcast=True, cache=False, alt_path=None)

    if format.lower() == 'json':
        out_buffer = StringIO()
        return df.to_json(path_or_buff=out_buffer, orient='records', index=False)
    
    elif format.lower() == 'csv':
        out_buffer = StringIO()
        return df.to_csv(out_buffer, index=False)

    elif format.lower() == 'parquet':
        out_buffer = BytesIO()
        return df.to_parquet(out_buffer, index=False)
    
    else:
        logger.info("Valid formats are json, csv, parquet.")
        return None

    