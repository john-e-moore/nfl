import pandas as pd
import pyarrow
from utils.logger import get_logger

logger = get_logger(__name__)

def ensure_single_dtype(ser: pd.Series) -> pd.Series:
    """parquet expects each column to have a single dtype."""

    logger.info(f"Checking dtypes for {ser.name}...")

    # Check if ser contains a single dtype
    if len(ser.apply(type).value_counts()) == 1:
        return ser
    
    # Try to convert to datetime
    try:
        ser = pd.to_datetime(ser, format='%Y-%m-%d')
        logger.info(f"{ser.name} converted to datetime.")
    except ValueError:
        # If conversion to datetime fails, try numeric
        try:
            ser = pd.to_numeric(ser)
            logger.info(f"{ser.name} converted to numeric.")
        except ValueError:
            # If conversion to datetime fails, convert to string
            ser = ser.astype(str)
            logger.info(f"{ser.name} converted to string.")
    
    return ser