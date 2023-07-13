from datetime import datetime

def get_current_nfl_season() -> int:
    current_year = datetime.now().year
    current_month = datetime.now().month

    if current_month <= 9:
        return current_year
    else:
        return current_year - 1