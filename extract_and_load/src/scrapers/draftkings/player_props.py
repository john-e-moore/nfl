# Standard
import yaml
import time
from typing import List
from datetime import datetime
# External
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
# Internal
from utils.logger import get_logger
from utils.s3_utils import write_df_to_s3, check_file_exists
from utils.scraper_utils import get_selenium_chrome_browser

logger = get_logger(__name__)

def get_draftkings_player_props(
        urls: List[str], 
        chromedriver_path: str, 
        sleep_secs_range: list):
    # Instantiate headless Chrome browser
    browser = get_selenium_chrome_browser(chromedriver_path)
    logger.info("Headless Selenium Chrome browser created.")

    dfs = []
    for url in urls:
        # Randomize sleep time
        min_sleep_secs = sleep_secs_range[0]
        max_sleep_secs = sleep_secs_range[1]
        sleep_secs = np.random.randint(min_sleep_secs, max_sleep_secs)

        # Connect to the website
        browser.get(url)
        logger.info(f"Connected to {url}")

        # Create BeautifulSoup object from html
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        logger.info("Scraping data...")
        
        # Lists to store extracted data
        player_names = []
        over_lines = []
        over_odds = []
        under_lines = []
        under_odds = []

        # Select the main div containing the desired data in the new HTML content
        main_div = soup.select_one('div[aria-labelledby="game_category_Player Stats"].sportsbook-responsive-card-container__card.selected')

        # Directly extracting all player names, over/under lines, and odds from the new main div
        all_player_names = main_div.select("a.sportsbook-event-accordion__title")
        all_game_props_cells = main_div.select("li.game-props-card17__cell.double")

        # Iterating through the new player names and game props cells in pairs
        for i, player_name_element in enumerate(all_player_names):
            # Extract player name
            player_name = player_name_element.text
            player_names.append(player_name)
            
            # Extract "Over" line and odds
            over_line_element = all_game_props_cells[2*i].select_one("span.sportsbook-outcome-cell__label")
            over_od_element = all_game_props_cells[2*i].select_one("span.sportsbook-odds.american.default-color")
            
            # Extract "Under" line and odds
            under_line_element = all_game_props_cells[2*i+1].select_one("span.sportsbook-outcome-cell__label")
            under_od_element = all_game_props_cells[2*i+1].select_one("span.sportsbook-odds.american.default-color")
            
            over_lines.append(over_line_element.text if over_line_element else None)
            over_odds.append(over_od_element.text if over_od_element else None)
            under_lines.append(under_line_element.text if under_line_element else None)
            under_odds.append(under_od_element.text if under_od_element else None)

        # Extract the player prop stat name from the url
        player_prop_stat_name = url.split('=')[-1].replace('-', '_')

        # Create a dataframe from the extracted data
        df = pd.DataFrame({
            "player_name": player_names,
            f"{player_prop_stat_name}_over_line": over_lines,
            f"{player_prop_stat_name}_over_odds": over_odds,
            f"{player_prop_stat_name}_under_line": under_lines,
            f"{player_prop_stat_name}_under_odds": under_odds
        })
        df['timestamp'] = datetime.now()

        # If no data was scraped, continue
        if df.empty:
            logger.info("No data scraped; continuing to next player stat.")
            logger.info(f"Sleeping for {sleep_secs} seconds...")
            time.sleep(sleep_secs)
            continue

        ## Transformations
        # Set index to player_name for future join
        df.set_index('player_name', inplace=True)
        for col in df.columns:
            # Replace unicode minus with ASCII hyphen-minus
            if 'odds' in col:
                logger.info(f"Replacing unicode in {col}...")
                df[col] = df[col].str.replace('−', '-')
            # Remove 'Over ' and 'Under ' from line columns
            if 'line' in col:
                logger.info(f"Replacing Over/Under strings in {col}...")
                df[col] = df[col].str.replace('Over ', '')
                df[col] = df[col].str.replace('Under ', '')
        logger.info(f"Preview of {player_prop_stat_name} data:")
        logger.info(df.head())
        logger.info(df.shape)
        logger.info(df.columns)

        # Append to list of DataFrames
        dfs.append(df)

        # Sleep
        # Refer to website's /robot.txt for appropriate sleep time
        logger.info(f"Sleeping for {sleep_secs} seconds...")
        time.sleep(sleep_secs)
    
    # Combine the dfs by outer joining on player_name
    result_df = pd.concat(dfs, axis=1, join='outer')

    logger.info("Preview of result_df:")
    logger.info(result_df.head())
    logger.info(result_df.shape)
    logger.info(result_df.columns)

    # Save .csv
    result_df.to_csv('dk_player_props.csv')
    logger.info("Data saved to csv.")

    # Close the browser
    browser.quit()
    logger.info("Browser closed.")
