from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests
from bs4 import BeautifulSoup
import pandas as pd

################################################################################
# This is working GPT-4-generated code to extract data from the html.
# I still need to get selenium working so that I don't have to download the
# html manually
# 
# I think my approach will be to do once daily scrapes so I can plot changes.
# This will allow me to ask questions about players who moved before drafting.
################################################################################

"""
# Read the uploaded HTML file
## Only used if we saved the HTML file locally
with open("/mnt/data/dk_rec_td_html.txt", "r") as file:
    html_content = file.read()
"""

# Set up the Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # to run chrome in the background
chrome_options.add_argument("--disable-gpu")  # if you're running on a Windows machine
chrome_options.add_argument("--window-size=1920x1080")  # optional


# Point to the location of chromedriver.exe if it's not in your PATH
driver_path = '/usr/bin/chromedriver'  # Change this to the path where chromedriver is located
browser = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

# Connect to the website
url = "https://sportsbook.draftkings.com/leagues/football/nfl?category=player-stats&subcategory=rec-yards"
browser.get(url)

# Let's use BeautifulSoup to parse the page source returned by Selenium
soup = BeautifulSoup(browser.page_source, 'html.parser')

# Load the content of the new HTML file
with open("/mnt/data/dk_rec_yds_html.txt", "r") as file:
    new_html_content = file.read()

# Parse the new HTML content using BeautifulSoup
new_soup = BeautifulSoup(new_html_content, 'html.parser')

# Lists to store extracted data
new_player_names = []
new_over_lines = []
new_over_odds = []
new_under_lines = []
new_under_odds = []

# Select the main div containing the desired data in the new HTML content
new_main_div = new_soup.select_one('div[aria-labelledby="game_category_Player Stats"].sportsbook-responsive-card-container__card.selected')

# Directly extracting all player names, over/under lines, and odds from the new main div
new_all_player_names = new_main_div.select("a.sportsbook-event-accordion__title")
new_all_game_props_cells = new_main_div.select("li.game-props-card17__cell.double")

# Iterating through the new player names and game props cells in pairs
for i, player_name_element in enumerate(new_all_player_names):
    # Extract player name
    player_name = player_name_element.text
    new_player_names.append(player_name)
    
    # Extract "Over" line and odds
    over_line_element = new_all_game_props_cells[2*i].select_one("span.sportsbook-outcome-cell__label")
    over_od_element = new_all_game_props_cells[2*i].select_one("span.sportsbook-odds.american.default-color")
    
    # Extract "Under" line and odds
    under_line_element = new_all_game_props_cells[2*i+1].select_one("span.sportsbook-outcome-cell__label")
    under_od_element = new_all_game_props_cells[2*i+1].select_one("span.sportsbook-odds.american.default-color")
    
    new_over_lines.append(over_line_element.text if over_line_element else None)
    new_over_odds.append(over_od_element.text if over_od_element else None)
    new_under_lines.append(under_line_element.text if under_line_element else None)
    new_under_odds.append(under_od_element.text if under_od_element else None)

# Create a dataframe from the extracted data
new_df = pd.DataFrame({
    "Player Name": new_player_names,
    "Over Line": new_over_lines,
    "Over Odds": new_over_odds,
    "Under Line": new_under_lines,
    "Under Odds": new_under_odds
})

new_df.head()
