from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup


# Set up the Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # to run chrome in the background
chrome_options.add_argument("--disable-gpu")  # if you're running on a Windows machine
chrome_options.add_argument("--window-size=1920x1080")  # optional


# Point to the location of chromedriver.exe if it's not in your PATH
driver_path = '/usr/bin/chromedriver'  # Change this to the path where chromedriver is located
browser = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

# Connect to the website
url = "https://sportsbook.draftkings.com/leagues/football/nfl?category=player-stats&subcategory=rec-tds"
browser.get(url)

# Let's use BeautifulSoup to parse the page source returned by Selenium
soup = BeautifulSoup(browser.page_source, 'html.parser')

# Now, find the divs
divs = soup.find_all("div", class_="sportsbook-event-accordion__wrapper expanded")

for div in divs:
    # Within each div, find the spans
    spans = div.find_all("span", class_="sportsbook-outcome-cell__label")
    
    for span in spans:
        print(span.text)

# Remember to close the browser once done
browser.quit()
