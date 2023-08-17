from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import requests
from bs4 import BeautifulSoup

def get_selenium_chrome_browser(chromedriver_path):
    """Returns a headless Selenium Chromedriver instance."""
    # Point to the location of chromedriver.exe if it's not in your PATH
    #driver_path = '/usr/bin/chromedriver'  # Change this to the path where chromedriver is located
    # Selenium 4.10.0 -- GPT does not know how to set up the browser/driver
    service = Service(executable_path=chromedriver_path)
    # Set up the Chrome options
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  # to run chrome in the background
    chrome_options.add_argument("--disable-gpu")  # if you're running on a Windows machine
    chrome_options.add_argument("--window-size=1920x1080")  # optional
    
    return webdriver.Chrome(service=service, options=chrome_options)
    