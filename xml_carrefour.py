import random
import time
import pandas as pd
from io import StringIO
from lxml import etree
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Agentes
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/107.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Mobile Safari/537.36"
]


    # Function to scrape XML content from a URL
def scrape_xml(url):
        # Setup Chrome options
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        options.add_argument("--window-position=-2400,-2400")
        options.add_argument("--start-minimized")
        options.add_argument("--headless=new")
        options.add_argument("--disable-extensions")
        options.add_argument("--ignore-certificate-errors")

        # Start Chrome session
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

        # Visit the page
        driver.get(url)
        time.sleep(2)  # Wait for the page to load completely

        # Get the page source
        xml_content = driver.page_source

        # Close the browser
        driver.quit()
        
        return xml_content

    # URLs to scrape
def csv_productos():
        urls = [
            "https://www.carrefour.es/sitemap/food/products/detail0_food-00000-of-00002.xml",
            "https://www.carrefour.es/sitemap/food/products/detail0_food-00001-of-00002.xml"
        ]

        # Data storage
        all_urls = []
        all_lastmod_dates = []

        # Scrape each URL and parse the XML
        for url in urls:
            print("Leyendo listado productos....")
            xml_content = scrape_xml(url)
            
            # Wrap the XML content in StringIO for parsing
            xml_buffer = StringIO(xml_content)
            
            # Parse the XML using lxml
            tree = etree.parse(xml_buffer)
            
            # Extract 'loc' and 'lastmod' from the XML using the correct namespace
            urls_extracted = tree.xpath('//ns:url/ns:loc/text()', namespaces={'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'})
            lastmod_dates_extracted = tree.xpath('//ns:url/ns:lastmod/text()', namespaces={'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'})
            
            # Append extracted data to the lists
            all_urls.extend(urls_extracted)
            all_lastmod_dates.extend(lastmod_dates_extracted)

        # Create a DataFrame with the combined extracted data
        df = pd.DataFrame({
            'url': all_urls,
            'lastmod': all_lastmod_dates
        })

        df.to_csv('output/carrefour-productos.csv', index=False)