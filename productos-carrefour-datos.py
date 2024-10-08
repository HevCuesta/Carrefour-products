import random
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

# User agents for random selection
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/107.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Mobile Safari/537.36"
]

# Function to scrape product details using Selenium with retry logic
def scrape_product_details(url, retries=3):
    for attempt in range(retries):
        options = Options()
        options.add_argument("--headless")  # Run in headless mode
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={random.choice(user_agents)}")  # Random user agent for each attempt

        options.add_argument("--window-size=1920x1080")  # Set a larger window size
        options.add_argument("--start-maximized")  # Start maximized
        options.add_argument("--ignore-certificate-errors")  # Ignore SSL certificate errors
        options.add_argument("--incognito")  # Open in incognito mode
        
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        
        try:
            driver.get(url)
            time.sleep(3)  # Shorter wait time to allow the page to load

            # Extract product details
            try:
                price = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div/div[1]/span').text.strip()
            except:
                price = None

            # Check for discounted price
            try:
                price_if_discounted = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[1]/div[1]/span[2]').text.strip()
            except:
                price_if_discounted = None
            
            name = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[1]/div[1]/h1').text.strip()
            category = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/nav/div/div/ol/li[3]/a').text.strip()
            subcategory = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/nav/div/div/ol/li[4]/a').text.strip()
            
            # Check for subsubcategory
            try:
                subsubcategory = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/nav/div/div/ol/li[5]/a').text.strip()
            except:
                subsubcategory = None
            
            try:
                img = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[1]/div[3]/div/div/div[1]/div/div/div/div/div/div/div/div/div/div/div/div/img[2]').get_attribute('src')
            except:
                img = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[1]/div[2]/div/div/div/div[1]/div/div/div/img[1]').get_attribute('src')
            return {
                'url': url,
                'nombre': name,
                'precio': price,
                'precio_descuento': price_if_discounted,
                'categoria': category,
                'subcategoria': subcategory,
                'subsubcategoria': subsubcategory,
                'imagen': img
            }
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(2)  # Wait before retrying
        finally:
            driver.quit()
    return None  # Return None if all attempts fail

# Main function to read CSV and scrape product details using multithreading
def main():
    df_products = pd.read_csv('carrefour-productos.csv')
    
    product_details = []

    # Limit to the first 5 products
    urls_to_scrape = df_products['url'][:50]  # Change to [:5] to scrape the first 5 products

    # Use ThreadPoolExecutor for multithreading
    max_workers = 20  # Use 20 threads to utilize your CPU effectively
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(scrape_product_details, url): url for url in urls_to_scrape}

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                if result is not None:
                    product_details.append(result)
            except Exception as e:
                print(f"Failed to scrape {url}: {e}")

    df_product_details = pd.DataFrame(product_details)
    df_product_details.to_csv('carrefour-product-details.csv', index=False)
    print("Product details saved to carrefour-product-details.csv")

if __name__ == "__main__":
    main()
