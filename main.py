import logging
import random
import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml_carrefour as xml_c

# Configuración del logger para errores de Python y Selenium
logging.basicConfig(
    filename='scraper_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# User agents
user_agents = xml_c.user_agents

# Invoca csv de listado de productos para actualizar el .csv que tiene los productos
xml_c.csv_productos()


# Scrapea productos y sus datos
def scrape_product_details(url, retries=3):
    for attempt in range(retries):
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
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # Redirigir el log de Chrome a un archivo
        service = ChromeService(ChromeDriverManager().install())
        service.log_path = "chrome_errors.log"  # Archivo donde se guardarán los logs de Chrome
        
        driver = webdriver.Chrome(service=service, options=options)
        
        try:
            driver.get(url)
            # Diferente tipos de precios segun layout
            try:
                price = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div/div[1]/span').text.strip()
                if price == 'BAJADA DE PRECIOS':
                    price = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[2]/div[1]/span').text.strip()
                elif len(price) == 0:
                    price = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[2]/div/div/div[1]/div[1]/span')
            except:
                price = None

            try:
                price_if_discounted = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[1]/div[1]/span[2]').text.strip()
            except:
                price_if_discounted = None
                
            #Si no tiene nombre entonces no existe o no es accesible
            try:
                name = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[1]/div[1]/h1').text.strip()
            except:
                logging.error(f"No encontrado: {url}")
                return None
            
            try:
                category = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/nav/div/div/ol/li[3]/a').text.strip()
            except:
                category = None

            try:
                subcategory = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/nav/div/div/ol/li[4]/a').text.strip()
            except:
                subcategory = None
            
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
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(2)
        finally:
            driver.quit()
    return None

def main():
    with open('output/carrefour-productos.csv', 'r') as csvfile:
        urls_to_scrape = [row['url'] for row in csv.DictReader(csvfile)]

    with open('output/carrefour-product-details.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['url', 'nombre', 'precio', 'precio_descuento', 'categoria', 'subcategoria', 'subsubcategoria', 'imagen']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        max_workers = 10  # Ajuste de hilos para estabilidad
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(scrape_product_details, url): url for url in urls_to_scrape[:50]} #Modificar corchete al final en caso de no querer logear todo, ejemplo: logear 50 [:50]

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result:
                        writer.writerow(result)
                        csvfile.flush()  # Asegura que se guarde después de cada escritura
                        print(f"Scrapeado con éxito: {url}")
                except Exception as e:
                    logging.error(f"No scrapeado {url}: {e}")
    print('Scrapeo terminado.')

if __name__ == "__main__":
    main()
