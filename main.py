import logging
import random
import time
import csv
import threading

from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml_carrefour as xml_c

# Creación de un bloqueo para asegurar la escritura sincronizada
lock = threading.Lock()

timestamp = datetime.now()
dt_string = timestamp.strftime("%d_%m_%Y_%H_%M_%S")
# Configuración del logger para errores de Python y Selenium
logging.basicConfig(
    filename='log/' + dt_string + 'scraper.log',
    level=logging.WARN,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# User agents
user_agents = xml_c.user_agents

# Pre-instalar el driver una sola vez
driver_path = ChromeDriverManager().install()

# Scrapea productos y sus datos
def scrape_product_details(url, retries=1, timeout=5):
    for attempt in range(retries):
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        #Headless no funciona bien asi que se añaden estos tres argumentos, ver actual 129 de chromedriver, si se actualiza verificar si funciona
        options.add_argument("--window-position=-2400,-2400")
        options.add_argument("--start-minimized")
        options.add_argument("--headless=new")
        
        options.add_argument("--disable-extensions")
        options.add_argument("--ignore-certificate-errors")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # Usar el driver_path preinstalado
        service = ChromeService(executable_path=driver_path)
        service.log_path = 'log/' + dt_string + "chrome_errors.log"
        
        driver = webdriver.Chrome(service=service, options=options)
        
        try:
            driver.get(url)
            
            # Diccionario de elementos con sus nombres y XPATHs posibles (hay algunos elementos que puede que no funcionen idk)
            elements_to_scrape = {
                'price': ['/html/body/div[2]/div/main/div[2]/div[1]/div/div/div/div[1]/span',
                          '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[2]/div[1]/span',
                          '/html/body/div[2]/div/main/div[2]/div[2]/div/div/div[1]/div[1]/span'],
                'price_if_discounted': ['/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[1]/div[1]/span[2]'],
                'name': ['/html/body/div[2]/div/main/div[1]/div[1]/h1'],
                'category': ['/html/body/div[2]/div/main/nav/div/div/ol/li[3]/a'],
                'subcategory': ['/html/body/div[2]/div/main/nav/div/div/ol/li[4]/a'],
                'subsubcategoria': ['/html/body/div[2]/div/main/nav/div/div/ol/li[5]/a'],
                'img': ['/html/body/div[2]/div/main/div[1]/div[3]/div/div/div[1]/div/div/div/div/div/div/div/div/div/div/div/div/img[2]',
                        '/html/body/div[2]/div/main/div[1]/div[2]/div/div/div/div[1]/div/div/div/img[1]']
            }

            scraped_data = {}
            for key, xpaths in elements_to_scrape.items():
                for xpath in xpaths:
                    try:
                        # Usar WebDriverWait para limitar el tiempo de espera
                        element = WebDriverWait(driver, timeout).until(
                            EC.presence_of_element_located((By.XPATH, xpath))
                        )
                        if key != 'img':
                            text = element.text.strip()
                            if key == 'price' and text == 'BAJADA DE PRECIOS':
                                continue  # Intentar el siguiente XPATH si el texto es "BAJADA DE PRECIOS"
                            scraped_data[key] = text
                        else:
                            scraped_data[key] = element.get_attribute('src')
                        break  # Salir del bucle si el elemento fue encontrado
                    except:
                        continue  # Intentar el siguiente XPATH si no se encontró el elemento
            
            if 'name' not in scraped_data:
                logging.error(f"No encontrado: {url}")
                return None

            return {
                'url': url,
                'nombre': scraped_data.get('name', None),
                'precio': scraped_data.get('price', None),
                'precio_descuento': scraped_data.get('price_if_discounted', None),
                'categoria': scraped_data.get('category', None),
                'subcategoria': scraped_data.get('subcategory', None),
                'subsubcategoria': scraped_data.get('subsubcategoria', None),
                'imagen': scraped_data.get('img', None)
            }

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(1)
        finally:
            driver.quit()
    return None

def main():
    logging.warning('Scrapeo iniciado.')
    
    # Lectura de las URLs a scrapear
    with open('output/carrefour-productos.csv', 'r') as csvfile:
        urls_to_scrape = [row['url'] for row in csv.DictReader(csvfile)]

    # Apertura del archivo de salida
    with open('output/carrefour-product-details.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['url', 'nombre', 'precio', 'precio_descuento', 'categoria', 'subcategoria', 'subsubcategoria', 'imagen']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        max_workers = 16  # Ajuste de hilos para estabilidad
        
        # Uso de ThreadPoolExecutor para manejo de concurrencia
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(scrape_product_details, url): url for url in urls_to_scrape[:100]}  # Modificar el límite de URLs aquí si es necesario

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result:
                        # Asegurar que solo un hilo escriba al CSV a la vez
                        with lock:
                            writer.writerow(result)
                            csvfile.flush()  # Asegura que se guarde después de cada escritura
                        print(f"Scrapeado con éxito: {url}")
                except Exception as e:
                    logging.error(f"No scrapeado {url}: {e}")

    logging.warning('Scrapeo terminado.')
    print('Scrapeo terminado.')

if __name__ == "__main__":
    # Invoca csv de listado de productos para actualizar el .csv que tiene los productos
    xml_c.csv_productos()
    main()
