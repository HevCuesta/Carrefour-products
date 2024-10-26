import logging
import random
import csv
import multiprocessing
from datetime import datetime
from functools import partial
from playwright.sync_api import sync_playwright
import xml_carrefour as xml_c

# User agents
user_agents = xml_c.user_agents

def scrape_product_details(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=random.choice(user_agents),
                ignore_https_errors=True
            )
            page = context.new_page()

            # Aumentar el timeout general
            page.set_default_timeout(10000)

            page.goto(url)
            elements_to_scrape = {
                'price': [
                    '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div/div[1]/span',
                    '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[2]/div[1]/span',
                    '/html/body/div[2]/div/main/div[2]/div[2]/div/div/div[1]/div[1]/span'
                ],
                'price_if_discounted': [
                    '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[1]/div[1]/span[2]'
                ],
                'name': ['/html/body/div[2]/div/main/div[1]/div[1]/h1'],
                'category': ['/html/body/div[2]/div/main/nav/div/div/ol/li[3]/a'],
                'subcategory': ['/html/body/div[2]/div/main/nav/div/div/ol/li[4]/a'],
                'subsubcategoria': ['/html/body/div[2]/div/main/nav/div/div/ol/li[5]/a'],
                'img': [
                    '/html/body/div[2]/div/main/div[1]/div[3]/div/div/div[1]/div/div/div/div/div/div/div/div/div/div/div/div/img[2]',
                    '/html/body/div[2]/div/main/div[1]/div[2]/div/div/div/div[1]/div/div/div/img[1]'
                ],
                'precio_por': [
                    '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div/div[1]/div/span',
                    '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[1]/div[1]/div/span[1]'
                ],
                'precio_por_descuento': [
                    '/html/body/div[2]/div/main/div[2]/div[1]/div/div/div[1]/div[1]/div/span[2]'
                ]
            }

            scraped_data = {}
            for key, xpaths in elements_to_scrape.items():
                for xpath in xpaths:
                    try:
                        element = page.wait_for_selector(f"xpath={xpath}", timeout=5000)
                        if key != 'img':
                            text = element.text_content().strip()
                            if key == 'price' and text == 'BAJADA DE PRECIOS':
                                continue
                            scraped_data[key] = text
                        else:
                            src = element.get_attribute('src')
                            scraped_data[key] = src
                        break
                    except:
                        continue

            browser.close()

            if 'name' not in scraped_data:
                logging.error(f"No encontrado: {url}")
                return None

            result = {
                'url': url,
                'nombre': scraped_data.get('name'),
                'precio': scraped_data.get('price'),
                'precio_descuento': scraped_data.get('price_if_discounted'),
                'categoria': scraped_data.get('category'),
                'subcategoria': scraped_data.get('subcategory'),
                'subsubcategoria': scraped_data.get('subsubcategoria'),
                'imagen': scraped_data.get('img'),
                'precio_por': scraped_data.get('precio_por'),
                'precio_por_descuento': scraped_data.get('precio_por_descuento')
            }
            print(f"Scrapeado con éxito: {url}")
            return result

    except Exception as e:
        logging.error(f"Error procesando {url}: {e}")
        return None

def main():
    # Configuración del logger para errores
    timestamp = datetime.now()
    dt_string = timestamp.strftime("%d_%m_%Y_%H_%M_%S")
    logging.basicConfig(
        filename=f'log/{dt_string}_scraper.log',
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.warning('Scrapeo iniciado.')

    # Leer URLs a scrapear
    with open('output/carrefour-productos.csv', 'r', encoding='utf-8') as csvfile:
        urls_to_scrape = [row['url'] for row in csv.DictReader(csvfile)]

    # Número de procesos
    num_processes = 16  # Ajusta este número según tu sistema

    # Apertura del archivo de salida
    with open('output/carrefour-product-details.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['url', 'nombre', 'precio', 'precio_descuento', 'precio_por', 'precio_por_descuento',
                      'categoria', 'subcategoria', 'subsubcategoria', 'imagen']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Usar Pool de multiprocessing
        with multiprocessing.Pool(processes=num_processes) as pool:
            for result in pool.imap_unordered(scrape_product_details, urls_to_scrape):
                if result:
                    writer.writerow(result)

    logging.warning('Scrapeo terminado.')
    print('Scrapeo terminado.')

if __name__ == "__main__":
    # Invoca csv de listado de productos para actualizar el .csv que tiene los productos
    xml_c.csv_productos()
    main()
