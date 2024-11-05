from curl_cffi import requests
from datetime import datetime
import logging
import os
import time
import csv


base_url = 'https://www.carrefour.es/cloud-api/plp-food-papi/v1'

def main():
    # Configuración del logger
    timestamp = datetime.now()
    dt_string = timestamp.strftime("%d_%m_%Y_%H_%M_%S")
    try:
        logging.basicConfig(
            filename=f'log/{dt_string}_scraper.log',
            level=logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    except FileNotFoundError:
        os.makedirs('log')
        logging.basicConfig(
            filename=f'log/{dt_string}_scraper.log',
            level=logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    logging.warning('Scrapeo iniciado.')

    # Leer URLs del archivo CSV y quitar el prefijo si está presente
    with open('output/carrefour-categories.csv', 'r', encoding='utf-8') as csvfile:
        urls_to_scrape = [
            row['url'].replace('https://www.carrefour.es', '') for row in csv.DictReader(csvfile)
        ]
    
    if not urls_to_scrape:
        logging.warning("No se encontraron URLs para scrapear.")
        print("No se encontraron URLs para scrapear.")
        return

    fieldnames = ['id', 'url', 'nombre', 'precio', 'precio_por', 'marca', 'categoria', 'imagen']
    with open('output/carrefour-product-details.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for url in urls_to_scrape:
            print(f"Scrapeando URL: {url}")
            scrape_product_details(url, writer)

    logging.warning('Scrapeo terminado.')
    print('Scrapeo terminado.')

def scrape_product_details(url, writer):
    offset = 0
    while True:
        full_url = f"{base_url}{url}?offset={offset}"
        print(f"Accediendo a: {full_url}")
        
        response = requests.get(full_url, impersonate="safari")
        if response.status_code != 200:
            logging.warning(f"Error al acceder a {full_url}: {response.status_code}")
            print(f"Error al acceder a {full_url}: {response.status_code}")
            break

        data = response.json()
        items = data.get('results', {}).get('items', [])
        
        if not items:
            print("No se encontraron productos en esta página.")
            break  # Salir si no hay más productos

        for item in items:
            product_data = {
                'id': item.get('product_id', ''),
                'url': 'https://www.carrefour.es' + item.get('url', ''),
                'nombre': item.get('name', ''),
                'precio': item.get('price', ''),
                'precio_por': item.get('price_per_unit', '') + '/' + item.get('measure_unit', ''),
                'marca': item.get('brand', {}).get('name', ''),
                'categoria': item.get('catalog', ''),
                'imagen': item.get('images', {}).get('desktop', '')
            }
            writer.writerow(product_data)

        # Incrementar offset y pausar para evitar sobrecarga
        offset += 24
        time.sleep(1)

if __name__ == "__main__":
    main()
