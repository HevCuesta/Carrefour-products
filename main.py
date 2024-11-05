from datetime import datetime
import logging
import os
import time
import csv
import xml_carrefour as xml_c

base_url = 'https://www.carrefour.es/cloud-api/plp-food-papi/v1'


def main():
    # Logger configuration for errors
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

    # URLs a scrapear
    with open('output/carrefour-categories.csv', 'r', encoding='utf-8') as csvfile:
        urls_to_scrape = [row['url'] for row in csv.DictReader(csvfile)]


    # Open the output file
    fieldnames = ['url', 'nombre', 'precio', 'precio_descuento', 'precio_por',
                  'categoria', 'subcategoria', 'subsubcategoria', 'imagen']
    with open('output/carrefour-product-details.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for url in urls_to_scrape:
            scrape_product_details(url, writer)


    logging.warning('Scrapeo terminado.')
    print('Scrapeo terminado.')


def scrape_product_details(url):
    # Scrapea los detalles de un producto
    

if __name__ == "__main__":
    # Invoca para obtener las urls de productos actualizadas
    xml_c.actualizar_csv_productos()
    asyncio.run(main())
