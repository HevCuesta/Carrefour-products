import logging
import random
import csv
import re
from datetime import datetime
from playwright.async_api import async_playwright
import asyncio
import xml_carrefour as xml_c

# User agents
user_agents = xml_c.user_agents

async def scrape_product_details(sem, url, browser):
    async with sem:
        context = await browser.new_context(
            user_agent=random.choice(user_agents),
            ignore_https_errors=True
        )
        page = await context.new_page()

        # Increase the general timeout
        page.set_default_timeout(10000)

        try:
            await page.goto(url)
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
                        element = await page.wait_for_selector(f"xpath={xpath}", timeout=5000)
                        if key != 'img':
                            # Obtener el contenido completo del elemento y luego limpiar el formato
                            text = (await element.inner_text()).strip()
                            # Limpia los saltos de línea y agrega un espacio uniforme alrededor del guion
                            text = re.sub(r'\s*-\s*', ' - ', re.sub(r'\s+', ' ', text))
                            if key == 'price' and text == 'BAJADA DE PRECIOS':
                                continue
                            scraped_data[key] = text
                        else:
                            src = await element.get_attribute('src')
                            scraped_data[key] = src
                        break
                    except Exception:
                        continue

            if 'name' not in scraped_data:
                logging.error(f"No encontrado: {url}")
                return None

            result = {
                'url': url,
                'nombre': scraped_data.get('name'),
                'precio': scraped_data.get('price'),
                'precio_descuento': scraped_data.get('price_if_discounted'),
                'precio_por': scraped_data.get('precio_por'),
                'precio_por_descuento': scraped_data.get('precio_por_descuento'),
                'categoria': scraped_data.get('category'),
                'subcategoria': scraped_data.get('subcategory'),
                'subsubcategoria': scraped_data.get('subsubcategoria'),
                'imagen': scraped_data.get('img')
            }
            print(f"Scrapeado con éxito: {url}")
            return result

        except Exception as e:
            logging.error(f"Error procesando {url}: {e}")
            return None
        finally:
            await context.close()

async def main():
    # Logger configuration for errors
    timestamp = datetime.now()
    dt_string = timestamp.strftime("%d_%m_%Y_%H_%M_%S")
    logging.basicConfig(
        filename=f'log/{dt_string}_scraper.log',
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.warning('Scrapeo iniciado.')

    # Read URLs to scrape
    with open('output/carrefour-productos.csv', 'r', encoding='utf-8') as csvfile:
        urls_to_scrape = [row['url'] for row in csv.DictReader(csvfile)]

    # Limit the number of concurrent tasks
    max_concurrent_tasks = 10  # Adjust this number based on your system capabilities
    sem = asyncio.Semaphore(max_concurrent_tasks)

    # Open the output file
    fieldnames = ['url', 'nombre', 'precio', 'precio_descuento', 'precio_por', 'precio_por_descuento',
                  'categoria', 'subcategoria', 'subsubcategoria', 'imagen']
    with open('output/carrefour-product-details.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        async with async_playwright() as playwright:
            # Launch the browser once
            browser = await playwright.chromium.launch(headless=True)

            tasks = []
            for url in urls_to_scrape:
                tasks.append(scrape_product_details(sem, url, browser))

            # Process results as they become available
            for future in asyncio.as_completed(tasks):
                result = await future
                if result:
                    writer.writerow(result)

            # Close the browser after all tasks are done
            await browser.close()

    logging.warning('Scrapeo terminado.')
    print('Scrapeo terminado.')

if __name__ == "__main__":
    #Invoca para obtener las urls de productos actualziadas
    xml_c.actualizar_csv_productos()
    asyncio.run(main())
