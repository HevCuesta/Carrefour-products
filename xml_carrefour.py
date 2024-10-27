import random
from io import StringIO
from lxml import etree
from playwright.sync_api import sync_playwright
import csv

# Agentes de usuario
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/107.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Mobile Safari/537.36"
]

# Función para extraer y parsear XML desde una URL
def fetch_xml_content(url, context):
    page = context.new_page()
    page.goto(url, wait_until='networkidle')
    content = page.content()
    page.close()
    return content

# Función principal para extraer URLs de productos y fechas, y actualizar CSV
def actualizar_csv_productos():
    urls = [
        "https://www.carrefour.es/sitemap/food/products/detail0_food-00000-of-00002.xml",
        "https://www.carrefour.es/sitemap/food/products/detail0_food-00001-of-00002.xml"
    ]

    # Configuración del navegador
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(user_agent=random.choice(user_agents), ignore_https_errors=True)

        # Extraer datos y guardarlos en el archivo CSV
        with open('output/carrefour-productos.csv', mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['url', 'lastmod'])  # Encabezados del CSV

            for url in urls:
                print(f"Extrayendo datos de: {url}")
                xml_content = fetch_xml_content(url, context)

                # Parsear el XML
                tree = etree.parse(StringIO(xml_content))
                ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls_extracted = tree.xpath('//ns:url/ns:loc/text()', namespaces=ns)
                lastmod_dates_extracted = tree.xpath('//ns:url/ns:lastmod/text()', namespaces=ns)

                # Escribir las filas en el CSV
                writer.writerows(zip(urls_extracted, lastmod_dates_extracted))

        browser.close()
