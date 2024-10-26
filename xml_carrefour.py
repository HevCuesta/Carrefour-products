import random
import pandas as pd
from io import StringIO
from lxml import etree
from playwright.sync_api import sync_playwright

# Agentes de usuario
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/107.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Mobile Safari/537.36"
]

# Función para extraer contenido XML de una URL
def scrape_xml(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-position=-2400,-2400",
                "--start-minimized",
                "--disable-extensions",
                "--ignore-certificate-errors"
            ]
        )
        user_agent = random.choice(user_agents)
        context = browser.new_context(
            user_agent=user_agent,
            ignore_https_errors=True
        )
        page = context.new_page()
        page.goto(url, wait_until='networkidle')
        xml_content = page.content()
        browser.close()
    return xml_content

# Función principal para obtener los productos
def csv_productos():
    urls = [
        "https://www.carrefour.es/sitemap/food/products/detail0_food-00000-of-00002.xml",
        "https://www.carrefour.es/sitemap/food/products/detail0_food-00001-of-00002.xml"
    ]

    # Almacenamiento de datos
    all_urls = []
    all_lastmod_dates = []

    # Extraer y parsear el XML de cada URL
    for url in urls:
        print("Leyendo listado de productos....")
        xml_content = scrape_xml(url)
        
        # Envolver el contenido XML en StringIO para parsearlo
        xml_buffer = StringIO(xml_content)
        
        # Parsear el XML usando lxml
        tree = etree.parse(xml_buffer)
        
        # Extraer 'loc' y 'lastmod' del XML usando el namespace correcto
        urls_extracted = tree.xpath('//ns:url/ns:loc/text()', namespaces={'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'})
        lastmod_dates_extracted = tree.xpath('//ns:url/ns:lastmod/text()', namespaces={'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'})
        
        # Añadir los datos extraídos a las listas
        all_urls.extend(urls_extracted)
        all_lastmod_dates.extend(lastmod_dates_extracted)

    # Crear un DataFrame con los datos combinados
    df = pd.DataFrame({
        'url': all_urls,
        'lastmod': all_lastmod_dates
    })

    # Guardar el DataFrame en un archivo CSV
    df.to_csv('output/carrefour-productos.csv', index=False)
