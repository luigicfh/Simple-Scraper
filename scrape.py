from bs4 import BeautifulSoup, element
import requests
import json
from google.cloud import compute_v1, storage
import google.cloud.logging
from google.cloud.exceptions import NotFound
import logging
import traceback
import os
import datetime

BASE_URL = 'https://books.toscrape.com'
PAGE_URL = BASE_URL + '/catalogue/page-{}.html'
LOG_MSG = "Scraper logs: "


def setup_env_variables():
    with open('config.json') as file:
        to_dict = json.loads(file.read())
    os.environ['PROJECT_ID'] = to_dict['project_id']
    os.environ['ZONE'] = to_dict['zone']
    os.environ['INSTANCE_NAME'] = to_dict['instance_name']
    os.environ['BUCKET'] = to_dict['bucket']
    os.environ['FOLDER'] = to_dict['folder']


def get_products() -> list[element.ResultSet]:
    product_list = []
    page = 1
    while True:
        response = requests.get(url=PAGE_URL.format(page))
        if response.status_code != 200:
            break
        soup = BeautifulSoup(response.content, 'html.parser')
        products = soup.find_all('article', {'class': 'product_pod'})
        product_list.append(products)
        page += 1
    return product_list


def setup_logging():
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()


def delete_instance():
    gce_client = compute_v1.InstancesClient()
    try:
        gce_client.delete(
            project=os.environ.get('PROJECT_ID'),
            zone=os.environ.get('ZONE'),
            instance=os.environ.get('INSTANCE_NAME')
        )
    except NotFound:
        pass


def upload_to_gcs(data: str) -> None:
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(os.environ.get("BUCKET"))
    blob = bucket.blob(f"{os.environ.get('FOLDER')}/results.json")
    blob._chunk_size = 8388608  # 1024 * 1024 B * 16 = 8 MB
    blob.upload_from_string(data=data, content_type="application/json")


def handle_exception():
    logging.error(LOG_MSG + traceback.format_exc())
    delete_instance()


def scrape() -> None:
    logging.info(
        LOG_MSG + f"scraping started {datetime.datetime.now().isoformat()}")
    book_products = get_products()
    d = {}
    data = []
    logging.info(
        LOG_MSG + f"transforming data {datetime.datetime.now().isoformat()}")
    for product in book_products:
        d['book_title'] = product[0].find('h3').find('a')['title']
        d['image'] = BASE_URL + product[0].find(attrs={'class': 'image_container'}).find(
            'a').find('img')['src'].replace("..", '')
        d['price'] = product[0].find(attrs={'class': 'product_price'}).find(
            attrs={'class': 'price_color'}).get_text()
        d['in_stock'] = product[0].find(
            attrs={'class': 'instock availability'}).get_text().replace('\n', '').strip()
        d['rating'] = product[0].find(attrs={'class': 'star-rating'})[
            'class'][-1]
        data.append(d)
        d = {}
    to_json = json.dumps(data, indent=4)
    logging.info(
        LOG_MSG + f"uploading file {datetime.datetime.now().isoformat()}")
    upload_to_gcs(to_json)
    logging.info(
        LOG_MSG + f"scraping process finished, deleting instance {datetime.datetime.now().isoformat()}")
    delete_instance()


if __name__ == '__main__':
    setup_env_variables()
    setup_logging()
    try:
        scrape()
    except Exception:
        handle_exception()
