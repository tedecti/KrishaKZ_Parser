from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import codecs
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def fetch_data(url):
    try:
        response = session.get(url, timeout=5)
        response.raise_for_status()

        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch data from {url}. Status Code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")


def process_item(item_id, overall_id):
    url_item = f"https://krisha.kz/a/show/{item_id}"
    response_item_text = fetch_data(url_item)

    if response_item_text:
        soup_item = BeautifulSoup(response_item_text, 'html.parser')
        name = soup_item.find('div', {'class': 'offer__advert-title'}).find('h1').get_text(strip=True)
        price_element = soup_item.find('div', {'class': 'offer__price'})
        price_element = soup_item.find('div', {'class': 'offer__price'})
        description_element = soup_item.find('div', {'class': 'text'})
        city = soup_item.find('div', {'class': 'offer__location offer__advert-short-info'}).get_text(
            strip=True).split(", ")
        year_of_building_element = soup_item.find('div', {'data-name': 'house.year'})
        floor_element = soup_item.find('div', {'data-name': 'flat.floor'})
        square_element = soup_item.find('div', {'data-name': 'live.square'})
        renovation_element = soup_item.find('div', {'data-name': 'flat.renovation'})

        if price_element:
            price = price_element.get_text(strip=True).replace("\xa0", " ")
        else:
            price = None

        if description_element:
            description = description_element.get_text(strip=True).replace("\n", " ")
        else:
            description = None

        if year_of_building_element:
            year_of_building = year_of_building_element.find('div', {
                'class': 'offer__advert-short-info'}).get_text(strip=True)
        else:
            year_of_building = None

        if floor_element:
            floor = floor_element.find('div', {
                'class': 'offer__advert-short-info'}).get_text(strip=True)
        else:
            floor = None

        if square_element:
            square = square_element.find('div', {
                'class': 'offer__advert-short-info'}).get_text(strip=True).split(', ')
        else:
            square = None

        if renovation_element:
            renovation = renovation_element.find('div', {
                'class': 'offer__advert-short-info'}).get_text(strip=True)
        else:
            renovation = None

        return {
            "id": overall_id,
            "app_id": item_id,
            "name": name,
            "price": price,
            "description": description,
            "city": city[0],
            "year_of_building": year_of_building,
            "floor": floor,
            "square": square[0],
            "renovation": renovation
        }


def process_page(page, overall_id):
    try:
        url_list = base_url.format(page)
        response_list_text = fetch_data(url_list)

        if response_list_text:
            soup_list = BeautifulSoup(response_list_text, 'html.parser')
            div_elements_with_data_id = soup_list.find_all('div', {'data-id': True})
            id_list = [div_element['data-id'] for div_element in div_elements_with_data_id]

            with ThreadPoolExecutor() as executor:
                results = list(executor.map(lambda item_id: process_item(item_id, overall_id), id_list))

            return results

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")


def main():
    overall_id = 1
    data_list = []
    page = 1

    while page < 3:
        results = process_page(page, overall_id)

        if results:
            data_list.extend(results)
            overall_id += 1  # Update overall_id based on the number of processed items
            page += 1
            print(overall_id)
        else:
            break

    with open('output.json', 'w', encoding='utf-8') as json_file:
        json.dump(data_list, json_file, ensure_ascii=False, indent=2)

    with open('output.json', 'r', encoding='utf-8') as json_file_read:
        data = json.load(json_file_read)

    json_string = json.dumps(data, indent=2, ensure_ascii=False)

    with codecs.open("output.txt", "w", encoding="utf-8") as txt_file:
        txt_file.write(json_string)

    df = pd.read_json('output.json')
    df.to_csv("output.csv", index=False)


if __name__ == "__main__":
    base_url = "https://krisha.kz/prodazha/kvartiry/astana/?page={}"
    data_list = []

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    main()
