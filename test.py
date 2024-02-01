from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import codecs
import pandas as pd

page = 1
base_url = "https://krisha.kz/prodazha/kvartiry/astana/?page={}"

data_list = []
overall_id = 1

while page != 50:
    try:
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        url_list = base_url.format(page)
        session = requests.get(url_list, timeout=5)
        session.raise_for_status()

        if session.status_code == 200:
            soup_list = BeautifulSoup(session.text, 'html.parser')
            div_elements_with_data_id = soup_list.find_all('div', {'data-id': True})
            id_list = [div_element['data-id'] for div_element in div_elements_with_data_id]

            for item_id in id_list:
                url_item = f"https://krisha.kz/a/show/{item_id}"
                response_item = requests.get(url_item)
                if response_item.status_code == 200:
                    soup_item = BeautifulSoup(response_item.text, 'html.parser')
                    name = soup_item.find('div', {'class': 'offer__advert-title'}).find('h1').get_text(strip=True)
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

                    data = {"id": overall_id,
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

                    data_list.append(data)
                    overall_id += 1
                    time.sleep(1)
                    print(overall_id)
            page += 1
        else:
            print(session.status_code)
            break
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
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
