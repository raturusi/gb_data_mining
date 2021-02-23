import time
import json
from pathlib import Path
import requests

class Parse5Ka:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"
    }

    def __init__(self, url_categories: str, url_products: str, products_path: Path):
        self.url_categories = url_categories
        self.url_products = url_products
        self.products_path = products_path

    def _get_response(self, url):
        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response
            time.sleep(0.5)

    def run(self):
        for category in self._parse_categories(self.url_categories):
            category_path = self.products_path.joinpath(f"{category['parent_group_code']}.json")
            dict = {"name": category['parent_group_name'], "code": category['parent_group_code'], "products": []}
            for product in self._parse(self.url_products+category['parent_group_code']):
                dict["products"].append(product)
            self._save(dict, category_path)

    def _parse(self, url):
        while url:
            response = self._get_response(url)
            data = response.json()
            url = data["next"]
            for product in data["results"]:
                yield product

    def _parse_categories(self, url):
        while url:
            response = self._get_response(url)
            data = response.json()
            for category in data:
                yield category


    @staticmethod
    def _save(data: dict, file_path):
        jdata = json.dumps(data, ensure_ascii=False)
        file_path.write_text(jdata, encoding="UTF-8")


if __name__ == "__main__":
    url_categories = "https://5ka.ru/api/v2/categories/"
    url_products = "https://5ka.ru/api/v2/special_offers/?categories="
    save_path = Path(__file__).parent.joinpath("categories")
    if not save_path.exists():
        save_path.mkdir()

parser_categories = Parse5Ka(url_categories, url_products, save_path)
parser_categories.run()