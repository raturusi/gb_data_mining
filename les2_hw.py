import datetime
import requests
import pymongo
from  pymongo import MongoClient as client
from bs4 import BeautifulSoup as bs
from urllib import parse
import regex as re
from  datetime import datetime as dt

class Magna:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"
    }
    m = ["start", "января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    def __init__(self, url_start: str, dbaddress: str):
        self.url_start = url_start
        self.dbcollection = client(dbaddress)['magna']['promo']
        self.dbcollection.drop()

    def _get_response(self, url):
        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response
            time.sleep(0.5)

    @staticmethod
    def isfind(f):
        return f.string if f is not None else None

    @staticmethod
    def getprice(price_find):
        try:
            price_i = (price_find.contents[1].string if price_find is not None else "0")
            price_d = (price_find.contents[3].string if price_find is not None else "0")
            price = (float(price_i) if price_i.isdigit() else 0) + (float(price_d)/10**len(price_d) if price_d.isdigit() else 0)
        except IndexError:
            price = 0
        return price if price !=0 else None

    def getdate(self, date_find):
        try:
            d = (date_find.contents[1].string.split() if date_find is not None else "0")
            d_from = (dt.strptime(d[1] + str(self.m.index(d[2])) + str(dt.now().year), "%d%m%Y") if d !="0" else "0")
            d = (date_find.contents[3].string.split() if date_find is not None else "0")
            d_to = (dt.strptime(d[1] + str(self.m.index(d[2])) + str(dt.now().year), "%d%m%Y") if d !="0" else "0")
        except Exception:
            d_from, d_to = None, None
        return d_from, d_to

    def run(self):
        for product in self._parse(self.url_start):
            dict = {}
            dict["url"] = parse.urljoin(self.url_start, product["href"])
            dict["promo_name"] = self.isfind(product.find("div", attrs={"class": "card-sale__header"}))
            dict["product_name"] = self.isfind(product.find("div", attrs={"class": "card-sale__title"}))
            dict["price_old"] = self.getprice(product.find("div", attrs={"class": "label__price label__price_old"}))
            dict["price_new"] = self.getprice(product.find("div", attrs={"class": "label__price label__price_new"}))
            dict["image_url"] = parse.urljoin(self.url_start, product.find("img", attrs={"data-src": re.compile("upload")})["data-src"])
            dict["date_from"], dict["date_to"] = self.getdate(product.find("div", attrs={"class": "card-sale__date"}))
            self._save(dict)

    def _parse(self, url):
        response = self._get_response(url)
        soup = bs(response.text, "lxml")
        content_promo = soup.find("div", attrs={"class": "сatalogue__main"})
        products = content_promo.find_all("a", attrs={"class": "card-sale"})
        for product in products:
             yield product

    def _save(self, data: dict):
        self.dbcollection.insert_one(data)

if __name__ == "__main__":
    url_promo = "https://magnit.ru/promo/"
    dbaddress = 'mongodb://127.0.0.1:27017'

parser_magna = Magna(url_promo, dbaddress)
parser_magna.run()