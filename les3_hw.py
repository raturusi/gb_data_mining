import requests
from bs4 import BeautifulSoup as bs
from urllib import parse
import time
from datetime import datetime as dt
from pymongo import MongoClient as client
from db_les3.db import Database

class hw3:


    def __init__(self, start_url: str, db: Database, dbaddress: str):
        self.page = 1
        self.db = Database
        self.start_url = start_url
        #self.dbcollection = client(dbaddress)['GB']['posts']
        #self.dbcollection.drop()

    def _get_response(self, url):
        while True:
            response = requests.get(url)
            if response.status_code >= 200 & response.status_code < 400:
                return response
            time.sleep(0.5)

    def _get_soup(self, url):
         soup = bs(self._get_response(url).text, "lxml")
         return soup

    def _get_parse(self, url):
        posts = self._get_soup(url)
        return posts.find_all("a", attrs={"class": "post-item__title"})

    def _get_datetime(self, d):
        if  d != None:
            return dt.strptime(d[:10], "%Y-%m-%d")
        else:
            return d

    def _get_comments(self, url, id_comment) -> list:
        response = self._get_response(parse.urljoin(url, f"/api/v2/comments?commentable_type=Post&commentable_id={id_comment}&order=desc"))
        return response.json()

    @staticmethod
    def isfind(f):
        return f.attrs.get("src") if f is not None else None

    def _get_post(self, url) -> dict:
        post = self._get_soup(url)
        dict = {
            "posts": {"title": post.find("h1", attrs={"class": "blogpost-title"}).text,
        "url": url,
        "imgurl": self.isfind(post.find("div", attrs={"class": "blogpost-content"}).find("img")),
        "datepost": self._get_datetime(post.find("div", attrs={"class": "blogpost-date-views"}).find("time").attrs.get("datetime"))
                },
            "writer": {"author": post.find("div", attrs={"itemprop": "author"}).text,
                       "url": parse.urljoin(url, post.find("div", attrs={"itemprop": "author"}).parent.attrs.get("href"))
            },
            "tags": [{"tag": tag.text, "urltag": parse.urljoin(url, tag.attrs.get("href"))} for tag in post.find_all("a", attrs={"class": "small"})],
            "comments": self._get_comments(url, post.find("comments", attrs={"commentable-type": "Post"}).attrs.get("commentable-id"))
                }

        return dict

    def _upd_db(self, dict):
        self.dbcollection.insert_one(dict)

    def run(self):
        while True:
            posts = self._get_parse(self.start_url + str(self.page))
            if len(posts) == 0:
                break
            for post in posts:
                data = self._get_post(parse.urljoin(self.start_url, post["href"]))
                #self._upd_db(data)
                db.create_post(data)
            self.page += 1

if __name__ == "__main__":
    start_url = "https://geekbrains.ru/posts?page="
    dbaddress = 'mongodb://127.0.0.1:27017'
    database = "sqlite:///hw3.db"
db = Database(database)
hw = hw3(start_url, db, dbaddress)
hw.run()




