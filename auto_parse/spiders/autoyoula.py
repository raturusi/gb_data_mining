import scrapy
from pymongo import MongoClient as client


class AutoyoulaSpider(scrapy.Spider):
    name = 'autoyoula'
    allowed_domains = ['auto.youla.ru']
    start_urls = ['https://auto.youla.ru/']

    _css_selectors = {'brands': "div.TransportMainFilters_brandsList__2tIkv .ColumnItemList_container__5gTrc a.blackLink",
                      "pagination": "Paginator_block__2XAPy a.Paginator_button__u1e7D",
                      "car": "article.SerpSnippet_snippet__3O1t2 .SerpSnippet_titleWrapper__38bZM a.blackLink"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbcollection = client('mongodb://127.0.0.1:27017')['youla']['auto']
        self.dbcollection.drop()

    def _get_follow(self, response, select_str, callback, **kwargs):
        for a in response.css(select_str):
            link = a.attrib.get("href")
            yield response.follow(link, callback=callback, cb_kwargs=kwargs)

    def parse(self, response, *args, **kwargs):
        yield from self._get_follow(response, self._css_selectors["brands"], self.brand_parse)
#        for brand_a in response.css(self._css_selectors["brands"]):
#            link = brand_a.attrib.get("href")
#            yield response.follow(link, callback=self.brand_parse, cb_kwargs={"hello": "MOTO"})


    def brand_parse(self, response, *args, **kwargs):
        yield from self._get_follow(response, self._css_selectors["pagination"], self.brand_parse)
        yield from self._get_follow(response, self._css_selectors["car"], self.car_parse)

        # for brand_a in response.css(self._css_selectors["pagination"]):
        #     link = brand_a.attrib.get("href")
        #     yield response.follow(link, callback=self.brand_parse)
        #
        # for brand_a in response.css(self._css_selectors["car"]):
        #     link = brand_a.attrib.get("href")
        #     yield response.follow(link, callback=self.car_parse)

    def car_parse(self, response):
        dict = {}
        dict["anouncment"] = response.css("div.AdvertCard_advertTitle__1S1Ak::text").get()
        dict["photos"] = [itm.attrib.get("style") for itm in response.css("button.PhotoGallery_thumbnailItem__UmhLO")]
        dict["params"]=[{"name": itm.css(".AdvertSpecs_label__2JHnS::text").extract_first(),
                        "value": itm.css(".AdvertSpecs_data__xK2Qx a::text").extract_first()} for itm in response.css("div.AdvertCard_specs__2FEHc .AdvertSpecs_row__ljPcX")]
        dict["description"]=response.css("div.AdvertCard_descriptionInner__KnuRi::text").extract_first()
        self.dbcollection.insert_one(dict)
