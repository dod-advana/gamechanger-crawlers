import typing
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import re

covid_re = re.compile(r'covid|covid\-19|coronavirus', flags=re.IGNORECASE)


class DODCoronavirusSpider(GCSpider):
    name = "DOD_Coronavirus_Guidance"
    start_urls = [
        "https://www.defense.gov/Explore/Spotlight/Coronavirus/Latest-DOD-Guidance/"
    ]
    cac_login_required = False
    doc_type = "DoD Coronavirus Guidance"

    def get_downloadable_item(self, href, base_url=None) -> dict:
        if base_url is None:
            base_url = self.start_urls[0]

        file_type = self.get_href_file_extension(href)
        web_url = self.ensure_full_href_url(href, base_url)
        return {
            "doc_type": file_type,
            "web_url": web_url.replace(' ', '%20'),
            "compression_type": None
        }

    def parse(self, response):

        blocks = response.css('div.dgov-grid div.block')

        for block in blocks:
            category_text = self.ascii_clean(block.css('h2.cat::text').get())
            items = block.css('div.stories div.item')

            for item in items:
                doc_title_raw = item.css('a.title::text').get()
                doc_title = self.ascii_clean(doc_title_raw)

                href_raw = item.css('a.title::attr(href)').get()
                web_url = self.ensure_full_href_url(
                    href_raw, self.start_urls[0])

                (file_type, has_ext) = self.get_href_file_extension_does_exist(
                    href_raw)

                publication_date_raw = item.css('p.date::text').get()
                if publication_date_raw:
                    publication_date = publication_date_raw.strip()
                else:
                    publication_date = publication_date_raw

                noted = " ".join(item.css('*.noted *::text').getall())
                supp_downloadable_items = []
                # add pub date to repeatedly issued listing
                if noted:
                    doc_title = f"{doc_title} - {publication_date}"
                    supplamental_hrefs = item.css(
                        '*.noted a::attr(href)').getall()
                    supp_downloadable_items = [
                        self.get_downloadable_item(href) for href in supplamental_hrefs
                    ]

                version_hash_fields = {
                    "publication_date": publication_date,
                    "noted": noted
                }

                doc_name = f"{category_text}: {doc_title}"
                item = DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    publication_date=publication_date,
                    version_hash_raw_data=version_hash_fields,
                )

                # some are downloadable items straight from start url
                if has_ext:
                    item["downloadable_items"] = [
                        {
                            "doc_type": file_type,
                            "web_url": web_url.replace(' ', '%20'),
                            "compression_type": None
                        }
                    ]
                    item["downloadable_items"] + supp_downloadable_items

                    item["version_hash_raw_data"].update({
                        "item_currency": item["downloadable_items"][0]["web_url"],
                    })

                    yield item

                # some are links to other websites
                else:
                    yield response.follow(href_raw, callback=self.parse_follow_page, meta={"item": item, "supp_downloadable_items": supp_downloadable_items})

    @staticmethod
    def get_body_hrefs(response) -> list:
        return list(set(response.css('div.body a::attr(href)').getall()))

    @staticmethod
    def get_unknown_layout_hrefs(response) -> list:
        anchors = response.css('a')
        covid_hrefs = [a.css("::attr(href)").get() for a in anchors if covid_re.search(
            " ".join(a.css('*::text').getall()))]
        return covid_hrefs

    def parse_follow_page(self, response) -> typing.Union[DocItem, None]:
        doc_item: DocItem = response.meta["item"]
        supp_downloadable_items = response.meta["supp_downloadable_items"]
        doc_item["downloadable_items"] = []

        href_finder = self.get_body_hrefs if len(
            response.css('div.body')) else self.get_unknown_layout_hrefs

        hrefs = self.filter_mailto_hrefs(href_finder(response))

        for href in hrefs:
            (file_type, has_ext) = self.get_href_file_extension_does_exist(href)
            web_url = self.ensure_full_href_url(href, self.start_urls[0])
            if has_ext:
                doc_item["downloadable_items"].append(
                    {
                        "doc_type": file_type,
                        "web_url": web_url.replace(' ', '%20'),
                        "compression_type": None
                    }
                )

        # page had no downloadable items use the page as the item
        if not len(doc_item["downloadable_items"]):
            doc_item["downloadable_items"] = [
                {
                    "doc_type": 'html',
                    "web_url": response.url.replace(' ', '%20'),
                    "compression_type": None
                }
            ]

            doc_item["downloadable_items"] + supp_downloadable_items

        doc_item["version_hash_raw_data"].update({
            "item_currency": doc_item["downloadable_items"][0]["web_url"],
        })

        yield doc_item
