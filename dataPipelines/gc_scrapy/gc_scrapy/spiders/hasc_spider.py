from calendar import day_abbr
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
import scrapy
import typing as t


class HASCSpider(GCSpider):
    name = "HASC"
    cac_login_required = False

    display_org = "Congress"
    data_source = "House Armed Services Committee Publications"
    source_title = "House Armed Services Committee"

    base_url = "https://armedservices.house.gov"

    start_urls = [base_url]

    randomly_delay_request = True

    def parse(self, _):
        pages_parser_map = [
            (f"{self.base_url}/hearings", self.recursive_parse_hearings),
            # (f"{self.base_url}/legislation",) # setup for future crawlers if needed
        ]

        for page_url, parser_func in pages_parser_map:
            yield scrapy.Request(page_url, callback=parser_func)

    @staticmethod
    def get_next_relative_url(response):
        return response.css('div#copy-content div.navbar-inner a:nth-child(3):not(.disabled)::attr(href)').get()

    def recursive_parse_hearings(self, response):

        yield from self.parse_hearings_table_page(response)

        next_relative_url = self.get_next_relative_url(response)
        if next_relative_url:
            next_url = f"{self.base_url}{next_relative_url}"
            yield scrapy.Request(url=next_url, callback=self.recursive_parse_hearings)

    def parse_hearings_table_page(self, response):

        rows = response.css(
            "div#copy-content div.recordsContainer table tbody tr")

        for row in rows:
            try:
                link = row.css("td:nth-child(2) a::attr(href)").get()

                if not link:
                    continue

                follow_link = f"{self.base_url}{link}"
                yield scrapy.Request(url=follow_link, callback=self.parse_hearing_detail_page)
            except Exception as e:
                print(f'Error following url: {follow_link}', e)

    def parse_hearing_detail_page(self, response):
        try:

            header = response.css(
                "div#copy-content div.header")
            title = self.ascii_clean(header.css("h1 a::text").get())

            date_el = header.css("span.date")
            month = date_el.css('span.month::text').get()
            day = date_el.css('span.day::text').get()
            year = date_el.css('span.year::text').get()

            date = f"{month} {day}, {year}"

            permalink = response.css(
                'div#copy-content div.foot p.permalink a::attr(href)').get()

            downloadable_items = [
                {
                    "doc_type": 'html',
                    "web_url": permalink,
                    "compression_type": None
                }
            ]

            doc_type = "HASC Hearing"
            doc_name = f"{doc_type} - {date} - {title}"

            version_hash_raw_data = {
                'item_currency': permalink,
            }

            yield DocItem(
                doc_name=doc_name,
                doc_title=title,
                doc_type=doc_type,
                display_doc_type="Hearing",
                publication_date=date,
                source_page_url=permalink,
                version_hash_raw_data=version_hash_raw_data,
                downloadable_items=downloadable_items
            )

        except Exception as e:
            print(e)
