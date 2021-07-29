import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import time
from dataPipelines.gc_scrapy.gc_scrapy.utils import abs_url


class ArmySpider(GCSpider):
    """
        Parser for Army Publications
    """

    name = "army_pubs"
    allowed_domains = ['armypubs.army.mil']
    start_urls = [
        'https://armypubs.army.mil/'
    ]

    file_type = "pdf"

    base_url = 'https://armypubs.army.mil'
    pub_url = base_url + '/ProductMaps/PubForm/'

    def parse(self, response):
        do_not_process = ["/ProductMaps/PubForm/PB.aspx",
                          "/Publications/Administrative/POG/AllPogs.aspx"]

        all_hrefs = response.css(
            'li.usa-nav__primary-item')[2].css('a::attr(href)').getall()

        links = [link for link in all_hrefs if link not in do_not_process]

        yield from response.follow_all(links, self.parse_source_page)

    def parse_source_page(self, response):
        table_links = response.css('table td a::attr(href)').extract()
        yield from response.follow_all([self.pub_url+link for link in table_links], self.parse_detail_page)

    def parse_detail_page(self, response):
        rows = response.css('tr')
        doc_name_raw = rows.css('span#MainContent_PubForm_Number::text').get()
        doc_title = rows.css('span#MainContent_PubForm_Title::text').get()
        doc_num_raw = doc_name_raw.split()[-1]
        doc_type_raw = doc_name_raw.split()[0]
        publication_date = rows.css(
            "span#MainContent_PubForm_Date::text").get()
        dist_stm = rows.css("span#MainContent_PubForm_Dist_Rest::text").get()
        if dist_stm and (dist_stm.startswith("A") or dist_stm.startswith("N")):
            # the distribution statement is distribution A or says Not Applicable so anyone can access the information
            cac_login_required = False
        else:
            # the distribution statement has more restrictions
            cac_login_required = True

        linked_items = rows.css("div#MainContent_uoicontainer a")
        downloadable_items = []

        if not linked_items:
            # skip over the publication
            filetype = rows.css("div#MainContent_uoicontainer::text").get()
            if filetype:
                di = {
                    "doc_type": filetype.strip().lower(),
                    "web_url": self.base_url,
                    "compression_type": None
                }
                downloadable_items.append(di)
            else:
                return
        else:
            for item in linked_items:
                di = {
                    "doc_type": item.css("::text").get().strip().lower(),
                    "web_url": abs_url(self.base_url, item.css("::attr(href)").get()).replace(' ', '%20'),
                    "compression_type": None
                }
                downloadable_items.append(di)
        version_hash_fields = {
            "pub_date": publication_date,
            "pub_pin": rows.css("span#MainContent_PubForm_PIN::text").get(),
            "pub_status": rows.css("span#MainContent_PubForm_Status::text").get(),
            "product_status": rows.css("span#MainContent_Product_Status::text").get(),
            "replaced_info": rows.css("span#MainContent_PubForm_Superseded::text").get()
        }

        yield DocItem(
            doc_name=self.ascii_clean(doc_name_raw),
            doc_title=self.ascii_clean(doc_title),
            doc_num=self.ascii_clean(doc_num_raw),
            doc_type=self.ascii_clean(doc_type_raw),
            source_page_url=response.url,
            publication_date=self.ascii_clean(publication_date),
            cac_login_required=cac_login_required,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields,
        )
