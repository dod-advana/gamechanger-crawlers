import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import time


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
        # print(response)
        # these links are not in the proper format to be scraped
        do_not_process = ["/ProductMaps/PubForm/PB.aspx",
                          "/Publications/Administrative/POG/AllPogs.aspx"]

        publications_list = response.css('li.nav-item')[2]

        # links = [link for link in publications_list.css('a.dropdown-item')
        #          if link.css('::attr(href)').extract()[0] not in do_not_process and link.css('::attr(href)').extract()[0] != "#"]
        all_hrefs = response.css(
            'li.nav-item')[2].css('a.dropdown-item::attr(href)').getall()

        links = [link for link in all_hrefs if link not in do_not_process]

        yield from response.follow_all(links, self.parse_source_page)

    def parse_source_page(self, response):
        print('parse_source_page', response.url)
        table_links = response.css('table.gridview a::attr(href)').extract()
        # for link in table_links:
        #     yield scrapy.Request(self.pub_url + link, callback=self.parse_detail_page)
        # for item in self.parse_detail_page(self.pub_url+link):
        #    yield item
        # yield from response.follow_all([self.pub_url+link for link in table_links], self.parse_detail_page)
        # print([self.pub_url+link for link in table_links])
        yield from response.follow_all([self.pub_url+link for link in table_links], self.parse_detail_page)

    def parse_detail_page(self, response):
        print('parse_detail_page', response.url)
        rows = response.css('tr')
        doc_name_raw = rows.css('span#MainContent_PubForm_Number::text').get()
        doc_title = rows.css('span#MainContent_PubForm_Title::text').get()
        doc_num_raw = doc_name_raw.split()[-1]
        doc_type_raw = doc_name_raw.split()[0]
        publication_date = rows.css(
            "span#MainContent_PubForm_Date::text").get()
        dist_stm = rows.css("span#MainContent_PubForm_Sec_Class::text").get()
        if dist_stm.startswith("A") or dist_stm.startswith("N"):
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
            di = {
                "doc_type": filetype.strip().lower(),
                "web_url": "",
                "compression_type": None
            }
            downloadable_items.append(di)
        else:
            for item in linked_items:
                di = {
                    "doc_type": item.css("::text").get().strip().lower(),
                    "web_url": item.css("::attr(href)").get(),
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
            doc_name=doc_name_raw,
            doc_title=doc_title,
            doc_num=doc_num_raw,
            doc_type=doc_type_raw,
            publication_date=publication_date,
            cac_login_required=cac_login_required,
            downloadable_items=downloadable_items,
            version_hash_raw_data=version_hash_fields,
        )
