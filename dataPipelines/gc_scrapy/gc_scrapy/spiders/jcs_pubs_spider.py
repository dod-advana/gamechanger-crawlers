import re
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem

doc_type_num_re = re.compile(r'(.*)\s(\d+.*)')


class JcsPubsSpider(GCSpider):
    name = 'jcs_pubs'

    start_urls = ['https://www.jcs.mil/Library/']
    base_url = 'https://www.jcs.mil'

    cac_required_options = [
        'CAC', 'PKI certificate required', 'placeholder', 'FOUO']

    def parse(self, response):
        doc_links = [
            a for a in response.css('div.librarylinkscontainer a') if 'CJCS' in a.css('::attr(href)').get()
        ]

        for link in doc_links:
            yield response.follow(url=link, callback=self.parse_doc_table_page)

    def parse_doc_table_page(self, response):
        rows = response.css('table#JCSDocsTable tbody tr')
        if not len(rows):
            return

        for row in rows:
            doc_type_num_raw = row.css('td.DocNoCol a::text').get()

            doc_type_num_groups = doc_type_num_re.search(doc_type_num_raw)
            if doc_type_num_groups:
                doc_type_raw = doc_type_num_groups.group(1)
                doc_num_raw = doc_type_num_groups.group(2)
            else:
                print("FAILED TO FIND GROUPS", doc_type_num_raw)
                continue

            doc_type = self.ascii_clean(doc_type_raw)
            doc_num = self.ascii_clean(doc_num_raw)
            href_raw = row.css('td.DocNoCol a::attr(href)').get()
            doc_title = row.css('td.DocTitle::text').get()
            publication_date = row.css('td.DocDateCol::text').get()
            current_of_date = row.css('td.CurrentCol::text').get(default='')

            web_url = self.ensure_full_href_url(href_raw, self.base_url)

            downloadable_items = [
                {
                    "doc_type": 'pdf',
                    "web_url": web_url.replace(' ', '%20'),
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "item_currency": href_raw,
                "current_of_date": current_of_date
            }

            doc_name = f"{doc_type} {doc_num}"

            # set boolean if CAC is required to view document
            cac_login_required = True if any(x in href_raw for x in self.cac_required_options) \
                or any(x in doc_title for x in self.cac_required_options) else False

            source_page_url = response.url

            yield DocItem(
                doc_name=doc_name,
                doc_title=doc_title,
                doc_type=doc_type,
                doc_num=doc_num,
                publication_date=publication_date,
                source_page_url=source_page_url,
                cac_login_required=cac_login_required,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
            )

        try:
            nav_table = response.css('table.dnnFormItem')[1]
            next_page_link = next(
                a for a in nav_table.css('a.CommandButton')
                if a.css('::text').get() == 'Next'
            )
            yield response.follow(url=next_page_link,
                                  callback=self.parse_doc_table_page)
        except:
            pass
