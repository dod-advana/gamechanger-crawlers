from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class DCMASpider(GCSpider):
    name = "DCMA"
    display_org = "Dept. of Defense"
    data_source = "Defense Contract Management Agency Policy Publications"
    source_title = "DCMA Policy"

    start_urls = [
        "https://www.dcma.mil/Policy/"
    ]

    cac_login_required = False

    def parse(self, response):
        sections = response.css('div#accGen div table tbody')

        for section in sections:
            rows = section.css('tr')

            # skip headers
            for row in rows[1:]:
                title_raw = row.css('td:nth-child(1)::text').get(default="")
                doc_type_raw = row.css('td:nth-child(2)::text').get(default="")
                policy_no_raw = row.css(
                    'td:nth-child(3)::text').get(default="")
                published_raw = row.css(
                    'td:nth-child(4)::text').get(default="")
                href_raw = row.css(
                    'td:nth-child(5) a::attr(href)').get(default="")

                if not href_raw:
                    continue

                doc_title = self.ascii_clean(title_raw)
                print(doc_title)
                doc_type = self.ascii_clean(doc_type_raw)
                doc_num = self.ascii_clean(policy_no_raw)
                publication_date = self.ascii_clean(published_raw)

                file_type = self.get_href_file_extension(href_raw)

                version_hash_fields = {
                    "item_currency": href_raw,
                    "document_title": doc_title
                }

                downloadable_items = [
                    {
                        "doc_type": file_type,
                        "web_url": f"https://www.dcma.mil{href_raw}",
                        "compression_type": None
                    }
                ]

                if doc_type == "DPS" or doc_type == "PTM":
                    display_doc_type = "Memo"
                else:
                    display_doc_type = doc_type

                doc_type = f"DCMA {doc_type}"

                yield DocItem(
                    doc_name=f"{doc_type} {doc_num}",
                    doc_type=doc_type,
                    display_doc_type=display_doc_type,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    publication_date=publication_date,
                    downloadable_items=downloadable_items,
                    version_hash_raw_data=version_hash_fields,
                    source_page_url=response.url
                )
