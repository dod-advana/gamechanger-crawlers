import re

from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider

digit_re = re.compile('\d')


def has_digit(text):
    return digit_re.match(text)


class MilpersmanSpider(GCSpider):
    name = 'milpersman_crawler'

    start_urls = ['https://www.mynavyhr.navy.mil/References/MILPERSMAN/']
    doc_type = "MILPERSMAN"
    cac_login_required = False

    def parse(self, response):

        anchors = [
            a for a in
            response.css('li[title="MILPERSMAN"] ul li a') if has_digit(a.css("::text").get())
        ]

        for res in response.follow_all(anchors, self.parse_doc_type):
            yield res

    def parse_doc_type(self, response):
        sub_anchors = response.css("ul.afAccordionMenuSubMenu a")
        # e.g. MILPERSMAN 1000 page has a dropdown for each subsection
        # pass each of those subpages to parse_page
        if len(sub_anchors):
            for res in response.follow_all(sub_anchors, self.parse_page):
                yield res
        else:
            for res in self.parse_page(response):
                yield res

    def parse_page(self, response):
        # get all rows that have an anchor tag in the first td
        rows = [
            tr for tr in
            response.css('div.livehtml table tbody tr')
            # have to check a special case
            if (len(tr.css('td:nth-child(1) a')) or len(tr.css('td:nth-child(2) a')))
        ]

        current_url = response.url

        for i, row in enumerate(rows):
            doc_num_raw = "".join(row.css('td:nth-child(1) *::text').getall())
            doc_num = self.ascii_clean(doc_num_raw)

            if not self.ascii_clean(doc_num):
                continue

            doc_title = " ".join(
                self.ascii_clean(text) for text in row.css('td:nth-child(2) *::text').getall()
            )

            href_raw = row.css("td:nth-child(1) a::attr(href)").get()
            web_url = self.ensure_full_href_url(href_raw, current_url)

            version_hash_fields = {
                "item_currency": href_raw,
                "document_title": doc_title
            }

            file_type = self.get_href_file_extension(href_raw)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "web_url": self.url_encode_spaces(web_url),
                    "compression_type": None
                }
            ]

            if doc_num == '1070-290':
                # if this changes, dont break
                try:
                    next_row = rows[i+1]
                    supp_href = next_row.css(
                        'td:nth-child(2) a::attr(href)').get()

                    supp_file_type = self.get_href_file_extension(supp_href)
                    supp_web_url = self.ensure_full_href_url(
                        supp_href, current_url)

                    downloadable_items.append({
                        "doc_type": supp_file_type,
                        "web_url": self.url_encode_spaces(supp_web_url),
                        "compression_type": None
                    })
                except:
                    pass

            doc_name = f"MILPERSMAN {doc_num}"

            yield DocItem(
                doc_name=doc_name,
                doc_title=doc_title,
                doc_num=doc_num,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
                source_page_url=current_url
            )
