# -- coding: utf-8 --
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider

from selenium.webdriver import Chrome
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import bs4
import re

re_1 = re.compile("^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$")
re_2 = re.compile("^\d$")


class NatoSpider(GCSeleniumSpider):
    name = "nato_stanag"

    start_urls = [
        "https://nso.nato.int/nso/nsdd/ListPromulg.html"
    ]

    selenium_request_overrides = {
        "wait_until": EC.presence_of_element_located((By.XPATH, "//*[@id='headerSO']"))
    }

    pdf_prefix = 'https://nso.nato.int/nso/'

    def parse(self, response):
        driver: Chrome = response.meta["driver"]
        page_url = response.url
        html = driver.execute_script(
            "return document.documentElement.outerHTML")
        soup = bs4.BeautifulSoup(html, features="html.parser")

        table = soup.find('table', attrs={'id': 'dataSearchResult'})
        rows = table.find_all('tr')

        for row in rows[1:]:
            data = row.find_all('td')
            if "No" not in data[1].text:
                doc_title = data[4].text.splitlines()[1].strip()
                doc_helper = data[2].text.split("Ed:")[0].strip()

                if "STANAG" in doc_helper or "STANREC" in doc_helper:
                    doc_num = doc_helper.split(
                        "\n")[1].strip().replace(" ", "_")
                    doc_type = doc_helper.split(
                        "\n")[0].strip().replace(" ", "_")

                else:
                    doc_ = doc_helper.split("\n")[0].strip()
                    doc_num = doc_.split('-', 1)[1].strip().replace(" ", "_")
                    doc_type = doc_.split('-', 1)[0].strip().replace(" ", "_")
                    if len(doc_helper.split()) > 1:
                        if re_1.match(doc_helper.split()[1].strip()):
                            doc_num = doc_num + "_VOL" + \
                                doc_helper.split()[1].strip()
                        if re_2.match(doc_helper.split()[1].strip()):
                            doc_num = doc_num + "_PART" + \
                                doc_helper.split()[1].strip()

                if len(data[2].text.split("VOL")) > 1:
                    volume = data[2].text.split("VOL")[1].split()[0].strip()
                    doc_num = doc_num + "_VOL" + volume

                if len(data[2].text.split("PART")) > 1:
                    volume = data[2].text.split("PART")[1].split()[0].strip()
                    doc_num = doc_num + "_PART" + volume
                doc_name = doc_type + " " + doc_num

                if len(data[2].text.split("Ed:")) > 1:
                    edition = data[2].text.split("Ed:")[1].strip()
                else:
                    edition = ""

                publication_date = data[5].text.splitlines()[1].strip()
                pdf_suffix = data[4].find('a')
                if pdf_suffix is None:
                    continue
                if "../classDoc.htm" in pdf_suffix['href']:
                    cac_login_required = True
                else:
                    cac_login_required = False

                di = {
                    'compression_type': None,
                    'doc_type': 'pdf',
                    'web_url': self.pdf_prefix + pdf_suffix['href'].replace('../', '').replace(" ", "%20")
                }

                version_hash_fields = {
                    "editions_and_volume": edition,
                    "type": data[1].text
                }

                yield DocItem(
                    doc_name=doc_name,
                    doc_title=doc_title,
                    doc_num=doc_num,
                    doc_type=doc_type,
                    publication_date=publication_date,
                    cac_login_required=cac_login_required,
                    source_page_url=page_url.strip(),
                    version_hash_raw_data=version_hash_fields,
                    downloadable_items=[di]
                )
