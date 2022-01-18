from pathy import tempfile
import scrapy
import re
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from io import BytesIO
import tempfile
import csv
import traceback


class OSDHistoricalSpider(GCSpider):
    name = "OSD Historical"
    source_url = "https://history.defense.gov/Publications/DoD-Key-Officials/"
    pdf_url = "https://history.defense.gov/Portals/70/Documents/key_officials/KeyOfficials01-03-22.pdf?ver=snJDaCBnxmKwsKY6YW7F3A%3d%3d"
    csv_url = "https://public.tableau.com/vizql/w/KeyOfficials2/v/TextTableDash/vudcsv/sessions/1C81C3FD01FF4E40ADF3B9B2BC80BF31-0:0/views/13032911137686962556_2168284586895473433?underlying_table_id=Migrated%20Data&underlying_table_caption=Full%20Data"
    start_urls = [csv_url]

    def parse(self, response):
        try:
            reader = csv.DictReader(response.text.splitlines())
            headers = [
                'SectionPlusSubsection',
                'DisplayNameTableau',
                'Name and Dates',
                'Category (Positions)',
                'DisplayName',
                'DoNotShowBook',
                'FirstNameLastName',
                'Full Name',
                'NotInTableau',
                'President',
                'RangeOrSingleFilter',
                'Acting?',
                'Current',
                'End Date',
                'EndDateApprox',
                'EndDateCalc',
                'EndDateNullIsToday',
                'First Name',
                'Last Name',
                'Middle Name',
                'Name-Date Spacing',
                'PositionName',
                'Rank at Time',
                'Section',
                'Section Head',
                'SortInTextTable',
                'Start Date',
                'Start-end Date',
                'StartDateApprox',
                'StartOrEnd',
                'Subhead',
                'Suffix',
                'TimeInOfficeLabel',
                'Months in term',
                'MonthsRemainder',
                'Reestablishment (Positions)',
                'SortInBook',
                'SortInGroup'
            ]
            count = 0

            for row in reader:
                count += 1
                print(row['Full Name'])
                if count > 3:
                    break

        except Exception:
            traceback.print_exc()
