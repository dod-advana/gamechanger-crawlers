from scrapy.exporters import BaseItemExporter, ScrapyJSONEncoder
from scrapy.utils.python import to_bytes


class JsonLinesAsJsonItemExporter(BaseItemExporter):
    def __init__(self, file, **kwargs):
        super().__init__(dont_fail=True, **kwargs)
        self.file = file
        self._kwargs.setdefault("ensure_ascii", not self.encoding)
        self.encoder = ScrapyJSONEncoder(**self._kwargs)

    def export_item(self, item):
        itemdict = dict(self._get_serialized_fields(item))
        data = self.encoder.encode(itemdict) + "\n"
        self.file.write(to_bytes(data, self.encoding))


class ZippedJsonLinesAsJsonItemExporter(JsonLinesAsJsonItemExporter):
    """Output exporter for spiders with zipped items"""
    def export_item(self, item):
        if not isinstance(item, list):
            item = [item]
        for i in item:
            itemdict = dict(self._get_serialized_fields(i))
            data = self.encoder.encode(itemdict) + "\n"
            self.file.write(to_bytes(data, self.encoding))