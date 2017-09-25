import time

from .base_record_parser import BaseParser


class Parser(BaseParser):
    source_name = 'TUJ'
    desc_tag_name = 'JobDescription'

    def build_id(self):
        return self.orig_data.get('JobID')

    def build_industry(self):
        # category may exist in orig data and the value is null
        # the output is a list with at least 1 industry
        return (self.orig_data.get('JobCategory') or '').split(',')

    def build_price(self):
        return 'PAY_SCALE_11'

    def build_title(self):
        return self.orig_data.get('JobTitle')

    def build_company(self):
        return self.orig_data.get('JobCompany')

    def build_zipcode(self):
        return self.orig_data.get('JobZip')

    def build_city(self):
        return self.orig_data.get('JobCity')

    def build_state(self):
        return self.orig_data.get('JobState')

    def build_url(self):
        return self.orig_data.get('JobUrl')

    def build_postingDate(self):
        return time.time()
