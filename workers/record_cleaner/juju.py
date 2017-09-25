from .base_record_parser import BaseParser


class Parser(BaseParser):
    source_name = 'JJ'
    desc_tag_name = 'description'

    def __init__(self, record):
        super(Parser, self).__init__(record)

        self.city, self.state = '', ''
        locations = (self.orig_data.get('location') or '').split(',')
        if len(locations) == 2:
            self.city, self.state = locations
        elif len(locations) == 1:
            self.state = locations[0]

    def build_id(self):
        return self.orig_data.get('id')

    def build_industry(self):
        # category may exist in orig data and the value is null
        # the output is a list with at least 1 industry
        return (self.orig_data.get('category') or '').split('/')

    def build_price(self):
        return 'PAY_SCALE_5'

    def build_postingDate(self):
        return self.orig_data.get('postingdate')

    def build_company(self):
        return self.orig_data.get('employer')

    def build_zipcode(self):
        # For JUJU feed we don't have zipcode present in job feed
        return None

    def build_url(self):
        return self.orig_data.get('joburl')

    def build_city(self):
        return self.city and self.city.strip()

    def build_state(self):
        return self.state and self.state.strip()
