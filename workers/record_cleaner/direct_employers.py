import bleach
import time

from .base_record_parser import BaseParser
import settings


def getin(path, d):
    path = path[::-1]

    while len(path) != 0:
        k = path.pop()
        if not isinstance(d, dict):
            return None
        d = d.get(k)

    return d


def parse_location(location):
    city = None
    state = None
    locations = location.split('-')
    if locations[0].upper() == 'USA' and len(locations) < 3:
        city = None
        state = None
    elif locations[0].upper() == 'USA' and len(locations) >= 3:
        state = locations[1]
        city = locations[2]
        if city.find('/') > -1:
            city = city.split('/')[0]
    else:
        state = locations[0]
        city = locations[1]
        if city.find('/') > -1:
            city = city.split('/')[0]
    return (city, state)


class Parser(BaseParser):
    source_name = 'DE'
    _city_state = None

    def build_id(self):
        return self.orig_data.get('{http://www.hr-xml.org/3}AlternateDocumentID') or ''

    def build_title(self):
        return getin(['{http://www.hr-xml.org/3}PositionProfile',
                      '{http://www.hr-xml.org/3}PositionTitle'], self.orig_data) or ''

    def build_company(self):
        return getin(['{http://www.hr-xml.org/3}PositionProfile',
                      '{http://www.hr-xml.org/3}PositionOrganization',
                      '{http://www.hr-xml.org/3}OrganizationIdentifiers',
                      '{http://www.hr-xml.org/3}OrganizationName'], self.orig_data)

    def _get_city_state(self, location):
        if not self._city_state:
            self._city_state = parse_location(location)
        return self._city_state

    def build_city(self):
        names = getin(['{http://www.hr-xml.org/3}PositionProfile',
                       '{http://www.hr-xml.org/3}PositionLocation',
                       '{http://www.hr-xml.org/3}LocationName'], self.orig_data)
        city = None
        if names:
            (city, state) = self._get_city_state(names)
        return city

    def build_state(self):
        names = getin(['{http://www.hr-xml.org/3}PositionProfile',
                       '{http://www.hr-xml.org/3}PositionLocation',
                       '{http://www.hr-xml.org/3}LocationName'], self.orig_data)
        state = None
        if names:
            (city, state) = self._get_city_state(names)
        return state

    def build_country(self):
        return getin(['{http://www.hr-xml.org/3}PositionProfile',
                      '{http://www.hr-xml.org/3}PositionLocation',
                      '{http://www.hr-xml.org/3}ReferenceLocation',
                      '{http://www.hr-xml.org/3}CountryCode'], self.orig_data) or 'USA'

    def build_zipcode(self):
        return getin(['{http://www.hr-xml.org/3}PositionProfile',
                      '{http://www.hr-xml.org/3}PositionLocation',
                      '{http://www.hr-xml.org/3}ReferenceLocation',
                      '{http://www.openapplications.org/oagis/9}PostalCode'], self.orig_data) or '';

    def get_desc_raw(self):
        return (getin([
            '{http://www.hr-xml.org/3}PositionProfile',
            '{http://www.hr-xml.org/3}PositionFormattedDescription',
            '{http://www.hr-xml.org/3}Content'], self.orig_data) or '')

    def build_industry(self):
        return [getin(['{http://www.hr-xml.org/3}PositionProfile',
                      '{http://www.hr-xml.org/3}JobCategoryCode'], self.orig_data) or '']

    def build_postingDate(self):
        return self.orig_data.get('attr_validFrom', None) or time.time()

    def build_url(self):
        return getin(['{http://www.hr-xml.org/3}PositionProfile',
                      '{http://www.hr-xml.org/3}PostingInstruction',
                      '{http://www.hr-xml.org/3}ApplicationMethod',
                      '{http://www.hr-xml.org/3}Communication',
                      '{http://www.openapplications.org/oagis/9}URI'], self.orig_data) or ''

    def build_price(self):
        return 'PAY_SCALE_7'
