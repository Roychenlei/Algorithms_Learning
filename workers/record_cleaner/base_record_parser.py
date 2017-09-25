import settings

from utils.text import build_desc_cleaner

desc_clean = build_desc_cleaner(settings.JOB_DESC_MAX_LEN,
                                settings.FAST_MODE,
                                settings.JOB_DESC_ALLOWED_TAGS,
                                settings.JOB_DESC_ALLOWED_ATTRS)


class BaseParser(object):

    source_name = None
    desc_tag_name = 'description'

    target_attrs = (
        'source',
        'id',
        'title',
        'desc',
        'company',
        'industry',
        'zipcode',
        'city',
        'state',
        'country',
        'price',
        'postingDate',
        'url',
        # 'jobLevel',
        # 'educationDegree',
    )

    required_fields = (
        'id',
        'title',
        'url',
        # 'city',
        # 'state',
    )

    def __init__(self, record):
        self.orig_data = record
        self.error = []

        if not self.source_name:
            raise "source_name field is required for a RecordParser."

    def build_source(self):
        return self.source_name

    def get_desc_raw(self):
        return (self.orig_data.get(self.desc_tag_name) or '')

    def build_desc(self):
        return desc_clean(self.get_desc_raw())

    def build_country(self):
        return 'USA'

    def build_zipcode(self):
        return self.orig_data.get('zip')

    def build_postingDate(self):
        return self.orig_data.get('posted_at')

    def build_value(self, tag):
        meth = getattr(self, 'build_%s' % tag, None)  # slow here
        return meth() if meth else self.orig_data.get(tag)

    def validate_data(self, data):
        for field in self.required_fields:
            if not data[field]:
                self.error.append('missing %s' % field)
                return False
        # validate city and state separately
        if not data['city'] and not data['state']:
            self.error.append('missing city')
            self.error.append('missing state')
            return False
        return True

    def run(self):
        data = {t: self.build_value(t) for t in self.target_attrs}
        self.validate_data(data)
        if self.error:
            return self.error, data

        # lstrip `$' from title company city state
        _title = data['title']
        _title = _title and _title.lstrip('$')

        _company = data['company']
        _company = _company and _company.lstrip('$')

        _city = data['city']
        _city = _city and _city.lstrip('$')

        _state = data['state']
        _state = _state and _state.lstrip('$')

        data.update({
            "title": _title,
            "company": _company,
            "city": _city,
            "state": _state
        })

        data.update({
            'jobId': data['id'],
            'titleDisplay': data['title'],
            'companyDisplay': data['company'],
        })
        return self.error, data
