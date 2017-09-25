import json
import time
import settings

from framework.base_worker import BaseWorker
from utils.dup_detect import build_unique_id
from utils.cache_based_validator import build_cache, cache_based_validator
from utils.mongo_service import build_mongo_cache, mongo_filler
from utils.pay_price import calc_pay_price
from settings import r_db

# import settings

MAJOR_IN_DESC_AND_PREFERRED_TITLE                     = 80
MAJOR_SYNONYM_IN_DESC_AND_PREFERRED_TITLE             = 55
MAJOR_IN_DESC_AND_TITLE                               = 50
MAJOR_SYNONYM_IN_DESC_AND_TITLE                       = 45
MAJOR_PREFERRED_TITLE                                 = 40
MAJOR_IN_DESC                                         = 37
MAJOR_SYNONYM_IN_DESC                                 = 35
MAJOR_TITLE                                           = 18
MAJOR_TITLE_TOKEN                                     = 10
MAJOR_PREFERRED_SIMILAR_TITLE                         = 6
MAJOR_SIMILAR_TITLE                                   = 3
MAJOR_DEPRIORTIZE_KEYWORD                             = 0.2
MAJOR_SIMILAR_TITLE_TOKEN                             = 1
MAJOR_DEPRIORTIZE_TITLE                               = 0.02

MAJOR_BUCKET_1_CHOICES = {
    MAJOR_IN_DESC_AND_TITLE,
    MAJOR_SYNONYM_IN_DESC_AND_TITLE,
    MAJOR_IN_DESC,
    MAJOR_SYNONYM_IN_DESC,
}

MAJOR_BUCKET_3_CHOICES = {
    MAJOR_IN_DESC_AND_PREFERRED_TITLE,
    MAJOR_SYNONYM_IN_DESC_AND_PREFERRED_TITLE,
    MAJOR_PREFERRED_TITLE,
}


def float_in(val, vals):
    for target in vals:
        if abs(val - target) < 0.001:
            return True
    return False


def fill_majors_bucket(job_data, norm_rsp):
    major = norm_rsp.get('major')
    if major:
        major_priority = major or {}
        majors = list(major_priority.keys())
        bucket_1 = []
        bucket_2 = []
        bucket_3 = []
        for k, v in major_priority.items():
            if float_in(v, MAJOR_BUCKET_1_CHOICES):
                bucket_1.append(k)
            elif float_in(v, MAJOR_BUCKET_3_CHOICES):
                bucket_3.append(k)
            else:
                bucket_2.append(k)

        job_data.update({
            'majorPriority': major_priority,  # using it later?
            'majors': majors,
            'majorsBucket1': bucket_1,
            'majorsBucket2': bucket_2,
            'majorsBucket3': bucket_3,
        })
    else:
        job_data['majorPriority'] = {}


def fill_price(job_data):
    posting_date = job_data.get('postingDate', time.time())
    price = job_data.get('price', None)
    print("posting_date " + str(posting_date))
    print("price " + str(price))
    job_data['price'] = calc_pay_price(posting_date, price)


def api_filler(job_id, norm_rsp, job_data):

    title = norm_rsp.get('closest_lay_title')
    city = norm_rsp.get('normalized_city')

    _company = norm_rsp.get('clean_org_name')
    _companyDisplay = norm_rsp.get('display_org_name') or job_data['company']

    fill_majors_bucket(job_data, norm_rsp)

    job_data.update({
        'title': title,
        'industry': norm_rsp.get('major_group_string'),
        'socCode': norm_rsp.get('soc_code'),
        'skillsets': norm_rsp.get('skills', []),
        'city': city,
        'state': norm_rsp.get('normalized_state_name'),
        'company': _company,
        'companyDisplay': _companyDisplay,
        'jobLevel': norm_rsp.get('jobLevel') or 0,
        'educationDegree': norm_rsp.get('educationDegree') or 0,
        'jobTypes': norm_rsp.get('job_type') or [],
        'benefits': norm_rsp.get('benefits') or [],
        'visaStatus': norm_rsp.get('Visa_sponsorship') or '',
        'title1': title and title[0],
        'city1': city and city[0],
        # TODO: We only store 1 industry (and that too as a string). If there are multiple, we'll take the first one
    })

    socCode = job_data["socCode"]
    broad = socCode[:6] if socCode and len(socCode) >= 6 else "00-000"
    location = "%s_%s" % (job_data["city"], job_data["state"])
    titleState = "%s_%s" % (job_data["title"], job_data["state"])
    price = job_data["price"] or 0

    job_data.update({
        "broad": broad,
        "location": location,
        "titleState": titleState,
        "price": price,
    })


class FillerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):

        # norm job validate method
        cache = build_cache()
        self.cache_based_validate = cache_based_validator(cache)

        # mongo filler
        mongo_cache = build_mongo_cache()
        self.mongo_fill = mongo_filler(*mongo_cache)
        
        # init super last because it will send OK msg to master node
        # and only after all init works it's considered OK.
        super(FillerWorker, self).__init__(__name__, PreTopic, NextTopic)

    def build_msg_key(self, job_id, seq, *args, **kwargs):
        return "%s-%s" % (job_id, str(seq))

    def process(self, job_id, norm_rsp, job_data, seq):
        api_filler(job_id, norm_rsp, job_data)

        # filter out norm failed jobs

        ok, fields = self.cache_based_validate(job_data)
        if not ok:
            fields_dump = {f: job_data[f] for f in fields}
            fields_dump["jobId"] = job_id
            fields_dump["source"] = job_data["source"]
            self.logger.debug('discarded - fields unknown - ' + json.dumps(fields_dump))
            return

        # mongo filler
        self.mongo_fill(job_data)

        # fill price
        fill_price(job_data)

        # generate uniqueID by
        # sha1(companyDisplay, titleDisplay, city, state)

        unique_id = build_unique_id(
            job_data["companyDisplay"],
            job_data["titleDisplay"],
            job_data["city"],
            job_data["state"],
        )

        # add processSeq field to distinguish between tasks
        processSeq = r_db.get(settings.KEY_PROCESS_SEQ)
        job_data["processSeq"] = processSeq

        kwargs = {
            'job_id': unique_id,
            'job_data': job_data,
            'seq': seq,
        }

        # import json
        # self.logger.info('record %s' % json.dumps(job_data, indent=4))

        self.produce_msg(**kwargs)

