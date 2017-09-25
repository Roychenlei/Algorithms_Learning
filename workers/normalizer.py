import logging
import requests
import time
import json

from framework.base_worker import BaseWorker
from framework import reports

import settings
from settings._redis import r_db


MAJOR_IN_DESC_AND_PREFERRED_TITLE = 60
MAJOR_SYNONYM_IN_DESC_AND_PREFERRED_TITLE = 60
MAJOR_IN_DESC_AND_TITLE = 45
MAJOR_SYNONYM_IN_DESC_AND_TITLE = 45
MAJOR_IN_DESC = 35
MAJOR_SYNONYM_IN_DESC = 35

MAJOR_BUCKET_1_CHOICES = {
    MAJOR_IN_DESC_AND_PREFERRED_TITLE,
    MAJOR_SYNONYM_IN_DESC_AND_PREFERRED_TITLE,
    MAJOR_IN_DESC_AND_TITLE,
    MAJOR_SYNONYM_IN_DESC_AND_TITLE,
    MAJOR_IN_DESC,
    MAJOR_SYNONYM_IN_DESC,
}

RETRY_PRESET = [(3, 0), (10, 0), (60, 10), (600, 30), (1800, 120)]
RETRY_PRESET_LEN = len(RETRY_PRESET)

logger = logging.getLogger(__name__)


LOG_INTERVAL = 10

LOCATION_EDIT_DISTANCE = 2

_job_batch = []
_job_ids = []
_job_seqs = []
_job_feeds = []


def timeperf(f):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        r = f(*args, **kwargs)
        t = time.time() - t1
        logger.info('timecost: %.4f' % t)
        return r
    return wrapper


# @timeperf
def request_norm(jobs, batch_id):
    data = {
        'batch_id': batch_id,
        'classify_job_level': True,
        'classify_major': True,
        'classify_education_degree': True,
        'extract_benefits': True,
        'extract_visa_status': True,
        'extract_job_type': True,
        'jobs': [{
            'title': r['title'],
            'description': r['desc'],
            'soc_hint': r.get('socCodeHint') or 'Undefined',
            'organization': r['company'],
            'city': r['city'] or '',
            'state': r['state'] or '',
            'edit_distance_threshold': settings.LOCATION_EDIT_DISTANCE,
        } for r in jobs],
    }
    r = requests.post(settings.URL_NORM_JOB, json=data)
    return r.status_code, r.json()


class NormalizerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):
        super(NormalizerWorker, self).__init__(__name__, PreTopic, NextTopic)
        self.processed = False

    def build_msg_key(self, job_id, seq, *args, **kwargs):
        return "%s-%s" % (job_id, str(seq))

    def process(self, job_id, job_data, feed_name, seq):

        _job_batch.append(job_data)
        _job_ids.append(job_id)
        _job_feeds.append(feed_name)
        _job_seqs.append(seq)

        job_number = len(_job_batch)
        if job_number < settings.NORM_BATCH_SIZE:
            self.processed = False
            return

        self.processed = True

        # batch full, send norm requests
        batch_id = r_db.incr(settings.r_batch_num_key)

        status_code, rsp_json = request_norm(_job_batch, batch_id)

        # handle status code: 429, non 200
        retry_times = 10
        for i in range(retry_times):
            if status_code != 429:
                break
            logger.info("batch %d status code 429, retry" % batch_id)
            status_code, rsp_json = request_norm(_job_batch, batch_id)
        else:
            logger.error('batch %d retried %d times, give up...' % (batch_id, retry_times))
            for jobid, feed in zip(_job_ids, _job_feeds):
                self.logger.debug('discarded - norm req failed - ' + json.dumps({"jobId": jobid, "feed": feed}))
            return

        # handle response length mismatch
        if len(rsp_json.get('normalized_jobs', [])) != job_number:
            reports.incr_by(job_number, 'norm-error')
            logger.error('unexpected norm error. length_neq_sent_size. batch_id: %s' % (batch_id, ))
            for jobid, feed in zip(_job_ids, _job_feeds):
                self.logger.debug('discarded - norm resp invalid - ' + json.dumps({"jobId": jobid, "feed": feed}))
            return

        # handle every single normed job
        for job_id, norm_rsp, i_job_data, job_seq, job_feed in zip(_job_ids, rsp_json['normalized_jobs'], _job_batch, _job_seqs, _job_feeds):
            if 'error_code' in norm_rsp:
                reports.incr('norm-error')
                self.logger.debug('discarded - norm failed - ' + json.dumps({"jobId": job_id, "feed": job_feed}))
            else:
                kwargs = {
                    'job_id': job_id,
                    'norm_rsp': norm_rsp,
                    'job_data': i_job_data,
                    'seq': job_seq,
                }
                self.produce_msg(**kwargs)

    def post_process(self, *args, **kwargs):
        if not self.processed:
            return

        del _job_batch[:]
        del _job_ids[:]
        del _job_seqs[:]
        del _job_feeds[:]
