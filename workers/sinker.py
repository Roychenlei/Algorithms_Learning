from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk

from framework.base_worker import BaseWorker
from models.joblisting import JobPosting

import settings


_jobs_es_batch = []


class SinkerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):
        super(SinkerWorker, self).__init__(__name__, PreTopic, NextTopic)
        JobPosting.init()

    def build_msg_key(self, job_id, seq, *args, **kwargs):
        return "%s-%s" % (job_id, str(seq))

    def process(self, job_id, job_data, seq):
        _jobs_es_batch.append(JobPosting(meta={'id': job_id}, **job_data))
        es_jobs_number = len(_jobs_es_batch)

        if es_jobs_number >= settings.ES_BATCH_SIZE:
            self.logger.info('saving to ES. job_id: %s' % job_id)
            bulk(connections.get_connection(), (d.to_dict(True) for d in _jobs_es_batch))
            del _jobs_es_batch[:]
