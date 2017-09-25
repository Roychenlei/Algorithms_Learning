import json

from framework.base_worker import BaseWorker
from framework import reports

from record_cleaner import parse_record
from utils.exceptions import UnsupportedFeed

from utils.dup_detect import get_or_build_job_id, build_listing_hash


class CleanerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic, NextTopicOldJob):
        super(CleanerWorker, self).__init__(__name__, PreTopic, NextTopic)

        self.next_topic_oldjob = NextTopicOldJob()

    def build_msg_key(self, record, feed_name, seq):
        return '%s-%s' % (feed_name, seq)

    def process(self, record, feed_name, seq):
        try:
            error, data = parse_record(record, feed_name, seq)
        except UnsupportedFeed:
            # error logged in parse_record method
            return

        # import json
        # self.logger.info('record %s' % json.dumps(data, indent=4))

        if error:
            reports.incr('clean-error', feed_name)
            self.logger.debug("discarded - clean error - " + json.dumps(
                {
                    "jobId": data and data.get("id",'unknown') or 'unknown',
                    "error": error,
                    "feed": feed_name,
                }))
        else:
            job_id, is_new = get_or_build_job_id(**data)

            ## listingHash
            # using 'undefined' to be compatible with the origin js version
            undefined = "undefined"
            if "listingHash" not in data.keys():
                listing_hash = build_listing_hash(
                    data.get("title", None) or undefined,
                    data.get("company", None) or undefined,
                    data.get("city", None) or undefined,
                    data.get("state", None) or undefined,
                    data.get("desc", None) or undefined,
                )
                data["listingHash"] = listing_hash

            kwargs = {
                'job_id': job_id,
                'job_data': data,
                'feed_name': feed_name,
                'seq': seq,
            }

            if is_new:
                self.produce_msg(**kwargs)
            else:
                self.produce_msg(self.next_topic_oldjob, **kwargs)
