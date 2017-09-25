import os
import time
from lxml import etree

import settings

from framework.base_worker import BaseWorker


LOG_INTERVAL = 10000


DEFAULT_FEED_TAG = 'job'


def _plaintag(tag):
    pos_right_bracket = tag.rfind('}')
    if pos_right_bracket >= 0:
        return tag[pos_right_bracket+1:]
    else:
        return tag


def _xml2dict(el):
    if len(el.getchildren()) == 0:
        return el.text
    else:
        return {i.tag: _xml2dict(i) for i in el}


def extract_data_de(elem):
    data = _xml2dict(elem)
    data['attr_validFrom'] = elem.attrib.get('validFrom', None)
    return data


def extract_data_common(elem):
    return {c.tag: c.text for c in elem.getchildren()}


def get_feed_tag(feed_name):
    for feed in settings.JOB_SOURCES:
        if feed['name'].upper() == feed_name.upper():
            tag = feed['tag']
            return tag
    return DEFAULT_FEED_TAG


class ParserWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):
        super(ParserWorker, self).__init__(__name__, PreTopic, NextTopic)

    def build_msg_key(self, order, feed_name, filename):
        return feed_name

    def process(self, order, feed_name, filename):
        if not os.path.exists(filename):
            self.logger.error('file not exists: %s.' % filename)
            return

        tag_name = get_feed_tag(feed_name)
        start_time = time.time()

        self.logger.info('start parsing %2d-%s. tag=%s. MAX_JOBS_PER_FEED: %s' % (order, filename, tag_name, settings.MAX_JOBS_PER_FEED))

        context = etree.iterparse(filename, events=('end',), tag=tag_name, recover=True)

        extract_meth = extract_data_de if feed_name == 'DIRECT_EMPLOYERS' else extract_data_common
        cnt = 0

        for event, elem in context:

            cnt += 1

            data = extract_meth(elem)
            self.produce_msg(record=data, feed_name=feed_name, seq=cnt)

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if settings.MAX_JOBS_PER_FEED and cnt > settings.MAX_JOBS_PER_FEED:
                break

            if cnt and cnt % LOG_INTERVAL == 0:
                self.logger.info('%s parsed in %s' % (cnt, feed_name))

        del context

        feed_timecost = time.time() - start_time
        self.logger.warning('%s jobs in %s, timecost: %s' % (cnt, feed_name, feed_timecost))
