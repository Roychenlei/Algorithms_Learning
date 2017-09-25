import os
import requests
from contextlib import closing

import settings

from framework.base_worker import BaseWorker


def _request(url, filename):
    # http://docs.python-requests.org/en/latest/user/advanced/#body-content-workflow
    with closing(requests.get(url, stream=True)) as r:
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
    return


class DownloaderWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):
        super(DownloaderWorker, self).__init__(__name__, PreTopic, NextTopic)

    def build_msg_key(self, name, url, *args, **kwargs):
        return url

    def process(self, name, url, order, retry=3, **kwargs):
        self.logger.info('downloading %2d. %s...' % (order, name))
        filename = os.path.join(settings.XML_PATH, '%s.xml' % name.lower())

        try:
            if settings.FAKE_DOWNLOAD:
                self.logger.info('skip downloading as FAKE_DOWNLOAD=True')
            else:
                _request(url, filename)
        except Exception as e:
            self.logger.error(e)
            if retry > 0:
                retry -= 1
                self.logger.info('download %s error. retry=%s' % (name, retry))
                self.process(name, url, order, retry=retry, **kwargs)
            else:
                self.logger.warning('download %s error. skip' % name)
            return

        kwargs = {
            'order': order,
            'feed_name': name,
            'filename': filename,
        }
        self.produce_msg(**kwargs)
