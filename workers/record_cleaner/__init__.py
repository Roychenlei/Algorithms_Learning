import logging
import importlib
from utils.exceptions import UnsupportedFeed


logger = logging.getLogger('workers.cleaner')

name2module = dict()


def parse_record(record, feed_name, cnt):
    if feed_name in name2module:
        module = name2module[feed_name]
    else:
        try:
            module = importlib.import_module('workers.record_cleaner.%s' % feed_name.lower())
        except ImportError as e :
            logger.exception(e)
            logger.error('unsupported feed. %s' % feed_name)
            module = None
        name2module[feed_name] = module

    if module is None:
        raise UnsupportedFeed(feed_name)
    else:
        return module.Parser(record).run()
