import time
import json

from os.path import join

import settings

from settings import r_db
from framework.base_worker import BaseWorker
from utils.clean_es import clean_es
from utils import cache
from topics.job_source import JobSourceTopic
from topics.downloaded_xml import DownloadedXmlTopic
from settings import MASTER_INTERVAL, KAFKA_WAIT_TIME, TOPIC_COUNT_MAX_IDLE_TIME

ENOTFIN = 1
SUCC = 0

TASK_RERUN_WAIT_TIME = 15 * 60

class Master(BaseWorker):

    def __init__(self, workers_info, topics):
        super(Master, self).__init__(__name__)
        self._workers_info = workers_info
        self._topics = topics

        # states
        # !IMPORTANT
        # if new state are added, make sure they are reset in handle_setup
        self._topic_check_idx = 0
        self._last_topic_counts = None
        self._topic_idle_start_time = None
        self._task_start_time = None
        self._task_finish_time = None

    @staticmethod
    def _run(meth, sleep_interval=None):
        while meth() != SUCC:
            if sleep_interval:
                time.sleep(sleep_interval)

    def run_loop(self):
        # init
        self.logger.info("minimal status " + json.dumps(self._workers_info))
        self._run(self.handle_init, MASTER_INTERVAL)
        while True:
            # setup
            self.handle_setup()
            # monitor
            self._run(self.handle_monitor, MASTER_INTERVAL)
            # finish
            self.handle_finish()
            # idle
            self.handle_idle()

    def handle_init(self):
        """
        cluster initialization work
        1. checks all workers are up
        """
        worker_status = {
            worker: r_db.get(settings.KEY_WORKER_READY % worker) or 0
            for worker in self._workers_info.keys()
        }

        self.logger.info("worker status " + json.dumps(worker_status))

        for worker in self._workers_info.keys():
            if int(worker_status[worker]) < int(self._workers_info[worker]):
                return ENOTFIN

        self.logger.info("workers ready")
        time.sleep(KAFKA_WAIT_TIME)
        return SUCC

    def handle_setup(self):
        """
        Setup environment for a task
        1. clear redis
        2. generate new process_seq id
        3. send init message to kafka
        4. reset state
        """
        self.logger.info("setting up env for new task")

        self.logger.info("clearing redis")
        cache.init_redis()

        process_seq = time.strftime("%Y%m%d%H%M", time.gmtime())
        self._process_seq = process_seq
        r_db.set(settings.KEY_PROCESS_SEQ, process_seq)
        self.logger.info("generated process_seq: %s" % process_seq)

        # send init message
        self.logger.info("sending init messages to start the task")
        topic = JobSourceTopic()
        topic.set_producer_running()

        for feed in topic.load_cache():
            self.logger.info('sent to Topic %s, feed=%2d. %s' % (topic.topic_name, feed['order'], feed['name']))
            topic.produce(feed)

        topic.set_producer_done()

        # reset state
        self._topic_check_idx = 0
        self._last_topic_counts = None
        self._topic_idle_start_time = None
        self._task_start_time = time.time()
        self._task_finish_time = None

        self.logger.info("finished setting up env")

    def handle_monitor(self):
        """
        Monitoring worker status and task progress
        1. checks if all worker are working properly
           1. the last respond time not exceeds max_res_time
        2. checks task progress
           1. how many steps have finished
           2. the number of message in, message out, error message
        """
        # dump current topic counts
        topic_counts = [(
            topic.get_cnt_produced() or 0,
            topic.get_cnt_consumed() or 0,
            topic.get_cnt_cached() or 0,
        ) for topic in self._topics]

        # if topic counts didn't change for more than TOPIC_COUNT_MAX_IDLE_TIME
        # consider the system has finished and log a warning
        # >> shared states involved <<
        # * _last_topic_counts
        # * _topic_idle_start_time
        # * TOPIC_COUNT_MAX_IDLE_TIME
        if self._last_topic_counts and self._last_topic_counts == topic_counts:
            if not self._topic_idle_start_time:
                self.logger.warn("topic counts didn't change, starts counting idle time")
                self._topic_idle_start_time = time.time()
            else:
                idle_time = time.time() - self._topic_idle_start_time
                if idle_time > TOPIC_COUNT_MAX_IDLE_TIME:
                    self.logger.warn("topic counts didn't change for %ds, consider as finished" %
                                     TOPIC_COUNT_MAX_IDLE_TIME)
                    return SUCC
                self.logger.warn("topic has been idle for %ds" % idle_time)
        else:
            # topic counts changed, so set _topic_idle_start_time to None
            self._topic_idle_start_time = None
        self._last_topic_counts = topic_counts

        # check topic finish state
        while self._topic_check_idx < len(self._topics):
            # all topics before are finished while this topic is not at last check
            # so check it
            topic = self._topics[self._topic_check_idx]
            n_produced, n_consumed, n_cached = topic_counts[self._topic_check_idx]

            # do not check n_produced > 0, for feed error may result in 0 message produced in some workerss
            if n_consumed < n_produced:  # n_msg_out >= n_msg_in
                self.logger.debug("running %s, produced/consumed/cached: %s/%s/%s" %
                                  (topic.topic_name, n_produced, n_consumed, n_cached))
                return ENOTFIN  # unfinished
            else:
                self._topic_check_idx += 1
                self.logger.warning("done %s, produced/consumed/cached: %s/%s/%s" %
                                    (topic.topic_name, n_produced, n_consumed, n_cached))

        self.logger.info("considered success, go to finish state.")
        return SUCC

    def handle_finish(self):
        """
        After all data has been processed.
        1. _delete_by_query { query: { bool: { must_not: { term { processSeq: $SEQ } } } } }
        """
        process_seq_id = self._process_seq
        if process_seq_id:
            try:
                is_succ, rate = clean_es(process_seq_id)
                if is_succ:
                    self.logger.info("successfully cleand up elasticsearch, del rate: %f" % rate)
                else:
                    self.logger.warn("failed to cleand up elasticsearch, del rate: %f" % rate)
            except Exception as e:
                self.logger.exception(e)
        else:
            self.logger.warn("process seq (%s) not valid, skip es clean up" % process_seq_id)
        
        # mark finish time
        self._task_finish_time = time.time()

    def handle_idle(self):
        """
        Checks if it's ok to start task of next day
        Then switch master status to S_SETUP
        """
        time_remain = TASK_RERUN_WAIT_TIME + self._task_finish_time - time.time()
        while time_remain > 0:
            self.logger.info("Idling... remaining idle time: %ds" % time_remain)
            time.sleep(60)
            time_remain = TASK_RERUN_WAIT_TIME + self._task_finish_time - time.time()
