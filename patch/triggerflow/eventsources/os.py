import os
import time
import json
import logging


logger = logging.getLogger(__name__)


class ObjectStorageEventSource:
    def __init__(self, config):
        self.config = config

    def get_events(self):
        if os.environ.get('LITHOPS_FIRST_EXEC') == 'False':
            logger.info('Event sourcing - Searching results in storage')
            to = time.time()
            done_jobs = self.internal_storage.get_executor_status(self.executor_id)
            logger.info('Events downloaded - TOTAL: {} - TIME: {}s'.format(len(done_jobs), round(time.time()-to, 3)))
            if not done_jobs:
                exit()
        else:
            done_jobs = []

        event_sourcing_jobs = {}
        for call in done_jobs:
            if call[1] not in self.event_sourcing_jobs:
                self.event_sourcing_jobs[call[1]] = []
            self.event_sourcing_jobs[call[1]].append(call[2])

        redis_config = self.config['redis']
        redis_config['class'] = 'RedisEventSource'
        redis_config['stream'] = 'lithops-redis-eventsource'
        redis_config['name'] = 'lithops-redis-eventsource'
        os.environ['__OW_TF_SINK'] = json.dumps(redis_config)

        return event_sourcing_jobs
