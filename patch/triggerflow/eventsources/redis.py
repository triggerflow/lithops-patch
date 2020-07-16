import os
import time
import json
import logging
import redis

logger = logging.getLogger(__name__)


class RedisEventSource:
    def __init__(self, config):
        self.config = config

    def get_events(self):
        if os.environ.get('PYWREN_FIRST_EXEC') == 'False':
            logger.info('Event sourcing - Recovering events from redis')
            to = time.time()
            redis_client = redis.StrictRedis(**self.config['redis'],
                                             charset="utf-8",
                                             decode_responses=True)
            records = redis_client.xread({'pywren-redis-eventsource': '0'}, block=5)[0][1]
            logger.info('Jobs downloaded - TOTAL: {} - TIME: {}s'.format(len(records), round(time.time()-to, 3)))
            if not records:
                exit()
        else:
            records = []

        event_sourcing_jobs = {}
        for e_id, event in records:
            if event['subject'].startswith(self.executor_id):
                executor_id, job_id, fn = event['subject'].rsplit('/', 2)
                data = json.loads(event['data'])
                if job_id not in event_sourcing_jobs:
                    event_sourcing_jobs[job_id] = []
                event_sourcing_jobs[job_id].append(data)

        redis_config = {}
        redis_config['class'] = 'RedisEventSource'
        redis_config['name'] = 'pywren-redis-eventsource'
        redis_config['parameters'] = self.config['redis']

        os.environ['__OW_TF_SINK'] = json.dumps(redis_config)

        return event_sourcing_jobs
