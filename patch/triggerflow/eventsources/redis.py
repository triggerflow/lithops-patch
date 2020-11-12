import os
import time
import json
import logging
import redis

logger = logging.getLogger(__name__)


class RedisEventSource:
    def __init__(self, config, executor_id):
        self.executor_id = executor_id
        self.stream = config['stream']
        self.name = config['name']
        self.host = config['host']
        self.port = config['port']
        self.password = config['password']
        self.db = config['db']

    def get_sink_data(self):
        redis_config = {}
        redis_config['class'] = 'RedisEventSource'
        redis_config['name'] = self.name
        redis_config['parameters'] = {}
        redis_config['parameters']['host'] = self.host
        redis_config['parameters']['port'] = self.port
        redis_config['parameters']['password'] = self.password
        redis_config['parameters']['db'] = self.db
        redis_config['parameters']['stream'] = self.stream

        return redis_config

    def get_events(self):
        if os.environ.get('LITHOPS_FIRST_EXEC') == 'False':
            logger.info('Event sourcing - Recovering events from redis stream: {}'.format(self.stream))
            to = time.time()
            redis_client = redis.StrictRedis(host=self.host, port=self.port,
                                             db=self.db, password=self.password,
                                             charset="utf-8",
                                             decode_responses=True)
            records = redis_client.xread({self.stream: '0'}, block=5)[0][1]
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

        return event_sourcing_jobs
