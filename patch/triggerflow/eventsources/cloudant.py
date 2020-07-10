import os
import time
import json
import logging
from pywren_ibm_cloud.libs.cloudant.cloudant_client import CloudantClient

logger = logging.getLogger(__name__)


class CloudantEventSource:
    def __init__(self, config):
        self.config = config

    def get_events(self):
        if os.environ.get('PYWREN_FIRST_EXEC') == 'False':
            to = time.time()
            cloudant_client = CloudantClient(**self.config['cloudant'])
            records = cloudant_client.get(database_name='pywren', document_id='events')
            logger.info('Jobs downloaded - TOTAL: {} - TIME: {}s'.format(len(records), time.time()-to))
            if not records:
                exit()
        else:
            records = []

        event_sourcing_jobs = {}
        for subject in records:
            executor_id, job_id, fn = subject.rsplit('/', 2)
            if executor_id == self.executor_id:
                for message in records[subject]:
                    event = message['data']
                    if job_id not in event_sourcing_jobs:
                        event_sourcing_jobs[job_id] = []
                    event_sourcing_jobs[job_id].append(event)

        kafka_config = self.config['kafka']
        kafka_config['class'] = 'KafkaEventSource'
        kafka_config['topic'] = 'pywren-kafka-eventsource'
        kafka_config['name'] = 'pywren-kafka-eventsource'
        os.environ['__OW_TF_SINK'] = json.dumps(kafka_config)

        return event_sourcing_jobs
