import os
import time
import json
import logging
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class KafkaEventSource:
    def __init__(self, config):
        self.config = config

    def get_events(self):
        if os.environ.get('PYWREN_FIRST_EXEC') == 'False':
            to = time.time()
            consumer = KafkaConsumer('pywren-kafka-eventsource', bootstrap_servers=self.config['kafka']['broker_list'],
                                     auto_offset_reset='earliest', enable_auto_commit=False)
            logger.info('Downloading Events')
            kafka_data = consumer.poll(timeout_ms=10000, max_records=10000)
            records = []
            for topic_partition in kafka_data:
                records.extend(kafka_data[topic_partition])
            logger.info('Events downloaded - TOTAL: {} - TIME: {}s'.format(len(records), round(time.time()-to, 3)))
            if not records:
                exit()
        else:
            records = []

        event_sourcing_jobs = {}
        for record in records:
            event = json.loads(record.value.decode('utf-8'))
            if event['subject'].startswith(self.executor_id):
                executor_id, job_id, fn = event['subject'].rsplit('/', 2)
                data = json.loads(event['data'])
                if job_id not in event_sourcing_jobs:
                    event_sourcing_jobs[job_id] = []
                event_sourcing_jobs[job_id].append(data)

        kafka_config = self.config['kafka']
        kafka_config['class'] = 'KafkaEventSource'
        kafka_config['topic'] = 'pywren-kafka-eventsource'
        kafka_config['name'] = 'pywren-kafka-eventsource'
        os.environ['__OW_TF_SINK'] = json.dumps(kafka_config)

        return event_sourcing_jobs
