from triggerflow import Triggerflow
from triggerflow.eventsources import KafkaEventSource, RedisEventSource
from pywren_ibm_cloud.triggerflow import TriggerflowExecutor
import pywren_ibm_cloud as pywren
import os
import time
import yaml


def my_function(x):
    time.sleep(3)
    return x + 1


def main(args):
    os.environ['PYWREN_EVENT_SOURCING'] = 'True'

    pw = pywren.ibm_cf_executor(**args, log_level='INFO')

    res = 0
    for i in range(5):
        pw.call_async(my_function, int(res))
        res = pw.get_result()

    return {'total_time': time.time()-float(args['start_time'])}


def create_tf_workspace():
    with open('pywren_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)

    tf = Triggerflow(endpoint=tf_config['triggerflow']['endpoint'],
                     user=tf_config['triggerflow']['user'],
                     password=tf_config['triggerflow']['password'])
    tf.delete_workspace(tf_config['triggerflow']['workspace'])

    # es = RedisEventSource(**tf_config['redis'])
    es = RedisEventSource(**tf_config['redis'])

    tf.create_workspace(workspace=tf_config['triggerflow']['workspace'],
                        event_source=es)


if __name__ == "__main__":
    create_tf_workspace()
    tf_exec = TriggerflowExecutor()
    tf_exec.run('pywren_tf_test', main)
