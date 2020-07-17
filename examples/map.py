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

    pw.map(my_function, range(10))
    res = pw.get_result()

    pw.map(my_function, res)
    res = pw.get_result()

    print(res)

    return {'total_time': time.time()-float(args['start_time'])}


if __name__ == "__main__":
    with open('pywren_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)
    tf_exec = TriggerflowExecutor(config=tf_config)
    tf_exec.run(main, name='triggerflow_pywren_map')
