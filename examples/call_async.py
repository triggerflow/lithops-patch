from triggerflow import Triggerflow
from triggerflow.eventsources import RedisEventSource
from pywren_ibm_cloud.triggerflow import TriggerflowExecutor
import pywren_ibm_cloud as pywren
import os
import time
import yaml


def my_function(x):
    time.sleep(10)
    return x + 1


def main(args):
    os.environ['PYWREN_EVENT_SOURCING'] = 'True'
    os.environ.pop('PYWREN_TOTAL_EXECUTORS', None)

    pw = pywren.ibm_cf_executor(**args, log_level='INFO')
    pw.call_async(my_function, 0)
    res = pw.get_result()

    print(res)

    #res = 0
    #for i in range(5):
    #    pw.call_async(my_function, int(res))
    #    res = pw.get_result()

    return {'total_time': time.time()-float(args['start_time'])}


if __name__ == "__main__":
    with open('pywren_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)
    tf_exec = TriggerflowExecutor(config=tf_config)
    tf_exec.run(main, name='triggerflow_pywren_callasync')
