from triggerflow import Triggerflow
from triggerflow.eventsources import RedisEventSource
from pywren_ibm_cloud.triggerflow import TriggerflowExecutor
import pywren_ibm_cloud as pywren
import os
import time


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


if __name__ == "__main__":
    tf_exec = TriggerflowExecutor()
    tf_exec.run(main, name='triggerflow_pywren_callasync')
