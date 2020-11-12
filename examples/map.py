from triggerflow import Triggerflow
from triggerflow.eventsources import KafkaEventSource, RedisEventSource
from lithops.triggerflow import TriggerflowExecutor
import lithops
import os
import time
import yaml


def my_function(x):
    time.sleep(3)
    return x + 1


def main(args):
    os.environ['PYWREN_EVENT_SOURCING'] = 'True'

    fexec = lithops.FunctionExecutor(**args, log_level='INFO')

    fexec.map(my_function, range(10))
    res = fexec.get_result()

    fexec.map(my_function, res)
    res = fexec.get_result()

    print(res)

    return {'total_time': time.time()-float(args['start_time'])}


if __name__ == "__main__":
    with open('lithops_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)
    tf_exec = TriggerflowExecutor(config=tf_config)
    tf_exec.run(main, name='triggerflow_lithops_map')
