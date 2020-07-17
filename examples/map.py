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


def create_tf_workspace():
    with open('pywren_config.yaml', 'r') as config_file:
        tf_config = yaml.safe_load(config_file)

    tf = Triggerflow(endpoint=tf_config['triggerflow']['endpoint'],
                     user=tf_config['triggerflow']['user'],
                     password=tf_config['triggerflow']['password'],
                     workspace=tf_config['triggerflow']['workspace'])

    try:
        tf.delete_workspace()
    except Exception as e:
        print(e)
    es = RedisEventSource(**tf_config['redis'])
    tf.create_workspace(workspace_name=tf_config['triggerflow']['workspace'], event_source=es)


if __name__ == "__main__":
    tf_exec = TriggerflowExecutor()
    tf_exec.run(main, name='triggerflow_pywren_map')
