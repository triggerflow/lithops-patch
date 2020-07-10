from pywren_ibm_cloud.libs.triggerflow import TriggerflowClient, CloudEvent, PythonCallable
from pywren_ibm_cloud.libs.triggerflow.utils import load_config_yaml
from pywren_ibm_cloud.libs.triggerflow.sources import KafkaEventSource, RedisEventSource

from pywren_ibm_cloud.triggerflow import TriggerflowExecutor
import pywren_ibm_cloud as pywren
import os
import time

WORKSPACE = 'pywren'


def my_function(x):
    time.sleep(3)
    print(x)
    return x + 1


def main(args):
    pywren_config = args.get('config', None)
    execution_id = args.get('execution_id', None)
    start_time = args.get('start_time', None)

    print(pywren_config)
    print(execution_id)
    print(start_time)

    #os.environ['PYWREN_EVENT_SOURCING'] = 'True'
    os.environ['__OW_TF_WORKSPACE'] = WORKSPACE
    os.environ.pop('PYWREN_TOTAL_EXECUTORS', None)

    if start_time:
        os.environ['START_TIME'] = start_time
    else:
        os.environ['START_TIME'] = str(time.time())

    pw = pywren.ibm_cf_executor(config=args['config'],
                                execution_id=args['execution_id'],
                                log_level='INFO')

    res = 0
    for i in range(20):
        pw.call_async(my_function, int(res))
        res = pw.get_result()

    # pw.map(my_function, range(10))
    # res = pw.get_result()

    result = {'total_time': time.time()-float(start_time)}
    print(result)

    return result


def create_tf_workspace():
    tf_config = load_config_yaml('client_config.yaml')
    tf = TriggerflowClient(**tf_config['triggerflow'])
    tf.delete_workspace(WORKSPACE)

    # es = RedisEventSource(**tf_config['redis'])
    es = KafkaEventSource(**tf_config['kafka'])

    tf.create_workspace(workspace=WORKSPACE, global_context={'ibm_cf': tf_config['ibm_cf']}, event_source=es)


if __name__ == "__main__":
    create_tf_workspace()
    tf_exec = TriggerflowExecutor()
    tf_exec.run('pywren_tf_test', main)
