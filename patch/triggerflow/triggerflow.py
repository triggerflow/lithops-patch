import os
import copy
import logging
import inspect

from pywren_ibm_cloud.libs.openwhisk.client import OpenWhiskClient
from pywren_ibm_cloud.utils import create_executor_id
from pywren_ibm_cloud.config import default_config

logger = logging.getLogger(__name__)


class TriggerflowExecutor:

    def __init__(self, config=None, runtime=None):
        """
        Initialize a FunctionExecutor class.

        :param config: Settings passed in here will override those in config file. Default None.
        :param runtime: Runtime name to use. Default None.
        """

        self.config = default_config(copy.deepcopy(config))
        self.ow_client = OpenWhiskClient(**list(self.config['ibm_cf']['regions'].values())[0])
        self.ow_client.create_package('triggerflow')
        logger.info('TriggerflowExecutor created')

    def run(self, coordinator_function, name):
        assert coordinator_function.__name__ == 'main', "Coordinator Function must have 'main' name"

        file_path = os.path.abspath(inspect.getfile(coordinator_function))

        with open(file_path, 'r') as fn:
            code = fn.read()
            self.ow_client.create_action('triggerflow', name, code=code, memory=256,
                                         kind='python:3', is_binary=False)

        payload = {'config': self.config, 'execution_id': None, 'start_time': 0}
        self.ow_client.invoke('triggerflow', name, payload)
        logger.info('Coordinator function {} invoked '.format(name))
