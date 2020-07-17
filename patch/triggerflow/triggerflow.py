import os
import sys
import copy
import logging
import inspect
import zipfile

import pywren_ibm_cloud
from pywren_ibm_cloud.libs.openwhisk.client import OpenWhiskClient
from pywren_ibm_cloud.utils import create_executor_id
from pywren_ibm_cloud.config import default_config

logger = logging.getLogger(__name__)


FH_ZIP_LOCATION = os.path.join(os.getcwd(), 'pywren_ibmcf.zip')
RUNTIME_DEFAULT = {'3.6': 'triggerflow/ibm_cloud_functions_runtime-v36',
                   '3.7': 'triggerflow/ibm_cloud_functions_runtime-v37',
                   '3.8': 'triggerflow/ibm_cloud_functions_runtime-v38'}

MAIN_FN_MEMORY = 256
MAIN_FN_TIMEOUT = 30


class TriggerflowExecutor:

    def __init__(self, config=None):
        """
        Initialize a FunctionExecutor class.

        :param config: Settings passed in here will override those in config file. Default None.
        :param runtime: Runtime name to use. Default None.
        """

        self.config = config
        self.ow_client = OpenWhiskClient(**self.config['ibm_cf'])
        self.ow_client.create_package('triggerflow')

        python_version = "{}.{}".format(sys.version_info[0], sys.version_info[1])
        self.default_runtime = RUNTIME_DEFAULT[python_version]
        logger.info('TriggerflowExecutor created')

    def _create_function_handler_zip(self, main_exec_file):

        logger.debug("Creating function handler zip in {}".format(FH_ZIP_LOCATION))

        def add_folder_to_zip(zip_file, full_dir_path, sub_dir=''):
            for file in os.listdir(full_dir_path):
                full_path = os.path.join(full_dir_path, file)
                if os.path.isfile(full_path):
                    zip_file.write(full_path, os.path.join('pywren_ibm_cloud', sub_dir, file))
                elif os.path.isdir(full_path) and '__pycache__' not in full_path:
                    add_folder_to_zip(zip_file, full_path, os.path.join(sub_dir, file))

        try:
            with zipfile.ZipFile(FH_ZIP_LOCATION, 'w', zipfile.ZIP_DEFLATED) as pywren_zip:
                module_location = os.path.dirname(os.path.abspath(pywren_ibm_cloud.__file__))
                pywren_zip.write(main_exec_file, '__main__.py')
                add_folder_to_zip(pywren_zip, module_location)
        except Exception:
            raise Exception('Unable to create the {} package: {}'.format(FH_ZIP_LOCATION))

    def _delete_function_handler_zip(self):
        os.remove(FH_ZIP_LOCATION)

    def run(self, coordinator_function, name, runtime=None):
        assert coordinator_function.__name__ == 'main', "Coordinator Function must have 'main' name"

        file_path = os.path.abspath(inspect.getfile(coordinator_function))
        runtime = runtime or self.default_runtime

        self._create_function_handler_zip(file_path)
        with open(FH_ZIP_LOCATION, "rb") as action_zip:
            action_bin = action_zip.read()
            self.ow_client.create_action('triggerflow', name,
                                         image_name=runtime,
                                         code=action_bin,
                                         memory=MAIN_FN_MEMORY,
                                         is_binary=True,
                                         timeout=MAIN_FN_TIMEOUT*1000)
        self._delete_function_handler_zip()

        payload = {'config': self.config, 'execution_id': None, 'start_time': 0, 'runtime': runtime}
        self.ow_client.invoke('triggerflow', name, payload)
        logger.info('Coordinator function {} invoked '.format(name))
