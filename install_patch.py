import pywren_ibm_cloud
from distutils.dir_util import copy_tree
import os

base_path = os.path.dirname(pywren_ibm_cloud.__file__)

copy_tree('patch', base_path)

print('Triggerflow patch for IBM-Pywren applied in {}'.format(base_path))
