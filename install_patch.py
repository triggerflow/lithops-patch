import lithops
from distutils.dir_util import copy_tree
import os

base_path = os.path.dirname(lithops.__file__)

copy_tree('patch', base_path)

print('Triggerflow patch for lithops applied in {}'.format(base_path))
