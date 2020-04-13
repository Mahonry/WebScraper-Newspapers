import yaml
import os

__config = None 
file_path = os.path.join(os.path.dirname(__file__),"config.yaml")



def config():
    global __config
    if not __config:
        with open(file_path, mode = 'r') as f:
            __config = yaml.load(f)
    return __config


