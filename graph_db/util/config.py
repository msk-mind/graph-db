'''
Created on April 20, 2021

@author: pashaa@mskcc.org
'''
import yaml


def get_data_config(path):

    try:
        stream = open(path, 'r')
    except IOError as err:
        print("Error: unable to find a config file "+ str(err))
        raise err

    return next(yaml.load_all(stream, Loader=yaml.FullLoader))