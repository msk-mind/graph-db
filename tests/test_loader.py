'''
Created on April 20, 2021

@author: pashaa@mskcc.org
'''
from graph_db.common.config import ConfigSet
from graph_db.loader import load_delta_table

APP_CFG = 'APP_CFG'
DATA_CFG = 'DATA_CFG'

def test_load_delta_table():
    # load spark session
    ConfigSet(name=APP_CFG, config_file='tests/test_app_config.yml')
    ConfigSet(name=DATA_CFG, config_file='tests/test_data_config.yml')


    load_delta_table(key='user_table')