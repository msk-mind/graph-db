'''
Created on April 20, 2021

@author: pashaa@mskcc.org
'''
import json

from graph_db.common.config import ConfigSet
from graph_db.loader import load_delta_table

APP_CFG = 'APP_CFG'
DATA_CFG = 'DATA_CFG'

def test_load_delta_table():
    # load spark session
    ConfigSet(name=APP_CFG, config_file='tests/test_app_config.yml')
    cfg = ConfigSet(name=DATA_CFG, config_file='tests/test_data_config.yml')


    #load_delta_table(key='user_table')


    tables = cfg.get_value(DATA_CFG + '::$.load')

    print(tables)

    # label = cfg.get_value(DATA_CFG + '::$.load-delta-table[0]["{0}"]["label"]'.format(key))
    # primary_keys = cfg.get_value(DATA_CFG + '::$.load-delta-table[0]["{0}"]["primary_keys"]'.format(key))
    # properties = cfg.get_value(DATA_CFG + '::$.load-delta-table[0]["{0}"]["properties"]'.format(key))

    for table in tables:

        # convert string table definition to dict
        table = table['delta_table'].replace("'", '"')
        table = json.loads(table)

        # extract table attributes
        path = table['path']
        label = table['label']
        primary_keys = table['primary_keys']
        properties = table['properties']

        # load table into neo4j
        load_delta_table(path, label, primary_keys, properties)