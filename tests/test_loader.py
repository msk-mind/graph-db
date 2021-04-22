'''
Created on April 20, 2021

@author: pashaa@mskcc.org
'''
import json

from graph_db.common.config import ConfigSet, APP_CFG, DATA_CFG
from graph_db.extractor import extract_from_delta_table
from graph_db.loader import load_spark_dataframe


def test_load_delta_table():
    # load configs
    ConfigSet(name=APP_CFG, config_file='tests/test_app_config.yml')
    cfg = ConfigSet(name=DATA_CFG, config_file='tests/test_data_config.yml')

    # get list of delta tables to load
    tables = cfg.get_value(DATA_CFG + '::$.load')

    for table in tables:

        # convert string table definition to dict
        table = table['delta_table'].replace("'", '"')
        table = json.loads(table)

        # extract table attributes
        path = table['path']
        label = table['label']
        primary_keys = table['primary_keys']
        properties = table['properties']

        # load dataframe
        df = extract_from_delta_table(path)

        # load table into neo4j
        load_spark_dataframe(df, label, primary_keys, properties)