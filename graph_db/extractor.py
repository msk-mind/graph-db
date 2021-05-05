'''
Created on April 22, 2021

@author: pashaa@mskcc.org
'''
import os
from graph_db.common.config import APP_CFG
from graph_db.common.custom_logger import init_logger
from graph_db.common.spark_session import SparkConfig


logger = init_logger()


def delta_to_json(delta_path, json_path):
    '''
    Read delta table and write to json file.

    :param delta_path: path to delta table
    :param json_path: path to json table
    '''

    # read table
    logger.info('extracting delta table ' + delta_path)
    spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="grapb_db")
    df = spark.read.format('delta').load(delta_path)
    logger.info('dataframe has ' + str(df.count()) + ' records')

    # write table
    logger.info('preparing to write json for '+str(df.count())+' records')
    df.coalesce(1).write.format('json').save(json_path)
