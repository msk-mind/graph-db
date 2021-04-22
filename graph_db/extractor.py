'''
Created on April 22, 2021

@author: pashaa@mskcc.org
'''
from graph_db.common.config import APP_CFG
from graph_db.common.spark_session import SparkConfig


def extract_from_delta_table(path):
    '''

    :param path: path to delta table
    :return: spark dataframe
    '''

    spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="grapb_db")
    df = spark.read.format('delta').load(path)

    return df