'''
Created on April 22, 2021

@author: pashaa@mskcc.org
'''
import glob
import os
import shutil

from graph_db.common.config import APP_CFG, ConfigSet, DATA_CFG
from graph_db.common.custom_logger import init_logger
from graph_db.common.spark_session import SparkConfig


logger = init_logger()

def get_df(delta_path):
    """
    Read delta table from the specified delta_path and return the dataframe.

    :param delta_path: path to delta_table
    :return: spark dataframe
    """
    logger.info('extracting delta table ' + delta_path)
    spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="grapb_db")
    df = spark.read.format('delta').load(delta_path)
    logger.info('dataframe has ' + str(df.count()) + ' records')

    return df


def df_to_json(df, json_path):
    """
    Convert specified spark dataframe to json and write to the specified json_path.

    :param df: spark dataframe to convert
    :param json_path: path where json should be written
    """
    df.coalesce(1).write.format('json').save(json_path)
    json_file = glob.glob(json_path+'/*.json')[0]
    new_json_file = json_path+'/part.json'
    os.rename(json_file, new_json_file)
    logger.info('wrote json for ' + str(df.count()) + ' records to '+new_json_file)


def delta_to_json(delta_path, json_path):
    """
    Read delta table and write to json file.

    :param delta_path: path to delta table
    :param json_path: path to json table
    """
    cfg = ConfigSet()
    overwrite_json = cfg.get_value(DATA_CFG + '::$.overwrite_json')

    if os.path.exists(json_path):
        if overwrite_json:
            logger.info('overwriting ' + json_path)
            shutil.rmtree(json_path)
            df = get_df(delta_path)
            df_to_json(df, json_path)
        else:
            logger.info('skipping writing of json for ' + delta_path)
    else:
        df = get_df(delta_path)
        df_to_json(df, json_path)
