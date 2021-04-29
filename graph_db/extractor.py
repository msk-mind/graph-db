'''
Created on April 22, 2021

@author: pashaa@mskcc.org
'''
import os
from graph_db.common.config import APP_CFG, ConfigSet, DATA_CFG
from graph_db.common.custom_logger import init_logger
from graph_db.common.spark_session import SparkConfig

logger = init_logger()

def extract_from_delta_table(path):
    '''

    :param path: path to delta table
    :return: spark dataframe
    '''

    # read table
    logger.info('extracting delta table '+path)
    spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="grapb_db")
    df = spark.read.format('delta').load(path)
    logger.info('dataframe has ' + str(df.count()) + ' records')

    logger.info('preparing to write json for '+str(df.count())+' records')

    # setup temp dir
    cfg = ConfigSet()
    tmp_dir = cfg.get_value(DATA_CFG + '::$.temp_dir')
    dest_path = os.path.join(tmp_dir,
                             os.path.basename(path+'.json'))

    df.coalesce(1).write.format('json').save(dest_path)
