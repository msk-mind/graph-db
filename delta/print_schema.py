'''
Created on April 25, 2021

@author: pashaa@mskcc.org
'''


import json
import sys
import click

from graph_db.common.config import ConfigSet, DATA_CFG, APP_CFG
from graph_db.common.custom_logger import init_logger
from graph_db.common.spark_session import SparkConfig

logger = init_logger()

@click.command()
@click.option('-d', '--data_config_file', default=None, type=click.Path(exists=True),
              help="path to yaml file containing data input and output parameters. "
                   "See ./data_config.yaml.template")
@click.option('-a', '--app_config_file', default='config.yaml', type=click.Path(exists=True),
              help="path to yaml file containing application runtime parameters. "
                   "See ./app_config.yaml.template")
def main(data_config_file, app_config_file):
    """Print delta table schemas."""
    logger.info('data config: ' + data_config_file)
    logger.info('app config: ' + app_config_file)

    # load configs
    ConfigSet(name=DATA_CFG, config_file=data_config_file)
    cfg = ConfigSet(name=APP_CFG, config_file=app_config_file)

    # get list of delta tables to load
    tables = cfg.get_value(DATA_CFG + '::$.load_delta')

    for table in tables:
        path = table['path']

        spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="grapb_db")
        df = spark.read.format('delta').load(path)

        df.printSchema()
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
