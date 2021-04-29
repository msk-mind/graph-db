"""Console script for graph_db."""
import json
import sys
import click

from graph_db.common.config import ConfigSet, DATA_CFG, APP_CFG
from graph_db.common.custom_logger import init_logger
from graph_db.extractor import extract_from_delta_table

logger = init_logger()

@click.command()
@click.option('-d', '--data_config_file', default=None, type=click.Path(exists=True),
              help="path to yaml file containing data input and output parameters. "
                   "See ./data_config.yaml.template")
@click.option('-a', '--app_config_file', default='config.yaml', type=click.Path(exists=True),
              help="path to yaml file containing application runtime parameters. "
                   "See ./app_config.yaml.template")
def main(data_config_file, app_config_file):
    """Console script for graph_db."""
    logger.info('data config: ' + data_config_file)
    logger.info('app config: ' + app_config_file)

    # load configs
    ConfigSet(name=DATA_CFG, config_file=data_config_file)
    cfg = ConfigSet(name=APP_CFG, config_file=app_config_file)

    # extract delta tables
    tables = cfg.get_value(DATA_CFG + '::$.load_delta')

    for table in tables:
        path = table['path']

        extract_from_delta_table(path)

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
