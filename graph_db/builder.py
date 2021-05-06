"""Console script for graph_db."""
import os
import shutil
import sys
import click
from graph_db.common.neo4j_connection import Neo4jConnection

from graph_db.common.config import ConfigSet, DATA_CFG, APP_CFG
from graph_db.common.custom_logger import init_logger
from graph_db.extractor import delta_to_json

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

    # get neo4j import dir
    neo4j_import_dir = cfg.get_value(DATA_CFG + '::$.neo4j_import_dir')

    # extract delta tables
    tables = cfg.get_value(DATA_CFG + '::$.delta_to_json')
    for table in tables:
        delta_path = table['path']
        logger.info('processing ' + delta_path)
        json_path = os.path.join(neo4j_import_dir, os.path.basename(delta_path))
        delta_to_json(delta_path, json_path)

    # run cypher queries in cql files
    uri = cfg.get_value(path=APP_CFG + '::$.neo4j[0][uri]'),
    user = cfg.get_value(path=APP_CFG + '::$.neo4j[1][username]'),
    pwd = cfg.get_value(path=APP_CFG + '::$.neo4j[2][password]')
    conn = Neo4jConnection(uri=uri[0], user=user[0],
                           pwd=pwd)  # todo: remove tuple for uri and user. not sure why they are tuples

    cyphers = cfg.get_value(DATA_CFG + '::$.cypher')

    for cypher in cyphers:
        cypher_path = cypher['path']
        logger.info('processing ' + cypher_path)

        overwrite_cypher = cfg.get_value(DATA_CFG + '::$.overwrite_cypher')

        cypher_file = os.path.basename(cypher_path)
        dest_path = os.path.join(neo4j_import_dir, cypher_file)

        if os.path.exists(dest_path):
            if overwrite_cypher:
                logger.info('overwriting ' + dest_path)
                os.remove(dest_path)
                shutil.copy(cypher_path, neo4j_import_dir)
                conn.query("call apoc.cypher.runFile('file:///{0}')".format(cypher_file))
            else:
                logger.info('skipping writing of cypher for ' + cypher_file)
        else:
            shutil.copy(cypher_path, neo4j_import_dir)
            conn.query("call apoc.cypher.runFile('file:///{0}')".format(cypher_file))



    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
