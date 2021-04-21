'''
Created on April 07, 2021

@author: pashaa@mskcc.org
'''
from graph_db.common.config import ConfigSet
from graph_db.common.neo4j_connection import Neo4jConnection
from graph_db.common.spark_session import SparkConfig

APP_CFG = 'APP_CFG'
DATA_CFG = 'DATA_CFG'

def load_delta_table(key):

    spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="load_delta_table")

    cfg = ConfigSet()

    path = cfg.get_value(DATA_CFG+'::$.load-delta-table[0]["{0}"]["path"]'.format(key))

    df = spark.read.format('delta').load(path)

    label = cfg.get_value(DATA_CFG + '::$.load-delta-table[0]["{0}"]["label"]'.format(key))
    primary_keys = cfg.get_value(DATA_CFG + '::$.load-delta-table[0]["{0}"]["primary_keys"]'.format(key))
    properties = cfg.get_value(DATA_CFG + '::$.load-delta-table[0]["{0}"]["properties"]'.format(key))
    uri = cfg.get_value(path=APP_CFG + '::$.neo4j[:1][uri]'),
    user = cfg.get_value(path=APP_CFG + '::$.neo4j[:2][username]'),
    pwd = cfg.get_value(path=APP_CFG + '::$.neo4j[:3][password]')



    broadcast_vars = spark.sparkContext.broadcast({'label': label,
                                      'primary_keys': primary_keys,
                                      'properties': properties,
                                      'uri':uri,
                                      'user': user,
                                      'pwd':pwd})

    def __write_node(row):

        # from config
        label = broadcast_vars.value['label']
        primary_keys = broadcast_vars.value['primary_keys']
        primary_keys = primary_keys.replace(" ", "").split(',')
        properties = broadcast_vars.value['properties']
        cols = properties.replace(" ", "").split(',')
        uri = broadcast_vars.value['uri']
        user = broadcast_vars.value['user']
        pwd = broadcast_vars.value['pwd']

        # build merge clause
        query = 'MERGE (x:{0} '.format(label)
        props = ''
        for key in primary_keys:
            if key == 'id':  # substitute id for uid because neo4j nodes contain an implicit id property
                props = props + "{0}: '{1}', ".format('uid', row[key])  # todo: need to take into account types
            else:
                props = props + "{0}: '{1}', ".format(key, row[key])
        query = query + '{{{0}}}) ON CREATE SET '.format(props[:-2])

        # build properties clause
        props = ''
        for col in cols:
            props = props + "x.{0} = '{1}', ".format(col, row[col])  # todo: need to take into account types

        query = query + props[:-2]

        # build return clause
        props = ''
        for key in primary_keys:
            if key == 'id':  # substitute id for uid because neo4j nodes contain an implicit id property
                props = props + 'x.{0}, '.format('uid')
            else:
                props = props + 'x.{0}, '.format(key)

        query = query + ' RETURN {0}'.format(props[:-2])

        print(uri)
        print(user)
        print(pwd)
        conn = Neo4jConnection(uri=uri[0],user=user[0],pwd=pwd)
        conn.query(query)


    df.rdd.foreach(__write_node)