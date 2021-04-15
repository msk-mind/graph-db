#!/usr/bin/env python
'''
Created on April 09, 2021

@author: pashaa@mskcc.org
'''
import os

"""Tests for `graph_db` package."""
import sys

import pytest

from click.testing import CliRunner
from pyspark.sql import SparkSession

from graph_db import graph_db
from graph_db import cli
from graph_db.util.Neo4jConnection import Neo4jConnection


@pytest.fixture
def setup():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """

    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    data2 = [("James", "", "Smith", "36636", "M", 3000),
             ("Michael", "Rose", "", "40288", "M", 4000),
             ("Robert", "", "Williams", "42114", "M", 4000),
             ("Maria", "Anne", "Jones", "39192", "F", 4000),
             ("Jen", "Mary", "Brown", "36632", "F", -1)
             ]

    schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
        ])

    spark = SparkSession.builder \
        .appName('test') \
        .master('local') \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.driver.host", 'localhost') \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.executor.memory", '6g') \
        .config("spark.driver.memory", '6g') \
        .config("spark.executor.cores", '2') \
        .config("spark.cores.max", '8') \
        .config("spark.executor.pyspark.memory", '6g') \
        .config("spark.sql.shuffle.partitions", '20') \
        .config("spark.driver.maxResultSize", '4g') \
        .config("fs.defaultFS", "file:///") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    # df = spark.createDataFrame(data=data2, schema=schema)
    # df.printSchema()
    # df.show(truncate=False)
    #
    # df.write.format("delta") \
    #     .mode("overwrite") \
    #     .option("header", True) \
    #     .save("tests/input_data/user_table")

def run_query(row):
    print('>>>>')
    # from config
    label = 'User'
    primary_keys = 'id, firstname'
    primary_keys = primary_keys.replace(" ", "").split(',')
    properties = 'middlename, lastname, gender, salary'
    cols = properties.replace(" ", "").split(',')

    # build merge clause
    query = 'MERGE (x:{0} '.format(label)
    props = ''
    for key in primary_keys:
        if key == 'id':   # substitute id for uid because neo4j nodes contain an implicit id property
            props = props + "{0}: '{1}', ".format('uid', row[key]) # todo: need to take into account types
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
        if key == 'id':   # substitute id for uid because neo4j nodes contain an implicit id property
            props = props + 'x.{0}, '.format('uid')
        else:
            props = props + 'x.{0}, '.format(key)

    query = query + ' RETURN {0}'.format(props[:-2])


    # query = '''MERGE (u:{0} {{uid: {1}}})
    #                 ON CREATE SET
    #                 u.firstname = '{2}',
    #                 u.middlename = '{3}',
    #                 u.lastname = '{4}',
    #                 u.gender = '{5}',
    #                 u.salary = {6}
    #                 RETURN u.firstname;
    #              '''.format(label,
    #                         row['id'],
    #                         row['firstname'],
    #                         row['middlename'],
    #                         row['lastname'],
    #                         row['gender'],
    #                         row['salary'])

    print(query)

def test_content(setup):
    """Sample pytest test function with the pytest fixture as an argument."""
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.format('delta').load("tests/input_data/user_table")

    df.show()
    df.printSchema()

    conn = Neo4jConnection(uri='bolt://localhost:7687', user=os.environ['username'], pwd=os.environ['password'])
    # todo:
    # bring in config load
    # do schema matching for yaml
    # do syntax regex parsing for value strings
    # add neo4j connection to config
    # apply to run_query
    # strip neo4j connection class

    dtypes = df.dtypes
    df.rdd.foreach(run_query)
    sys.exit(0)




    for index, row in tuple_to_add.iterrows():
        query = '''MERGE (u:{0} {{uid: {1}}})
                    ON CREATE SET
                    u.firstname = '{2}',
                    u.middlename = '{3}',
                    u.lastname = '{4}',
                    u.gender = '{5}',
                    u.salary = {6}
                    RETURN u.firstname;
                 '''.format(label,
                            row['id'],
                            row['firstname'],
                            row['middlename'],
                            row['lastname'],
                            row['gender'],
                            row['salary'])
        conn.query(query)




