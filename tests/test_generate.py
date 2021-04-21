#!/usr/bin/env python
'''
Created on April 09, 2021

@author: pashaa@mskcc.org
'''
import os

import yaml

from graph_db.common.config import ConfigSet
from graph_db.common.spark_session import SparkConfig

"""Tests for `graph_db` package."""
import sys
import pprint as pp

import pytest

from click.testing import CliRunner
from pyspark.sql import SparkSession



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

    # spark = SparkSession.builder \
    #     .appName('test') \
    #     .master('local') \
    #     .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
    #     .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    #     .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    #     .config("spark.driver.host", 'localhost') \
    #     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    #     .config("spark.executor.memory", '6g') \
    #     .config("spark.driver.memory", '6g') \
    #     .config("spark.executor.cores", '2') \
    #     .config("spark.cores.max", '8') \
    #     .config("spark.executor.pyspark.memory", '6g') \
    #     .config("spark.sql.shuffle.partitions", '20') \
    #     .config("spark.driver.maxResultSize", '4g') \
    #     .config("fs.defaultFS", "file:///") \
    #     .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    #     .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    #     .getOrCreate()

    # APP_CFG = 'APP_CFG'
    #
    # cfg = ConfigSet(name=APP_CFG, config_file='tests/test_app_config.yml')
    # spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="test_generate")

    # df = spark.createDataFrame(data=data2, schema=schema)
    # df.printSchema()
    # df.show(truncate=False)
    #
    # df.write.format("delta") \
    #     .mode("overwrite") \
    #     .option("header", True) \
    #     .save("tests/input_data/user_table")


def test_content(setup):
    """Sample pytest test function with the pytest fixture as an argument."""
    # spark = SparkSession.builder.getOrCreate()
    #
    # df = spark.read.format('delta').load("tests/input_data/user_table")
    #
    # df.show()
    # df.printSchema()



    # conn = Neo4jConnection(uri='bolt://localhost:7687', user=os.environ['username'], pwd=os.environ['password'])
    #
    # try:
    #     stream = open('data_config.yml', 'r')
    # except IOError as err:
    #     print("Error: unable to find a config file with name config file."+ str(err))
    #     raise err
    #
    # config = next(yaml.load_all(stream, Loader=yaml.FullLoader))
    #
    # print('>>>>>'+str(config[1]['cipher']))
    # #print('>>>>>' + str(config))
    #
    # query = config[1]['cipher']





