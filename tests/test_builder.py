"""
Created on May 05, 2021

@author: pashaa@mskcc.org
"""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, MapType

from graph_db.common.config import APP_CFG, ConfigSet, DATA_CFG
from graph_db.common.spark_session import SparkConfig


def test_builder():
    ConfigSet(name=APP_CFG, config_file='tests/test_app_config.yml')
    cfg = ConfigSet(name=DATA_CFG, config_file='tests/test_data_config.yml')

    schema = StructType([
        StructField("path", StringType(), True),
        StructField("modificationTime", TimestampType(), True),
        StructField("length", LongType(), True),
        StructField("dicom_record_uuid", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])

    spark = SparkConfig().spark_session(config_name=APP_CFG, app_name="grapb_db")
    # df = spark.read.schema(schema).option("multiline","true").json("tests/input_data/ct_sample.json").cache()
    # df.printSchema()
    # df.show(truncate=False)
    #
    # df.write.format("delta") \
    #     .mode("overwrite") \
    #     .option("header", True) \
    #     .save("tests/input_data/CT_table")

    schema = StructType([
        StructField("path", StringType(), True),
        StructField("modificationTime", TimestampType(), True),
        StructField("length", LongType(), True),
        StructField("AccessionNumber", StringType(), True),
        StructField("scan_annotation_record_uuid", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])

    df = spark.read.schema(schema).option("multiline", "true").json("tests/input_data/mha_sample.json").cache()
    df.printSchema()
    df.show(truncate=False)

    df.write.format("delta") \
        .mode("overwrite") \
        .option("header", True) \
        .save("tests/input_data/MHA_table")

