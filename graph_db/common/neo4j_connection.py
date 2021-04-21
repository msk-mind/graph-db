'''
Created on April 14, 2021

@author: pashaa@mskcc.org
'''

from neo4j import GraphDatabase
from neo4j import __version__ as neo4j_version

from pyspark.sql.types import StringType, StructType, StructField
import re


def pretty_path(path):
    to_print = ''
    for i, x in enumerate(path):
        if type(x) == dict:
            node_desc = '(' + ','.join([key + ":" + x[key] for key in x.keys()]) + ')'
            if i == 0:
                to_print += "SOURCE:" + node_desc  # First
            elif i == (len(path) - 1):
                to_print += "SINK:" + node_desc  # Last
            else:
                to_print += node_desc  # Middle
        if type(x) == str:
            to_print += '-[' + x + ']-'
    return to_print


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as ex:
            print("Failed to create the driver: ", ex)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        """
        Runs a cyper query against the initalized driver
        """
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

    def test_connection(self):
        try:
            self.__driver.session().run("MATCH () RETURN 1 LIMIT 1")
            return True
        except Exception:
            return False