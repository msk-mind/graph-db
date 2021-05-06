"""
Created on May 05, 2021

@author: pashaa@mskcc.org
"""
from graph_db.common.neo4j_connection import Neo4jConnection

from graph_db.builder import main


def test_builder(monkeypatch):
    def mock_query(*args, **kwargs):
        print('query called...')
        return 0

    monkeypatch.setattr(Neo4jConnection, "query", mock_query)

    main(['-d', 'tests/test_data_config.yml', '-a', 'tests/test_app_config.yml'], standalone_mode=False)

