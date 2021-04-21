'''
Created on April 20, 2021

@author: pashaa@mskcc.org
'''
from graph_db.util.config import get_data_config


def test_get_data_config():
    expected = get_data_config('tests/util/data_config.yml')

    actual = [{'load_delta_table': 'file:///<path_to_delta_table>'},
              {'cipher': 'match (a:User) merge (b:UserGroup {firstName: a.firstName}) merge (a)-[:belongs_to]->(b) on create set b.count = coalesce(b.count, 0)+1 return a, b'},
              {'cipher': 'MATCH (p:Person)<-[:LIKES]-(t:Technology)'}]

    assert expected == actual