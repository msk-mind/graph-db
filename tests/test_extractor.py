'''
Created on May 05, 2021

@author: pashaa@mskcc.org
'''
import glob
import json
import os

import pandas as pd
import shutil

import pytest
from graph_db.common.config import ConfigSet, APP_CFG
from graph_db.extractor import delta_to_json

delta_table = 'tests/input_data/user_table'
json_path = 'tests/output_data/user_table'

@pytest.fixture
def setup():
    if os.path.exists(json_path):
        shutil.rmtree(json_path)
    cfg = ConfigSet(name=APP_CFG, config_file='tests/test_app_config.yml')


def test_delta_to_json(setup):

    delta_to_json(delta_table, json_path)

    json_file = glob.glob(json_path+'/*.json')[0]

    df = pd.read_json(json_file, lines=True)

    assert df.shape == (5,6)


