# -*- coding: utf-8 -*-

import os
import yaml


config_path = os.getenv('MONGOSYNC_CONF') or\
    os.path.join(os.path.dirname(__file__), 'config.yaml')

with open(config_path, 'r') as f:
    conf = yaml.safe_load(f)
