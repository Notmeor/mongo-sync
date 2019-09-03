# -*- coding: utf-8 -*-

import os
import yaml
import logging
import logging.config


config_path = os.environ['MONGOSYNC_CONF']

with open(config_path, 'r') as f:
    conf = yaml.safe_load(f)


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    try:
        config = conf['logging']
        logging.config.dictConfig(config)
    except:
        raise
    else:
        logging.basicConfig(level=default_level)