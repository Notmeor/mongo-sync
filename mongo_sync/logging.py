# -*- coding: utf-8 -*-

import os
import logging.config

import yaml


def setup_logging(
    default_path=None,
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """
    Setup logging configuration
    """
    if default_path:
        path = default_path
    else:
        path = os.path.join(os.path.dirname(__file__), default_path)
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())['logging']
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
