#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='mongo_sync',
    version='0.1.0',
    packages=find_packages(include=["mongo_sync"]),
    author='notmeor',
    author_email='kevin.inova@gmail.com',
    description='',
    entry_points={
        'console_scripts': ['mongo-sync = mongo_sync.main:main'],
    },
    install_requires=['pymongo']
)
