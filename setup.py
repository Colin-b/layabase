import os

from setuptools import setup, find_packages

from pycommon_database._version import __version__

this_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(this_dir, 'README.md'), 'r') as f:
    long_description = f.read()

setup(
    name='pycommon_database',
    version=__version__,
    description="Common Database handling",
    long_description=long_description,
    packages=find_packages(exclude=[
        'test',
    ]),
    install_requires=[
        # Used to handle errors
        'pycommon-error==2.0.0'
    ],
    extras_require={
        'testing': [
            # Used to run tests
            'nose==1.3.7',
            # Used to provide testing help
            'pycommon-test==1.15.2',
            # Used to Manage Mongo Database
            'mongomock==3.13.0',
        ],
        # Used to Manage Mongo Database
        'mongo': [
            'pymongo[tls]==3.7.2',
            # Used to manage date and datetime deserialization
            'python-dateutil==2.7.5',
        ],
        # Used to Manage Non-Mongo Database
        'sqlalchemy': [
            'marshmallow==2.16.1',
            'SQLAlchemy==1.2.12',
            'marshmallow_sqlalchemy==0.14.1',
        ],
    },
)
