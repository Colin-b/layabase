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
        # Used to manage fields
        'flask-restplus==0.11.0',
    ],
    extras_require={
        'testing': [
            'nose',
            'pycommon-test==1.9.0',
            # Used to Manage Mongo Database
            'mongomock==3.10.0',
        ],
        # Used to Manage Mongo Database
        'mongo': [
            'pymongo[tls]==3.6.1',
            # Used to manage date and datetime deserialization
            'python-dateutil==2.7.3',
        ],
        # Used to Manage Non-Mongo Database
        'sqlalchemy': [
            'marshmallow==2.15.4',
            'SQLAlchemy==1.2.10',
            'marshmallow_sqlalchemy==0.14.0',
        ],
    },
)
