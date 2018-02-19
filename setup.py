from setuptools import setup, find_packages

from pycommon_database._version import __version__
setup(
    name='pycommon_database',
    version=__version__,
    packages=find_packages(exclude=[
        'test',
    ]),
    install_requires=[
        # Used to manage fields
        'flask-restplus==0.10.1',
    ],
    extras_require={
        'testing': [
            'nose',
            # Used to Manage Mongo Database
            'mongomock',
            # Test without DB
            'sqlalchemy'
        ],
        # Used to Manage Mongo Database
        'mongo': [
            'pymongo==3.6.0',
            # Used to manage date and datetime deserialization
            'python-dateutil==2.6.1',
        ],
        # Used to Manage Non-Mongo Database
        'sqlalchemy': [
            'marshmallow_sqlalchemy==0.13.2',
        ],
    },
)
