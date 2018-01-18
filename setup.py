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
        # Used to Manage Database
        'marshmallow_sqlalchemy==0.13.2',
    ],
    extras_require={
        'testing': [
        'nose'],

    },
)
