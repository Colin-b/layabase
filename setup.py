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
        'flask-restplus',
        # Used to Manage Database
        'marshmallow_sqlalchemy',
    ],
    extras_require={
        'testing': [
        ],
    },
)
