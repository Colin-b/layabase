import os

from setuptools import setup, find_packages
from pycommon_database.version import __version__

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

extra_requirements = {
    "testing": [
        # Used to provide testing help
        "pycommon_test==11.0.0",
        # Used to manage testing of Mongo
        "mongomock==3.17.0",
    ],
    "mongo": [
        "pymongo[tls]==3.8.0",
        # Used to manage date and datetime deserialization
        "iso8601==0.1.12",
    ],
    # Used to Manage Non-Mongo Database
    "sqlalchemy": [
        "marshmallow==2.19.5",
        "SQLAlchemy==1.3.5",
        "marshmallow_sqlalchemy==0.17.0",
    ],
}

# Add all extra requirements to testing
extra_requirements["testing"] += [
    extra
    for extra_name, extra_list in extra_requirements.items()
    if extra_name != "testing"
    for extra in extra_list
]

setup(
    name="pycommon_database",
    version=__version__,
    description="Common Database handling",
    long_description=long_description,
    packages=find_packages(exclude=["test"]),
    install_requires=[
        # Used to handle errors
        "pycommon_error==2.20.0"
    ],
    extras_require=extra_requirements,
    python_requires=">=3.6",
    project_urls={
        "Changelog": "https://github.tools.digital.engie.com/GEM-Py/pycommon-database/blob/development/CHANGELOG.md",
        "Issues": "https://github.tools.digital.engie.com/GEM-Py/pycommon-database/issues",
    },
)
