import os

from setuptools import setup, find_packages

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

extra_requirements = {
    "testing": [
        # Used to provide testing help
        "pycommon-test==5.0.1"
    ],
    "mongo": [
        "pymongo[tls]==3.7.2",
        # Used to manage date and datetime deserialization
        "iso8601==0.1.12",
    ],
    # Used to Manage Non-Mongo Database
    "sqlalchemy": [
        "marshmallow==2.18.0",
        "SQLAlchemy==1.2.16",
        "marshmallow_sqlalchemy==0.15.0",
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
    version=open("pycommon_database/_version.py")
    .readlines()[-1]
    .split()[-1]
    .strip("\"'"),
    description="Common Database handling",
    long_description=long_description,
    packages=find_packages(exclude=["test"]),
    install_requires=[
        # Used to handle errors
        "pycommon-error==2.16.0"
    ],
    extras_require=extra_requirements,
)
