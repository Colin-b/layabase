import os

from setuptools import setup, find_packages

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

extra_requirements = {
    "testing": [
        # Used to provide testing help
        "pycommon-test==5.2.0",
        "mongomock==3.16.0",
    ],
    "mongo": [
        "pymongo[tls]==3.7.2",
        # Used to manage date and datetime deserialization
        "iso8601==0.1.12",
    ],
    # Used to Manage Non-Mongo Database
    "sqlalchemy": [
        "marshmallow==2.19.2",
        "SQLAlchemy==1.3.1",
        "marshmallow_sqlalchemy==0.16.1",
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
    version=open("pycommon_database/version.py")
    .readlines()[-1]
    .split()[-1]
    .strip("\"'"),
    description="Common Database handling",
    long_description=long_description,
    packages=find_packages(exclude=["test"]),
    install_requires=[
        # Used to handle errors
        "pycommon-error==2.17.0"
    ],
    extras_require=extra_requirements,
    python_requires=">=3.6",
    project_urls={
        "Changelog": "https://github.tools.digital.engie.com/GEM-Py/pycommon-database/blob/development/CHANGELOG.md",
        "Issues": "https://github.tools.digital.engie.com/GEM-Py/pycommon-database/issues",
    },
)
