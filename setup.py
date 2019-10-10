import os

from setuptools import setup, find_packages

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

extra_requirements = {
    "testing": [
        # Used to run test cases
        "pytest-flask==0.15.0",
        # Used to manage testing of Mongo
        "mongomock==3.18.0",
    ],
    "mongo": [
        "pymongo[tls]==3.9.0",
        # Used to manage date and datetime deserialization
        "iso8601==0.1.12",
    ],
    # Used to Manage Non-Mongo Database
    "sqlalchemy": [
        "marshmallow==3.2.1",
        "SQLAlchemy==1.3.10",
        "marshmallow_sqlalchemy==0.19.0",
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
    name="layabase",
    version=open("layabase/version.py").readlines()[-1].split()[-1].strip("\"'"),
    description="Database for layab",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["test"]),
    install_requires=[
        # Used to handle errors
        "layaberr==2.0.0"
    ],
    extras_require=extra_requirements,
    python_requires=">=3.6",
    project_urls={
        "Changelog": "https://github.tools.digital.engie.com/gempy/layabase/blob/master/CHANGELOG.md",
        "Issues": "https://github.tools.digital.engie.com/gempy/layabase/issues",
    },
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Build Tools",
    ],
    keywords=["mongo", "sqla", "db", "flask"],
    platforms=["Windows", "Linux"],
)
