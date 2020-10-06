import os

from setuptools import setup, find_packages

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

extra_requirements = {
    "testing": [
        # Used to manage testing of flask-restx
        "pytest-flask==1.*",
        "flask-restx==0.2.*",
        # Used to manage testing of Mongo
        "mongomock==3.*",
        # Used to check coverage
        "pytest-cov==2.*",
    ],
    "mongo": [
        "pymongo[tls]==3.*",
        # Used to manage date and datetime deserialization
        "iso8601==0.1.*",
    ],
    # Used to Manage Non-Mongo Database
    "sqlalchemy": [
        "marshmallow==3.*",
        "SQLAlchemy==1.*",
        "marshmallow_sqlalchemy==0.23.*",
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
    author="Colin Bounouar",
    author_email="colin.bounouar.dev@gmail.com",
    maintainer="Colin Bounouar",
    maintainer_email="colin.bounouar.dev@gmail.com",
    url="https://colin-b.github.io/layabase/",
    description="Database for layab",
    long_description=long_description,
    long_description_content_type="text/markdown",
    download_url="https://pypi.org/project/layabase/",
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
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Build Tools",
    ],
    keywords=["mongo", "sqla", "db", "flask"],
    packages=find_packages(exclude=["tests*"]),
    install_requires=[],
    extras_require=extra_requirements,
    python_requires=">=3.6",
    project_urls={
        "GitHub": "https://github.com/Colin-b/layabase",
        "Changelog": "https://github.com/Colin-b/layabase/blob/master/CHANGELOG.md",
        "Issues": "https://github.com/Colin-b/layabase/issues",
    },
    platforms=["Windows", "Linux"],
)
