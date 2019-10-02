<h2 align="center">Database for layab</h2>

<p align="center">
<a href='https://github.tools.digital.engie.com/gempy/layabase/releases/latest'><img src='https://pse.tools.digital.engie.com/drm-all.gem/buildStatus/icon?job=team/layabase/master&config=version'></a>
<a href='https://pse.tools.digital.engie.com/drm-all.gem/job/team/view/Python%20modules/job/layabase/job/master/'><img src='https://pse.tools.digital.engie.com/drm-all.gem/buildStatus/icon?job=team/layabase/master'></a>
<a href='https://pse.tools.digital.engie.com/drm-all.gem/job/team/view/Python%20modules/job/layabase/job/master/cobertura/'><img src='https://pse.tools.digital.engie.com/drm-all.gem/buildStatus/icon?job=team/layabase/master&config=testCoverage'></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href='https://pse.tools.digital.engie.com/drm-all.gem/job/team/view/Python%20modules/job/layabase/job/master/lastSuccessfulBuild/testReport/'><img src='https://pse.tools.digital.engie.com/drm-all.gem/buildStatus/icon?job=team/layabase/master&config=testCount'></a>
</p>

Query your databases easily and transparently thanks to this module providing helpers on top of most brilliant python
database modules (SQLAlchemy and PyMongo).

## Table of Contents

  * [Features](#features)
  * [How to use](#usage)
  * [Technical documentation](#technical)

## Features

Features:

- Audit
  - Automatic audit support (if `layabauth` python module is used, user will be logged as well)
- Rollback
  - Automatic rollback support (when history is activated)
- History
  - Automatic history management
- Validation
  - Enforce proper values are received (type, restricted choices, required fields)
- Conversion
  - Converting JSON received data to appropriate database data type
  - Converting Database data type to JSON
- Health check
- Smart queries
  - HTTP query parameters are extracted and converted from HTTP query arguments
    - Special parameter: order_by (__Feature not available for mongo__)
    - Special parameter: limit
    - Special parameter: offset
  - Query on multiple equality via `field=value1&field=value2`
  - Query on excluded intervals via `field=>value1&field=<value2` (__Feature not yet available for sqla__)
  - Query on included intervals via `field=>=value1&field=<=value2` (__Feature not yet available for sqla__)
  - Query on restricted values via `field=!=value1&field=!=value2` (__Feature not yet available__)
  - Query via a mix of all those features if needed as long as it make sense to you
  - Query regex thanks to `*` character via `field=v*lue` (__Feature not yet available for mongo__) 

## Usage

You will define a class to help you with the manipulation of:

 * A collection if this is a MongoDB you are connecting to.
 * A table if this is a non-Mongo database you are connecting to.

This class will describe:

 * The document fields if this is a MongoDB you are connecting to.
 * The table columns if this is a non-Mongo database you are connecting to.

By providing this class to a layabase.CRUDController instance, you will automatically have all features described in the previous section.

## CRUD Controller

layabase.CRUDController provides C.R.U.D. methods (and more, as listed in features) on a specified table or mongo collection.

### Controller definition

```python
import layabase

# This will be the class describing your table or collection as defined in Table or Collection sections afterwards
table_or_collection = None 

controller = layabase.CRUDController(table_or_collection)
```

### Controller features

#### Retrieving data

You can retrieve a list of rows or documents described as dictionaries:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

all_rows_or_documents = controller.get({})

filtered_rows_or_documents = controller.get({"value": 'value1'})
```

You can retrieve a single row or document described as dictionary:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

row_or_document = controller.get_one({"value": 'value1'})
```

#### Inserting data

You can insert many rows or documents at once using dictionary representation:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

inserted_rows_or_documents = controller.post_many([
    {'key': 'key1', 'value': 'value1'},
    {'key': 'key2', 'value': 'value2'},
])
```

You can insert a single row or document using dictionary representation:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

inserted_row_or_document = controller.post({'key': 'key1', 'value': 'value1'})
```

#### Updating data

You can update many rows or documents at once using (partial) dictionary representation:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

updated_rows_or_documents = controller.put_many([{'key': 'key1', 'value': 'new value1'}, {'key': 'key2', 'value': 'new value2'}])
```

You can update a single row or document using (partial) dictionary representation:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

updated_row_or_document = controller.put({'key': 'key1', 'value': 'new value1'})
```

#### Removing data

You can remove a subset of rows or documents:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

nb_removed_rows_or_documents = controller.delete({"key": 'key1'})
```

You can remove all rows or documents:

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

nb_removed_rows_or_documents = controller.delete({})
```

#### Retrieving table or collection mapping

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

# non mongo description = {'table': 'MyTable', 'key': 'key', 'value': 'value'}
# mongo description = {'table': 'MyCollection', 'key': 'key', 'value': 'value'}
description = controller.get_model_description()
```

#### Auditing

```python
import layabase

# This will be the controller as created in Controller definition section
controller: layabase.CRUDController = None

all_audit_models_as_dict_list = controller.get_audit({})

filtered_audit_models_as_dict_list = controller.get_audit({"value": 'value1'})
```

## Link to a database

### Link to a Mongo database

```python
import layabase


# Should be a list of CRUDController inherited classes
my_controllers = []
layabase.load("mongodb://host:port/server_name", my_controllers)
```

### Link to a Mongo in-memory database

```python
import layabase


# Should be a list of CRUDController inherited classes
my_controllers = []
layabase.load("mongomock", my_controllers)
```

### Link to a non Mongo database

```python
import layabase


# Should be a list of CRUDController inherited classes
my_controllers = []
layabase.load("your_connection_string", my_controllers)
```

## Relational databases (non-Mongo)

SQLAlchemy is the underlying framework used to manipulate relational databases.

To create a representation of a table you will need to create a [Mixin](https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/mixins.html#declarative-mixins).

### Table

```python
from sqlalchemy import Column, String

class MyTable:
    __tablename__ = "my_table"

    key = Column(String, primary_key=True)
    # info can be used to provide information to marshmallow, such as requiring the field to be set on queries or interpreting star character as like
    value = Column(String, info={'marshmallow': {"required_on_query": True, "interpret_star_character": True}})
```

## MongoDB (non-relational)

PyMongo is the underlying framework used to manipulate MongoDB.

To create a representation of a collection you will need to create a Mixin class.

To link your model to the underlying collection, you will need to provide a connection string.

### Collection

```python
from layabase.database_mongo import Column

class MyCollection:
    __collection_name__ = "my_collection"

    key = Column(str, is_primary_key=True)
    dict_value = Column(dict)
```

##### String fields

Fields containing string can be described using layabase.database_mongo.Column

```python
from layabase.database_mongo import Column

class MyCollection:
    __collection_name__ = "my_collection"

    key = Column()
```

As string is considered as the default field type, not providing the type explicitly when creating a column is valid.

The following parameters can also be provided when creating a column of string type:

<table>
    <th>
        <td><em>Description</em></td>
        <td><em>Default value</em></td>
    </th>
    <tr>
        <td><strong>choices</strong></td>
        <td>Restrict valid values. Should be a list of string or a function (without parameters) returning a list of string.</td>
        <td>None (unrestricted)</td>
    </tr>
    <tr>
        <td><strong>default_value</strong></td>
        <td>Default field value returned to the client if field is not set. Should be a string or a function (with dictionary as parameter) returning a string.</td>
        <td>None</td>
    </tr>
    <tr>
        <td><strong>description</strong></td>
        <td>Field description used in OpenAPI definition and in error messages. Should be a string value.</td>
        <td>None</td>
    </tr>
    <tr>
        <td><strong>index_type</strong></td>
        <td>If and how this field should be indexed. Value should be one of IndexType enum.</td>
        <td>None (not indexed)</td>
    </tr>
    <tr>
        <td><strong>allow_none_as_filter</strong></td>
        <td>If None value should be kept in queries (GET/DELETE). Should be a boolean value.</td>
        <td>False (remove None from queries)</td>
    </tr>
    <tr>
        <td><strong>is_primary_key</strong></td>
        <td>If this field value is not allowed to be modified after insert. Should be a boolean value.</td>
        <td>False (field value can always be modified)</td>
    </tr>
    <tr>
        <td><strong>is_nullable</strong></td>
        <td>If field value is optional. Should be a boolean value. Note that it is not allowed to force False if field has a default value.</td>
        <td>
        Default to True if field is not a primary key.
        Default to True if field has a default value.
        Otherwise default to False.</td>
    </tr>
    <tr>
        <td><strong>is_required</strong></td>
        <td>If field value must be specified in client requests. Use it to avoid heavy requests. Should be a boolean value.</td>
        <td>False (optional)</td>
    </tr>
    <tr>
        <td><strong>min_length</strong></td>
        <td>Minimum value length.</td>
        <td>None (no minimum length)</td>
    </tr>
    <tr>
        <td><strong>max_length</strong></td>
        <td>Maximum value length.</td>
        <td>None (no maximum length)</td>
    </tr>
    <tr>
        <td><strong>allow_comparison_signs</strong></td>
        <td>If field value should be interpreted to extract >, >=, <, <= prefix.</td>
        <td>False (value is kept as provided for equlity comparison)</td>
    </tr>
</table>

##### Dictionary fields

Fields containing a dictionary can be described using layabase.database_mongo.DictColumn

##### List fields

Fields containing a list can be described using layabase.database_mongo.ListColumn

## How to install
1. [python 3.7+](https://www.python.org/downloads/) must be installed
2. Use pip to install module:
```sh
python -m pip install layabase -i https://all-team-remote:tBa%40W%29tvB%5E%3C%3B2Jm3@artifactory.tools.digital.engie.com/artifactory/api/pypi/all-team-pypi-prod/simple
```

Note that depending on what you want to connect to, you will have to use a different module name than layabase:
* Mongo database: layabase[mongo]
* Mongo in-memory database: layabase mongomock
* Other database: layabase[sqlalchemy]

## Technical

 * [Requirements](#developer-requirements)
 * [Usage](#developer-usage)

## Developer-Requirements

Industrial valuation requires the following to run:

  * [python 3.7+](https://www.python.org/downloads/)

## Developer-Usage

## Installation

layabase is easiest to work with when installed into a virtual environment using the setup.py.

To install all test required dependencies, use the following command:

```python
python -m pip install .[testing]
```
