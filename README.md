<h2 align="center">Database for layab</h2>

<p align="center">
<a href="https://pypi.org/project/layabase/"><img alt="pypi version" src="https://img.shields.io/pypi/v/layabase"></a>
<a href="https://travis-ci.org/Colin-b/layabase"><img alt="Build status" src="https://api.travis-ci.org/Colin-b/layabase.svg?branch=develop"></a>
<a href="https://travis-ci.org/Colin-b/layabase"><img alt="Coverage" src="https://img.shields.io/badge/coverage-100%25-brightgreen"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://travis-ci.org/Colin-b/layabase"><img alt="Number of tests" src="https://img.shields.io/badge/tests-702 passed-blue"></a>
<a href="https://pypi.org/project/layabase/"><img alt="Number of downloads" src="https://img.shields.io/pypi/dm/layabase"></a>
</p>

Query your databases easily and transparently thanks to this module providing helpers on top of most brilliant python
database modules ([SQLAlchemy](https://docs.sqlalchemy.org) and [PyMongo](https://api.mongodb.com/python/current/)).

## Table of Contents

  * [Features](#features)
  * [How to use](#usage)
  * [Technical documentation](#technical)

## Features

Features:

- Audit
  - Automatic audit support (if [`layabauth`](https://pypi.org/project/layabauth/) python module is used, user will be logged as well)
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
  - Query on excluded intervals via `field=>value1&field=<value2`
  - Query on included intervals via `field=>=value1&field=<=value2`
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

[SQLAlchemy](https://docs.sqlalchemy.org) is the underlying framework used to manipulate relational databases.

To create a representation of a table you will need to create a [Mixin](https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/mixins.html#declarative-mixins).

### Table

You can add extra information to a column thanks to the info parameter.

If the field should be required on queries: 
```python
info={'marshmallow': {"required_on_query": True}}
```

If `*` character in queries values should be interpreted as any characters: 
```python
info={'marshmallow': {"interpret_star_character": True}}
```

If the field can be queried with comparison signs such as >, <, >=, <=: 
```python
info={'marshmallow': {"allow_comparison_signs": True}}
```

When querying, provide a single value of a list of values.

if provided in `order_by` parameter, it will be considered as ascending order, add ` desc` at the end of the value to explicitly order by in descending order.

If the field allow comparison signs (`allow_comparison_signs`), you can add `>`, `>=`, `<`, `<=` in front of the value.

```python
from sqlalchemy import Column, String

class MyTable:
    __tablename__ = "my_table"

    key = Column(String, primary_key=True)
    value = Column(String)
```

## MongoDB (non-relational)

[PyMongo](https://api.mongodb.com/python/current/) is the underlying framework used to manipulate MongoDB.

To create a representation of a collection you will need to create a Mixin class.

To link your model to the underlying collection, you will need to provide a connection string.

### Collection

```python
from layabase.mongo import Column

class MyCollection:
    __collection_name__ = "my_collection"

    key = Column(str, is_primary_key=True)
    dict_value = Column(dict)
```

##### String fields

Fields containing string can be described using layabase.mongo.Column

```python
from layabase.mongo import Column

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

Fields containing a dictionary can be described using layabase.mongo.DictColumn

```python
from layabase.mongo import DictColumn

class MyCollection:
    __collection_name__ = "my_collection"

    key = DictColumn()
```

##### List fields

Fields containing a list can be described using layabase.mongo.ListColumn

```python
from layabase.mongo import ListColumn, Column

class MyCollection:
    __collection_name__ = "my_collection"

    key = ListColumn(Column())
```

## How to install
1. [python 3.6+](https://www.python.org/downloads/) must be installed
2. Use pip to install module:
```sh
python -m pip install layabase
```

Note that depending on what you want to connect to, you will have to use a different module name than layabase:
* Mongo database: layabase[mongo]
* Mongo in-memory database: layabase mongomock
* Other database: layabase[sqlalchemy]

## Technical

 * [Requirements](#developer-requirements)
 * [Usage](#developer-usage)

## Developer-Requirements

The module requires the following to run:

  * [python 3.6+](https://www.python.org/downloads/)

## Developer-Usage

## Installation

Dependencies can be installed via `setup.py`:

```sh
python -m pip install .
```

### Testing

Test specific dependencies can be installed via `setup.py` `testing` optional:

```sh
python -m pip install .[testing]
```

Then you can launch tests using [pytest](http://doc.pytest.org/en/latest/).
