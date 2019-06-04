<h2 align="center">Python Common Database Module</h2>

<p align="center">
<a href="https://github.com/ambv/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href='https://pse.tools.digital.engie.com/drm-all.gem/job/team/view/Python%20modules/job/pycommon_database/job/master/'><img src='https://pse.tools.digital.engie.com/drm-all.gem/buildStatus/icon?job=team/pycommon_database/master'></a>
</p>

Query your databases easily and transparently thanks to this module providing helpers on top of most brilliant python
database modules (SQLAlchemy and PyMongo).

Features:

 * Audit
 * Rollback
 * History
 * Validation
 * Conversion

## Concept ##

You will define a model class to help you with the manipulation of:

 * A collection if this is a MongoDB you are connecting to.
 * A table if this is a non-Mongo database you are connecting to.

This model will describe:

 * The document fields if this is a MongoDB you are connecting to.
 * The table columns if this is a non-Mongo database you are connecting to.

By providing this model to a controller class, you will automatically have flask-restplus models and arguments parsers.

Every feature provided by a model if exposed via the controller class so that you never have to manipulate the model yourself.

## Installation ##

pycommon_database is easiest to work with when installed into a virtual environment using the setup.py.

To install all test required dependencies, use the following command:

```python
python -m pip install .[testing]
```

## Relational databases (non-Mongo) ##

SQLAlchemy is the underlying framework used to manipulate relational databases.

To create a representation of a table you will need to extend pycommon_database.database_sqlalchemy.CRUDModel

### SQLAlchemy model ###

Extending pycommon_database.database_sqlalchemy.CRUDModel will provides C.R.U.D. methods on your SQLAlchemy model.

#### Model definition ####

```python
from sqlalchemy import Column, String
from pycommon_database.database_sqlalchemy import CRUDModel

base = None  # Base is provided when calling load method

class MyModel(CRUDModel, base):
    
    key = Column(String, primary_key=True)
    value = Column(String)
```

#### Retrieving data ####

```python
all_models_as_dict_list = MyModel.get_all()

filtered_models_as_dict_list = MyModel.get_all(value='value1')

filtered_model_as_dict = MyModel.get(key='key1')
```

#### Inserting data ####

```python
inserted_models_as_dict_list = MyModel.add_all([
    {'key': 'key1', 'value': 'value1'},
    {'key': 'key2', 'value': 'value2'},
])

inserted_model_as_dict = MyModel.add({'key': 'key1', 'value': 'value1'})
```

#### Updating data ####

```python
updated_model_as_dict = MyModel.update({'key': 'key1', 'value': 'new value'})
```

#### Removing data ####

```python
nb_removed_models = MyModel.remove(key='key1')
```

## MongoDB (non-relational) ##

PyMongo is the underlying framework used to manipulate MongoDB.

To create a representation of a collection you will need to extend pycommon_database.database_mongo.CRUDModel

To link your model to the underlying collection, you will need to provide a connection string.

### Mongo model ###

Extending pycommon_database.database_mongo.CRUDModel will provides C.R.U.D. methods on your Mongo model.

#### Model definition ####

```python
from pycommon_database.database_mongo import CRUDModel, Column

pymongo_database = None  # pymongo database instance

class MyModel(CRUDModel, base=pymongo_database, table_name="related collection name"):

    key = Column(str, is_primary_key=True)
    dict_value = Column(dict)
```

##### String fields #####

Fields containing string can be described using pycommon_database.database_mongo.Column

```python
from pycommon_database.database_mongo import CRUDModel, Column

pymongo_database = None  # pymongo database instance

class MyModel(CRUDModel, base=pymongo_database, table_name="related collection name"):

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
        <td>Field description used in Swagger and in error messages. Should be a string value.</td>
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
</table>

##### Dictionary fields #####

Fields containing a dictionary can be described using pycommon_database.database_mongo.DictColumn

##### List fields #####

Fields containing a list can be described using pycommon_database.database_mongo.ListColumn

#### Retrieving data ####

```python
all_models_as_dict_list = MyModel.get_all()

filtered_models_as_dict_list = MyModel.get_all(dict_value={'dict_key': 'value1'})

filtered_model_as_dict = MyModel.get(key='key1')
```

#### Inserting data ####

```python
inserted_models_as_dict_list = MyModel.add_all([
    {'key': 'key1', 'dict_value': {'dict_key': 'value1'}},
    {'key': 'key2', 'dict_value': {'dict_key': 'value2'}},
])

inserted_model_as_dict = MyModel.add({'key': 'key1', 'dict_value': {'dict_key': value1'}})
```

#### Updating data ####

```python
updated_model_as_dict = MyModel.update({'key': 'key1', 'dict_value': {'dict_key': 'new value'}})
```

#### Removing data ####

```python
nb_removed_models = MyModel.remove(key='key1')
```

### Link to a Mongo database ###

Mongo specific dependencies must be installed, use the following command:

```python
python -m pip install .[mongo]
```

Note that to link to a fake Mongo database (in-memory), you can install the following package as well:
```python
python -m pip install mongomock
```

and use the following connection string: "mongomock" instead of a real mongodb connection string.

```python
import pycommon_database

def create_models(database):
    my_model_class = None  # It should be a model class
    return [my_model_class]  # It should be a list containing all the models that should be linked

pycommon_database.load("mongodb://host:port/server_name", create_models)
```

## CRUD Controller ##

Extending pycommon_database.database.CRUDController will provides C.R.U.D. methods on your controller.

It returns you JSON ready to be sent to your client.

It also allows you to manage audit.

### Controller definition ###

```python
from pycommon_database.database import CRUDController

class MyController(CRUDController):
    pass

MyController.model(MyModel)
```

### Retrieving data ###

```python
all_models_as_dict_list = MyController.get()

filtered_models_as_dict_list = MyController.get(value='value1')
```

### Inserting data ###

```python
inserted_models_as_dict_list = MyController.post_many([
    {'key': 'key1', 'value': 'value1'},
    {'key': 'key2', 'value': 'value2'},
])

inserted_model_as_dict = MyController.post({'key': 'key1', 'value': 'value1'})
```

### Updating data ###

```python
updated_model_as_dict = MyController.put({'key': 'key1', 'value': 'new value'})
```

### Removing data ###

```python
nb_removed_models = MyController.delete(key='key1')
```

### Retrieving table mapping ###

```python
# Return a dictionary like
# {'table': 'MyModel', 'key': 'key', 'value': 'value'}
description = MyController.get_model_description()
```

### Auditing ###

```python
all_audit_models_as_dict_list = MyController.get_audit()

filtered_audit_models_as_dict_list = MyController.get_audit(value='value1')
```
