# Python Common Database Module #

Provide helper to manipulate database(s).

## SQLAlchemy model ##

Extending pycommon_database.database_sqlalchemy.CRUDModel will provides C.R.U.D. methods on your SQLAlchemy model.

### Model definition ###

```python
from sqlalchemy import Column, String
from pycommon_database.database_sqlalchemy import CRUDModel

base = None  # Base is provided when calling load method

class MyModel(CRUDModel, base):
    
    key = Column(String, primary_key=True)
    value = Column(String)
```

### Retrieving data ###

```python
all_models_as_dict_list = MyModel.get_all()

filtered_models_as_dict_list = MyModel.get_all(value='value1')

filtered_model_as_dict = MyModel.get(key='key1')
```

### Inserting data ###

```python
inserted_models_as_dict_list = MyModel.add_all([
    {'key': 'key1', 'value': 'value1'},
    {'key': 'key2', 'value': 'value2'},
])

inserted_model_as_dict = MyModel.add({'key': 'key1', 'value': 'value1'})
```

### Updating data ###

```python
updated_model_as_dict = MyModel.update({'key': 'key1', 'value': 'new value'})
```

### Removing data ###

```python
nb_removed_models = MyModel.remove(key='key1')
```

## Mongo model ##

Extending pycommon_database.database_mongo.CRUDModel will provides C.R.U.D. methods on your Mongo model.

### Model definition ###

```python
from pycommon_database.database_mongo import CRUDModel, Column

class MyModel(CRUDModel):

    key = Column(str, is_primary_key=True)
    dict_value = Column(dict)
```

### Retrieving data ###

```python
all_models_as_dict_list = MyModel.get_all()

filtered_models_as_dict_list = MyModel.get_all(dict_value={'dict_key': 'value1'})

filtered_model_as_dict = MyModel.get(key='key1')
```

### Inserting data ###

```python
inserted_models_as_dict_list = MyModel.add_all([
    {'key': 'key1', 'dict_value': {'dict_key': 'value1'}},
    {'key': 'key2', 'dict_value': {'dict_key': 'value2'}},
])

inserted_model_as_dict = MyModel.add({'key': 'key1', 'dict_value': {'dict_key': value1'}})
```

### Updating data ###

```python
updated_model_as_dict = MyModel.update({'key': 'key1', 'dict_value': {'dict_key': 'new value'}})
```

### Removing data ###

```python
nb_removed_models = MyModel.remove(key='key1')
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
