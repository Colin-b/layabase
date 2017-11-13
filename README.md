# Python Common Database Module #

Provide helper to manipulate database(s).

## SQLAlchemy model ##

Extending pycommon_database.database.CRUDModel will provides C.R.U.D. methods on your SQLAlchemy model.

### Model definition ###

```python
from sqlalchemy import Column, String
from pycommon_database.database import CRUDModel

base = None

class MyModel(CRUDModel, base):
    
    key = Column(String, primary_key=True)
    value = Column(String)

model_as_dict = MyModel.remove(key='key1')
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
