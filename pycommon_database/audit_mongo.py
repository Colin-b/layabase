import logging
import datetime
import enum
import inspect

from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound
from pycommon_database.database_mongo import Column

logger = logging.getLogger(__name__)


class AuditModel:
    """
    Class providing Audit fields for a MONGODB model.
    """
    _model = None
    __collection__ = None

    audit_user = Column(name='audit_user', type=str, primary_key=True)
    audit_date_utc = Column(name='audit_date_utc', type=datetime.datetime, primary_key=True)
    audit_action = Column(name='audit_action', type=enum.Enum('action_type', 'I U D'))

    @classmethod
    def audit_add(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')

        cls._audit_action(action='I', model_as_dict=dict(model_as_dict))

    @classmethod
    def audit_update(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')
        model_as_dict_keys = cls._model._to_primary_keys_model(model_as_dict)
        previous_model_as_dict = cls._model.__collection__.find_one(model_as_dict_keys)
        if not previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict)

        cls._audit_action(action='U', model_as_dict=previous_model_as_dict)

    @classmethod
    def audit_remove(cls, **kwargs):
        query = cls._build_query(**kwargs)
        all_docs = cls._model.__collection__.find(query)
        removed_dict_models = [removed_dict_model for removed_dict_model in all_docs]
        for removed_dict_model in removed_dict_models:
            cls._audit_action(action='D', model_as_dict=removed_dict_model)

    @classmethod
    def _audit_action(cls, action, model_as_dict):
        model_as_dict['audit_user'] = ''
        model_as_dict['audit_date_utc'] = datetime.datetime.utcnow().isoformat()
        model_as_dict['audit_action'] = action
        audit_model_as_dict = {k: v for k, v in model_as_dict.items() if k != '_id'}
        cls.__collection__.insert(audit_model_as_dict)


def create_from(model):
    class AuditModelForModel(AuditModel, *model.__bases__):
        __collection__ = model.__db__['audit_' + model.__collection__.name]
        _model = model

    for attribute in inspect.getmembers(model):
        if type(attribute[1]) == Column:
            setattr(AuditModelForModel, attribute[0], attribute[1])

    return AuditModelForModel
