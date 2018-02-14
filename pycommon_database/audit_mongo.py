import logging
import datetime
import enum
import copy

from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound
from pycommon_database.database_mongo import Column

logger = logging.getLogger(__name__)


class AuditModel:
    """
    Class providing Audit fields for a MONGODB model.
    """
    _model = None
    __collection__ = None

    audit_user = Column(str, is_primary_key=True)
    audit_date_utc = Column(datetime.datetime, is_primary_key=True)
    audit_action = Column(enum.Enum('action_type', 'I U D'))

    @classmethod
    def audit_add(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')

        cls._audit_action(action='I', model_as_dict=copy.deepcopy(model_as_dict))

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
        for removed_dict_model in cls._model.__collection__.find(query):
            cls._audit_action(action='D', model_as_dict=removed_dict_model)

    @classmethod
    def _audit_action(cls, action, model_as_dict):
        model_as_dict['audit_user'] = ''
        model_as_dict['audit_date_utc'] = datetime.datetime.utcnow()
        model_as_dict['audit_action'] = action
        model_as_dict.pop('_id', None)
        cls.__collection__.insert_one(model_as_dict)


def create_from(model):
    class AuditModelForModel(AuditModel, *model.__bases__):
        __tablename__ = f'audit_{model.__tablename__}'
        _model = model

    for field in model.get_fields():
        setattr(AuditModelForModel, field.name, field)

    return AuditModelForModel
