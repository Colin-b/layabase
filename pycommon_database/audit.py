import logging
from sqlalchemy import Column, DateTime, Enum, String, inspect
from pycommon_database.mongo import (
    MongoColumn,
    mongo_inspect,
    mongo_get_primary_keys_values,
    mongo_build_query,
    mongo_from_list_of_list_to_dict
)
import datetime
import enum

from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound

logger = logging.getLogger(__name__)


class AuditModel:
    """
    Class providing Audit fields for a SQL Alchemy model.
    """
    _model = None

    audit_user = Column(String, primary_key=True)
    audit_date_utc = Column(DateTime, primary_key=True)
    audit_action = Column(Enum('I', 'U', 'D', name='action_type'))

    @classmethod
    def audit_add(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')

        cls._audit_action(action='I', model_as_dict=dict(model_as_dict))

    @classmethod
    def audit_update(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')
        previous_model = cls._model.schema().get_instance(model_as_dict)
        if not previous_model:
            raise ModelCouldNotBeFound(model_as_dict)
        # Merge previous dict and new dict
        new_model_as_dict = {**inspect(previous_model).dict, **model_as_dict}

        cls._audit_action(action='U', model_as_dict=new_model_as_dict)

    @classmethod
    def audit_remove(cls, **kwargs):
        query = cls._session.query(cls._model)
        for key, value in kwargs.items():
            if value is not None:
                query = query.filter(getattr(cls, key) == value)
        removed_dict_models = [inspect(removed_model).dict for removed_model in query.all()]
        for removed_dict_model in removed_dict_models:
            cls._audit_action(action='D', model_as_dict=removed_dict_model)

    @classmethod
    def _audit_action(cls, action, model_as_dict):
        model_as_dict['audit_user'] = ''
        model_as_dict['audit_date_utc'] = datetime.datetime.utcnow().isoformat()
        model_as_dict['audit_action'] = action
        model, errors = cls.schema().load(model_as_dict, session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, marshmallow_errors=errors)
        try:
            cls._session.add(model)
            cls._session.commit()
        except Exception:
            cls._session.rollback()
            raise

class MongoAuditModel(AuditModel):
    """
    Class providing Audit fields for a MONGODB model.
    """
    _model = None
    __collection__ = None

    audit_user = MongoColumn(name='audit_user', type=str, primary_key=True)
    audit_date_utc = MongoColumn(name='audit_date_utc', type=datetime.datetime, primary_key=True)
    audit_action = MongoColumn(name='audit_action', type=enum.Enum('action_type', 'I U D'))

    @classmethod
    def audit_add(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')

        fmt_model_as_dict = mongo_from_list_of_list_to_dict(cls._model, model_as_dict)
        cls._audit_action(action='I', model_as_dict=dict(fmt_model_as_dict))

    @classmethod
    def audit_update(cls, model_as_dict: dict):
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')
        model_as_dict_keys = mongo_get_primary_keys_values(cls._model, model_as_dict)
        fmt_model_as_dict_keys = mongo_from_list_of_list_to_dict(cls._model, model_as_dict)
        fmt_previous_model_as_dict = cls._model.__collection__.find_one(fmt_model_as_dict_keys)
        if not fmt_previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict)

        cls._audit_action(action='U', model_as_dict=fmt_previous_model_as_dict)

    @classmethod
    def audit_remove(cls, **kwargs):
        query = mongo_build_query(**kwargs)
        fmt_query = mongo_from_list_of_list_to_dict(cls._model, query)
        all_docs = cls._model.__collection__.find(fmt_query)
        fmt_removed_dict_models = [removed_dict_model for removed_dict_model in all_docs]
        for fmt_removed_dict_model in fmt_removed_dict_models:
            cls._audit_action(action='D', model_as_dict=fmt_removed_dict_model)

    @classmethod
    def _audit_action(cls, action, model_as_dict):
        model_as_dict['audit_user'] = ''
        model_as_dict['audit_date_utc'] = datetime.datetime.utcnow().isoformat()
        model_as_dict['audit_action'] = action
        audit_model_as_dict = {k: v for k, v in model_as_dict.items() if k != '_id'}
        try:
            object_id = cls.__collection__.insert(audit_model_as_dict)
        except Exception:
            raise

def _column(attribute):
    if len(attribute.columns) != 1:
        raise Exception(f'Recreating an attribute ({attribute}) based on more than one column is not handled for now.')
    column = attribute.columns[0]
    column_copy = Column(column.name, column.type, primary_key=column.primary_key, nullable=column.nullable)
    return column_copy

def _mongo_column(column):
    column_copy = MongoColumn(name=column.name,
                              type=column.type_,
                              primary_key=column.primary_key,
                              nullable=column.nullable,
                              default=column.default,
                              required=column.required)
    return column_copy


def create_from(model):
    class AuditModelForModel(AuditModel, *model.__bases__):
        __tablename__ = 'audit_' + model.__tablename__
        _model = model

    mapper = inspect(model)
    for attr in mapper.attrs:
        setattr(AuditModelForModel, attr.key, _column(attr))

    return AuditModelForModel

def mongo_create_from(model):
    class MongoAuditModelForModel(MongoAuditModel, *model.__bases__):
        __collection__ = model.__db__['audit_' + model.__collection__.name]
        _model = model

    mapper = mongo_inspect(model)
    for attr in mapper:
        setattr(MongoAuditModelForModel, attr[0], _mongo_column(attr[1]))

    return MongoAuditModelForModel
