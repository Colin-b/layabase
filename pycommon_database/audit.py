import logging
from sqlalchemy import Column, DateTime, Enum, String, inspect
import datetime

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
        model_as_dict['audit_date_utc'] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
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


def _column(attribute):
    if len(attribute.columns) != 1:
        raise Exception(f'Recreating an attribute ({attribute}) based on more than one column is not handled for now.')
    column = attribute.columns[0]
    column_copy = Column(column.name, column.type, primary_key=column.primary_key, nullable=column.nullable)
    return column_copy


def create_from(model):
    class AuditModelForModel(AuditModel, *model.__bases__):
        __tablename__ = 'audit_' + model.__tablename__
        _model = model

    mapper = inspect(model)
    for attr in mapper.attrs:
        setattr(AuditModelForModel, attr.key, _column(attr))

    return AuditModelForModel
