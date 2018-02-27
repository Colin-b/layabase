import logging
from sqlalchemy import Column, DateTime, Enum, String, inspect as inspect_sqla
import datetime

from pycommon_database.flask_restplus_errors import ValidationFailed

logger = logging.getLogger(__name__)


def _column(attribute):
    if len(attribute.columns) != 1:
        raise Exception(f'Recreating an attribute ({attribute}) based on more than one column is not handled for now.')
    column = attribute.columns[0]
    column_copy = Column(column.name, column.type, primary_key=column.primary_key, nullable=column.nullable)
    return column_copy


def _create_from(model):
    class AuditModel(*model.__bases__):
        """
        Class providing Audit fields for a SQL Alchemy model.
        """
        __tablename__ = f'audit_{model.__tablename__}'
        _model = model

        audit_user = Column(String, primary_key=True)
        audit_date_utc = Column(DateTime, primary_key=True)
        audit_action = Column(Enum('I', 'U', 'D', name='action_type'))

        @classmethod
        def audit_add(cls, model_as_dict: dict):
            """
            :param model_as_dict: Dictionary that was properly inserted.
            """
            cls._audit_action(action='I', model_as_dict=dict(model_as_dict))

        @classmethod
        def audit_update(cls, model_as_dict: dict):
            """
            :param model_as_dict: Dictionary that was properly inserted.
            """
            cls._audit_action(action='U', model_as_dict=dict(model_as_dict))

        @classmethod
        def audit_remove(cls, **kwargs):
            """
            :param kwargs: Filters as requested.
            """
            for removed_dict_model in cls._model.get_all(**kwargs):
                cls._audit_action(action='D', model_as_dict=removed_dict_model)

        @classmethod
        def _audit_action(cls, action: str, model_as_dict: dict):
            model_as_dict['audit_user'] = ''
            model_as_dict['audit_date_utc'] = datetime.datetime.utcnow().isoformat()
            model_as_dict['audit_action'] = action
            model, errors = cls.schema().load(model_as_dict, session=cls._session)
            if errors:
                raise ValidationFailed(model_as_dict, marshmallow_errors=errors)
            cls._session.add(model)  # Let any error be handled by the caller (main model), sane for commit

    for attribute in inspect_sqla(model).attrs:
        setattr(AuditModel, attribute.key, _column(attribute))

    return AuditModel
