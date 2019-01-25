import datetime
import enum

from pycommon_error.validation import ValidationFailed
from sqlalchemy import Column, DateTime, Enum, String, inspect, Integer

from pycommon_database.audit import current_user_name


@enum.unique
class Action(enum.Enum):
    Insert = "I"
    Update = "U"
    Delete = "D"


def _column(attribute):
    if len(attribute.columns) != 1:
        raise Exception(
            f"Recreating an attribute ({attribute}) based on more than one column is not handled for now."
        )
    column = attribute.columns[0]
    return Column(column.name, column.type, nullable=column.nullable)


def _create_from(model):
    class AuditModel(*model.__bases__):
        """
        Class providing Audit fields for a SQL Alchemy model.
        """

        __tablename__ = f"audit_{model.__tablename__}"
        _model = model

        revision = Column(Integer, primary_key=True, autoincrement=True)

        audit_user = Column(String)
        audit_date_utc = Column(DateTime)
        audit_action = Column(
            Enum(*[action.value for action in Action], name="action_type")
        )

        @classmethod
        def get_response_model(cls, namespace):
            return namespace.model(
                "Audit" + cls._model.__name__, cls._flask_restplus_fields()
            )

        @classmethod
        def audit_add(cls, row: dict):
            """
            :param row: Dictionary that was properly inserted.
            """
            cls._audit_action(Action.Insert, dict(row))

        @classmethod
        def audit_update(cls, row: dict):
            """
            :param row: Dictionary that was properly inserted.
            """
            cls._audit_action(Action.Update, dict(row))

        @classmethod
        def audit_remove(cls, **filters):
            """
            :param filters: Filters as requested.
            """
            for removed_row in cls._model.get_all(**filters):
                cls._audit_action(Action.Delete, removed_row)

        @classmethod
        def _audit_action(cls, action: Action, row: dict):
            row["audit_user"] = current_user_name()
            row["audit_date_utc"] = datetime.datetime.utcnow().isoformat()
            row["audit_action"] = action.value
            row_model, errors = cls.schema().load(row, session=cls._session)
            if errors:
                raise ValidationFailed(row, errors)
            cls._session.add(
                row_model
            )  # Let any error be handled by the caller (main model), same for commit

    for attribute in inspect(model).attrs:
        setattr(AuditModel, attribute.key, _column(attribute))

    return AuditModel
