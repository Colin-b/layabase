import datetime
import enum
import copy

from sqlalchemy import Column, DateTime, Enum, String, Integer


@enum.unique
class Action(enum.Enum):
    Insert = "I"
    Update = "U"
    Delete = "D"


def _to_audit_column(column):
    """
    Remove primary keys from mixin as revision is the unique primary key
    Remove auto increment from mixin as value is already auto incremented by model

    :param column: SQLAlchemy column or any other attribute of the original Mixin.
    """
    if hasattr(column, "primary_key") and column.primary_key:
        column = copy.deepcopy(column)
        column.primary_key = False
    if hasattr(column, "autoincrement") and column.autoincrement:
        column = copy.deepcopy(column)
        column.autoincrement = False

    return column


def _create_from(model, retrieve_user: callable):
    """

    :param model: CRUDModel of the table that should be audited.
    :return The class providing additional audit fields and features.
    """

    class AuditModel:
        """
        Class providing Audit fields.
        """

        revision = Column(Integer, primary_key=True, autoincrement=True)

        audit_user = Column(String)
        audit_date_utc = Column(DateTime)
        # Enum is created with a table specific name to avoid conflict in PostGreSQL (as enum is created outside table)
        audit_action = Column(
            Enum(
                *[action.value for action in Action],
                name=f"audit_{model.__tablename__}_action_type",
            )
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
            for removed_row in model.get_all(**filters):
                cls._audit_action(Action.Delete, removed_row)

        @classmethod
        def _audit_action(cls, action: Action, row: dict):
            row["audit_user"] = retrieve_user()
            row["audit_date_utc"] = datetime.datetime.utcnow().isoformat()
            row["audit_action"] = action.value
            # Let any error be handled by the caller (main model), same for commit
            cls._session.add(cls.schema().load(row, session=cls._session))

    return AuditModel
