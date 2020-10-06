import logging
import datetime
import enum
import copy
from typing import Type

from layabase._database_mongo import _CRUDModel
from layabase.mongo import Column
from layabase._versioning_mongo import VersionedCRUDModel

logger = logging.getLogger(__name__)


@enum.unique
class Action(enum.IntEnum):
    Insert = 1
    Update = 2
    Delete = 3
    Rollback = 4


def _create_from(mixin, model: Type[_CRUDModel], base, retrieve_user: callable):
    return (
        _versioning_audit(mixin, base, retrieve_user)
        if issubclass(model, VersionedCRUDModel)
        else _common_audit(mixin, model, base, retrieve_user)
    )


def _common_audit(mixin, model, base, retrieve_user: callable):
    class AuditModel(mixin, _CRUDModel, base=base, skip_name_check=True):
        """
        Class providing Audit fields for a MONGODB model.
        """

        __collection_name__ = f"audit_{model.__collection_name__}"

        revision = Column(int, is_primary_key=True)

        audit_user = Column(str)
        audit_date_utc = Column(datetime.datetime)
        audit_action = Column(Action)

        @classmethod
        def audit_add(cls, document: dict):
            """
            :param document: Document as inserted in Mongo.
            """
            cls._audit_action(Action.Insert, copy.deepcopy(document))

        @classmethod
        def audit_update(cls, document: dict):
            """
            :param document: Document as updated in Mongo.
            """
            cls._audit_action(Action.Update, copy.deepcopy(document))

        @classmethod
        def audit_remove(cls, **filters):
            """
            :param filters: Arguments that can directly be provided to Mongo.
            """
            for removed_document in model.__collection__.find(filters):
                cls._audit_action(Action.Delete, removed_document)

        @classmethod
        def _audit_action(cls, action: Action, document: dict):
            document.pop("_id", None)
            document[cls.revision.name] = cls._increment(
                "revision", model.__collection_name__
            )
            document[cls.audit_user.name] = retrieve_user()
            document[cls.audit_date_utc.name] = datetime.datetime.utcnow()
            document[cls.audit_action.name] = action.value
            cls.__collection__.insert_one(document)

    return AuditModel


def _versioning_audit(mixin, base, retrieve_user: callable):
    class AuditModel(_CRUDModel, base=base, skip_name_check=True):
        """
        Class providing the audit for all versioned MONGODB models.
        """

        __collection_name__ = "audit"

        table_name = Column(str, is_primary_key=True)
        revision = Column(int, is_primary_key=True)

        audit_user = Column(str)
        audit_date_utc = Column(datetime.datetime)
        audit_action = Column(Action)

        @classmethod
        def get_all(cls, **filters):
            filters[cls.table_name.name] = mixin.__collection_name__
            return super().get_all(**filters)

        @classmethod
        def audit_add(cls, revision: int):
            cls._audit_action(Action.Insert, revision)

        @classmethod
        def audit_update(cls, revision: int):
            cls._audit_action(Action.Update, revision)

        @classmethod
        def audit_remove(cls, revision: int):
            cls._audit_action(Action.Delete, revision)

        @classmethod
        def audit_rollback(cls, revision: int):
            cls._audit_action(Action.Rollback, revision)

        @classmethod
        def _audit_action(cls, action: Action, revision: int):
            cls.__collection__.insert_one(
                {
                    cls.table_name.name: mixin.__collection_name__,
                    cls.revision.name: revision,
                    cls.audit_user.name: retrieve_user(),
                    cls.audit_date_utc.name: datetime.datetime.utcnow(),
                    cls.audit_action.name: action.value,
                }
            )

    return AuditModel
