import logging
import datetime
import enum
import copy

from pycommon_database.database_mongo import Column, IndexType, CRUDModel
from pycommon_database.audit import current_user_name
from pycommon_database.versioning_mongo import VersionedCRUDModel, REVISION_COUNTER

logger = logging.getLogger(__name__)


@enum.unique
class Action(enum.IntEnum):
    Insert = 1
    Update = 2
    Delete = 3
    Rollback = 4


def _create_from(model, base):
    return _versioning_audit(model, base) if issubclass(model, VersionedCRUDModel) else _common_audit(model, base)


def _common_audit(model, base):
    class AuditModel(model, base=base, table_name=f'audit_{model.__tablename__}', audit=False):
        """
        Class providing Audit fields for a MONGODB model.
        """
        revision = Column(int, is_primary_key=True, index_type=IndexType.Unique)

        audit_user = Column(str)
        audit_date_utc = Column(datetime.datetime)
        audit_action = Column(Action)

        @classmethod
        def get_response_model(cls, namespace):
            return namespace.model('Audit' + model.__name__, cls._flask_restplus_fields(namespace))

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
            document.pop('_id', None)
            document[cls.revision.name] = cls._increment(*REVISION_COUNTER)
            document[cls.audit_user.name] = current_user_name()
            document[cls.audit_date_utc.name] = datetime.datetime.utcnow()
            document[cls.audit_action.name] = action.value
            cls.__collection__.insert_one(document)

    return AuditModel


def _versioning_audit(model, base):
    class AuditModel(CRUDModel, base=base, table_name='audit', audit=False):
        """
        Class providing the audit for all versioned MONGODB models.
        """
        table_name = Column(str, is_primary_key=True, index_type=IndexType.Unique)
        revision = Column(int, is_primary_key=True, index_type=IndexType.Unique)

        audit_user = Column(str)
        audit_date_utc = Column(datetime.datetime)
        audit_action = Column(Action)

        @classmethod
        def query_get_parser(cls):
            query_get_parser = super().query_get_parser()
            query_get_parser.remove_argument(cls.table_name.name)
            return query_get_parser

        @classmethod
        def get_response_model(cls, namespace):
            all_fields = cls._flask_restplus_fields(namespace)
            del all_fields[cls.table_name.name]
            return namespace.model('AuditModel', all_fields)

        @classmethod
        def get_all(cls, **filters):
            filters[cls.table_name.name] = model.__tablename__
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
            cls.__collection__.insert_one({
                cls.table_name.name: model.__tablename__,
                cls.revision.name: revision,
                cls.audit_user.name: current_user_name(),
                cls.audit_date_utc.name: datetime.datetime.utcnow(),
                cls.audit_action.name: action.value,
            })

    return AuditModel
