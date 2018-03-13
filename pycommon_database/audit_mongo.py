import logging
import datetime
import enum
import copy

from pycommon_database.database_mongo import Column, IndexType, CRUDModel
from pycommon_database.audit import current_user_name
from pycommon_database.versioning_mongo import VersionedCRUDModel

logger = logging.getLogger(__name__)


@enum.unique
class Action(enum.IntEnum):
    Insert = 1
    Update = 2
    Delete = 3


def _create_from(model, base):
    return _versioning_audit(model, base) if issubclass(model, VersionedCRUDModel) else _common_audit(model, base)


def _common_audit(model, base):
    class AuditModel(model, base=base, table_name=f'audit_{model.__tablename__}', audit=False):
        """
        Class providing Audit fields for a MONGODB model.
        """
        _model = model

        audit_user = Column(str, is_primary_key=True)
        audit_date_utc = Column(datetime.datetime, is_primary_key=True, index_type=IndexType.Unique)
        audit_action = Column(Action)

        @classmethod
        def audit_add(cls, model_as_dict: dict):
            """
            :param model_as_dict: Model as inserted into Mongo.
            """
            cls._audit_action(action=Action.Insert, model_as_dict=copy.deepcopy(model_as_dict))

        @classmethod
        def audit_update(cls, model_as_dict: dict):
            """
            :param model_as_dict: Model (updated version) as inserted into Mongo.
            """
            cls._audit_action(action=Action.Update, model_as_dict=copy.deepcopy(model_as_dict))

        @classmethod
        def audit_remove(cls, **model_to_query):
            """
            :param model_to_query: Arguments that can directly be provided to Mongo.
            """
            for removed_dict_model in cls._model.__collection__.find(model_to_query):
                cls._audit_action(action=Action.Delete, model_as_dict=removed_dict_model)

        @classmethod
        def _audit_action(cls, action: Action, model_as_dict: dict):
            model_as_dict[cls.audit_user.name] = current_user_name()
            model_as_dict[cls.audit_date_utc.name] = datetime.datetime.utcnow()
            model_as_dict[cls.audit_action.name] = action.value
            model_as_dict.pop('_id', None)
            cls.__collection__.insert_one(model_as_dict)

    return AuditModel


def _versioning_audit(model, base):
    class AuditModel(CRUDModel, base=base, table_name=f'audit_{model.__tablename__}', audit=False):
        """
        Class providing Audit fields for a MONGODB model.
        """
        audit_user = Column(str, is_primary_key=True)
        audit_date_utc = Column(datetime.datetime, is_primary_key=True, index_type=IndexType.Unique)
        audit_action = Column(Action)

        @classmethod
        def audit_add(cls, audit_date: datetime.datetime):
            cls._audit_action(action=Action.Insert, audit_date=audit_date)

        @classmethod
        def audit_update(cls, audit_date: datetime.datetime):
            cls._audit_action(action=Action.Update, audit_date=audit_date)

        @classmethod
        def audit_remove(cls, audit_date: datetime.datetime):
            cls._audit_action(action=Action.Delete, audit_date=audit_date)

        @classmethod
        def _audit_action(cls, action: Action, audit_date: datetime.datetime):
            cls.__collection__.insert_one({
                cls.audit_user.name: current_user_name(),
                cls.audit_date_utc.name: audit_date,
                cls.audit_action.name: action.value,
            })

    return AuditModel
