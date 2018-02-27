import logging
import datetime
import enum
import copy

from pycommon_database.database_mongo import Column, IndexType

logger = logging.getLogger(__name__)


@enum.unique
class Action(enum.IntEnum):
    Insert = 1
    Update = 2
    Delete = 3


def _create_from(model, base):
    class AuditModel(model, base=base, table_name=f'audit_{model.__tablename__}'):
        """
        Class providing Audit fields for a MONGODB model.
        """
        _model = model

        audit_user = Column(str, is_primary_key=True)
        audit_date_utc = Column(datetime.datetime, is_primary_key=True, index_type=IndexType.Unique)
        audit_action = Column(Action)

        @classmethod
        def audit_add(cls, model_as_dict: dict):
            cls._audit_action(action=Action.Insert, model_as_dict=copy.deepcopy(model_as_dict))

        @classmethod
        def audit_update(cls, model_as_dict: dict):
            cls._audit_action(action=Action.Update, model_as_dict=copy.deepcopy(model_as_dict))

        @classmethod
        def audit_remove(cls, **kwargs):
            for removed_dict_model in cls._model.get_all(**kwargs):
                cls._audit_action(action=Action.Delete, model_as_dict=removed_dict_model)

        @classmethod
        def _audit_action(cls, action: Action, model_as_dict: dict):
            model_as_dict['audit_user'] = ''
            model_as_dict['audit_date_utc'] = datetime.datetime.utcnow()
            model_as_dict['audit_action'] = action.name
            model_as_dict.pop('_id', None)
            cls.add(model_as_dict)

    return AuditModel
