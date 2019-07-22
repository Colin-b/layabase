import datetime
import logging
import sys
import unittest
from threading import Thread

import sqlalchemy
from flask_restplus import fields as flask_rest_plus_fields, inputs
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from pycommon_error.validation import ValidationFailed, ModelCouldNotBeFound
from test.flask_restplus_mock import TestAPI

from pycommon_database import database, database_sqlalchemy


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class SQlAlchemyDatabaseTest(unittest.TestCase):
    def test_none_connection_string_is_invalid():
        with assertRaises(Exception) as cm:
            database.load(None, None)
        assertEqual("A database connection URL must be provided.", str(cm.exception))

    def test_empty_connection_string_is_invalid():
        with assertRaises(Exception) as cm:
            database.load("", None)
        assertEqual("A database connection URL must be provided.", str(cm.exception))

    def test_no_create_models_function_is_invalid():
        with assertRaises(Exception) as cm:
            database.load("sqlite:///:memory:", None)
        assertEqual(
            "A method allowing to create related models must be provided.",
            str(cm.exception),
        )

    def test_models_are_added_to_metadata():
        def create_models(base):
            class TestModel(base):
                __tablename__ = "sample_table_name"

                key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

                @classmethod
                def _post_init(cls, base):
                    pass

            return [TestModel]

        db = database.load("sqlite:///:memory:", create_models)
        assertEqual("sqlite:///:memory:", str(db.metadata.bind.engine.url))
        assertEqual(
            ["sample_table_name"], [table for table in db.metadata.tables.keys()]
        )

    def test_sybase_url():
        assertEqual(
            "sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2",
            database_sqlalchemy._clean_database_url(
                "sybase+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2"
            ),
        )

    def test_sybase_does_not_support_offset():
        assertFalse(database_sqlalchemy._supports_offset("sybase+pyodbc"))

    def test_sybase_does_not_support_retrieving_metadata():
        assertFalse(database_sqlalchemy._can_retrieve_metadata("sybase+pyodbc"))

    def test_mssql_url():
        assertEqual(
            "mssql+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2",
            database_sqlalchemy._clean_database_url(
                "mssql+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2"
            ),
        )

    def test_mssql_does_not_support_offset():
        assertFalse(database_sqlalchemy._supports_offset("mssql+pyodbc"))

    def test_mssql_does_not_support_retrieving_metadata():
        assertFalse(database_sqlalchemy._can_retrieve_metadata("mssql+pyodbc"))

    def test_sql_lite_support_offset():
        assertTrue(database_sqlalchemy._supports_offset("sqlite"))

    def test_in_memory_database_is_considered_as_in_memory():
        assertTrue(database_sqlalchemy._in_memory("sqlite:///:memory:"))

    def test_real_database_is_not_considered_as_in_memory():
        assertFalse(
            database_sqlalchemy._in_memory(
                "sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2"
            )
        )


class SQLAlchemyFlaskRestPlusModelsTest(unittest.TestCase):
    def test_rest_plus_type_for_string_field_is_string():
        field = marshmallow_fields.String()
        assertEqual(
            flask_rest_plus_fields.String,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_int_field_is_integer():
        field = marshmallow_fields.Integer()
        assertEqual(
            flask_rest_plus_fields.Integer,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_bool_field_is_boolean():
        field = marshmallow_fields.Boolean()
        assertEqual(
            flask_rest_plus_fields.Boolean,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_date_field_is_date():
        field = marshmallow_fields.Date()
        assertEqual(
            flask_rest_plus_fields.Date, database_sqlalchemy._get_rest_plus_type(field)
        )

    def test_rest_plus_type_for_datetime_field_is_datetime():
        field = marshmallow_fields.DateTime()
        assertEqual(
            flask_rest_plus_fields.DateTime,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_decimal_field_is_fixed():
        field = marshmallow_fields.Decimal()
        assertEqual(
            flask_rest_plus_fields.Fixed, database_sqlalchemy._get_rest_plus_type(field)
        )

    def test_rest_plus_type_for_float_field_is_float():
        field = marshmallow_fields.Float()
        assertEqual(
            flask_rest_plus_fields.Float, database_sqlalchemy._get_rest_plus_type(field)
        )

    def test_rest_plus_type_for_number_field_is_decimal():
        field = marshmallow_fields.Number()
        assertEqual(
            flask_rest_plus_fields.Decimal,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_time_field_is_datetime():
        field = marshmallow_fields.Time()
        assertEqual(
            flask_rest_plus_fields.DateTime,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_field_field_is_string():
        field = marshmallow_fields.Field()
        assertEqual(
            flask_rest_plus_fields.String,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_none_field_cannot_be_guessed():
        with assertRaises(Exception) as cm:
            database_sqlalchemy._get_rest_plus_type(None)
        assertEqual(
            "Flask RestPlus field type cannot be guessed for None field.",
            cm.exception.args[0],
        )

    def test_rest_plus_example_for_string_field():
        field = marshmallow_fields.String()
        assertEqual("sample_value", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_int_field_is_integer():
        field = marshmallow_fields.Integer()
        assertEqual("0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_bool_field_is_true():
        field = marshmallow_fields.Boolean()
        assertEqual("true", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_date_field_is_YYYY_MM_DD():
        field = marshmallow_fields.Date()
        assertEqual("2017-09-24", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_datetime_field_is_YYYY_MM_DDTHH_MM_SS():
        field = marshmallow_fields.DateTime()
        assertEqual("2017-09-24T15:36:09", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_decimal_field_is_decimal():
        field = marshmallow_fields.Decimal()
        assertEqual("0.0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_float_field_is_float():
        field = marshmallow_fields.Float()
        assertEqual("0.0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_number_field_is_decimal():
        field = marshmallow_fields.Number()
        assertEqual("0.0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_time_field_is_HH_MM_SS():
        field = marshmallow_fields.Time()
        assertEqual("15:36:09", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_none_field_is_sample_value():
        assertEqual("sample_value", database_sqlalchemy._get_example(None))


class SQlAlchemyColumnsTest(unittest.TestCase):
    _model = None

    @classmethod
    def setUpClass(cls):
        revert_now()
        database.load("sqlite:///:memory:", cls._create_models)

    @classmethod
    def _create_models(cls, base):
        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample_table_name"

            string_column = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            integer_column = sqlalchemy.Column(sqlalchemy.Integer)
            boolean_column = sqlalchemy.Column(sqlalchemy.Boolean)
            date_column = sqlalchemy.Column(sqlalchemy.Date)
            datetime_column = sqlalchemy.Column(sqlalchemy.DateTime)
            float_column = sqlalchemy.Column(sqlalchemy.Float)

        cls._model = TestModel
        return [TestModel]

    def test_field_declaration_order_is_kept_in_schema():
        fields = _model.schema().fields
        assertEqual(
            [
                "string_column",
                "integer_column",
                "boolean_column",
                "date_column",
                "datetime_column",
                "float_column",
            ],
            [field_name for field_name in fields],
        )

    def test_python_type_for_sqlalchemy_string_field_is_string():
        field = _model.schema().fields["string_column"]
        assertEqual(str, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_integer_field_is_integer():
        field = _model.schema().fields["integer_column"]
        assertEqual(int, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_boolean_field_is_boolean():
        field = _model.schema().fields["boolean_column"]
        assertEqual(inputs.boolean, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_date_field_is_date():
        field = _model.schema().fields["date_column"]
        assertEqual(
            inputs.date_from_iso8601, database_sqlalchemy._get_python_type(field)
        )

    def test_python_type_for_sqlalchemy_datetime_field_is_datetime():
        field = _model.schema().fields["datetime_column"]
        assertEqual(
            inputs.datetime_from_iso8601, database_sqlalchemy._get_python_type(field)
        )

    def test_python_type_for_sqlalchemy_float_field_is_float():
        field = _model.schema().fields["float_column"]
        assertEqual(float, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_none_field_cannot_be_guessed():
        with assertRaises(Exception) as cm:
            database_sqlalchemy._get_python_type(None)
        assertEqual(
            "Python field type cannot be guessed for None field.", cm.exception.args[0]
        )
