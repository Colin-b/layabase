# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- SQLAlchemy models do not need to extend anything. Provide [Mixins](https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/mixins.html#declarative-mixins) instead of models.
- CRUDController does not expose class methods anymore but must be instantiated instead. Meaning there is no need for placeholder classes anymore.
- database.load now request a list of controller instances as parameters.

### Fixed
- Avoid creating f-string when nothing needs to be interpreted.

### Removed
- layabase.database is not exposed anymore. Use layabase instead.
- layabase.database_sqlalchemy is not exposed anymore.
- layabase.audit is not exposed anymore. It should never have been used anyway.
- layabase.audit_mongo is not exposed anymore.
- layabase.audit_sqlalchemy is not exposed anymore.
- layabase.database_mongo.CRUDModel is not exposed anymore.

### Added
- layabase.testing.mock_mongo_audit_datetime pytest fixture
- layabase.testing.mock_sqlalchemy_audit_datetime pytest fixture
- layabase.testing.mock_mongo_health_datetime pytest fixture
- layabase.testing.mock_sqlalchemy_health_datetime pytest fixture

## [2.0.1] - 2019-09-26
### Fixed
- [SQLAlchemy] Avoid processing invalid data types when inserting or updating data.

## [2.0.0] - 2019-09-26
### Removed
- database.dump
- database.restore

### Changed
- layabase.database.reset is now layabase.testing.reset
- layabase.health_details is now layabase.check

### Fixed
- [SQLAlchemy] sample values now have the correct data type in the OpenAPI definition.

## [1.3.1] - 2019-09-25
### Fixed
- [SQLAlchemy] Description now return the schema as well if provided.
- [Mongo] Release 1.2.0 introduced a regression when logging was in DEBUG. It is now fixed and tested.

### Removed
- [SQLAlchemy] Remove support for List field as it is unused and not covered in test cases.
- [SQLAlchemy] Remove support for Number fields that are not Float or Decimal as it is unused and not covered in test cases.

## [1.3.0] - 2019-09-25
### Added
- [SQLAlchemy] It is now possible to mark a column as mandatory on query via info={'marshmallow': {"required_on_query": True}}

### Fixed
- query parsers now extract arguments from query arguments only (and do not look at body anymore)

## [1.2.0] - 2019-09-24
### Added
- You can now query mongo on something else than equality: >, >=, <, <=. Refer to documentation for details.

### Deprecated
- database.dump
- database.restore
- database.reset (in case of non testing purpose)

### Changed
- As a result of the fact that it's possible to query on non-equality, the field type is now string for float, date, datetime and int within the OpenAPI definition
- Update [marshmallow](https://marshmallow.readthedocs.io/en/latest/changelog.html) to version 3.2.0
- Update [mongomock](https://github.com/mongomock/mongomock/releases) to version 3.18.0

## [1.1.1] - 2019-09-10
### Fixed
- Use the proper query to ping SQLAlchemy related database instead of the hardcoded "SELECT 1".

### Changed
- Update pytest to version 5.1.2
- Update [marshmallow](https://marshmallow.readthedocs.io/en/latest/changelog.html) to version 3.0.3
- Update [SQLAlchemy](https://docs.sqlalchemy.org/en/13/changelog/index.html) to version 1.3.8
- Update [marshmallow_sqlalchemy](https://marshmallow-sqlalchemy.readthedocs.io/en/latest/changelog.html) to version 0.19.0

## [1.1.0] - 2019-08-22
### Changed
- Update CONTRIBUTING documentation to explain how to retrieve pre-commit via pip.
- Update layaberr to version 2.0.0
- Update pytest to version 5.1.1
- Update [pymongo](https://api.mongodb.com/python/3.9.0/changelog.html) to version 3.9.0
- Update [SQLAlchemy](https://docs.sqlalchemy.org/en/13/changelog/index.html) to version 1.3.7
- Update [marshmallow](https://marshmallow.readthedocs.io/en/latest/changelog.html) to version 3.0.0

## [1.0.0] - 2019-08-01
### Changed
- Initial release.

[Unreleased]:https://github.tools.digital.engie.com/gempy/layabase/compare/v2.0.1...HEAD
[2.0.1]:https://github.tools.digital.engie.com/gempy/layabase/compare/v2.0.0...v2.0.1
[2.0.0]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.3.1...v2.0.0
[1.3.1]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.3.0...v1.3.1
[1.3.0]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.2.0...v1.3.0
[1.2.0]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.1.1...v1.2.0
[1.1.1]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.1.0...v1.1.1
[1.1.0]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.tools.digital.engie.com/gempy/layabase/releases/tag/v1.0.0
