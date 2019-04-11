# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [13.20.0] - 2019-04-11
### Changed
- Update pycommon-test to version 5.3.0
- Update SQLAlchemy to version 1.3.2
- Update marshmallow_sqlalchemy to version 0.16.2
- Update pycommon-error to version 2.18.0

## [13.19.0] - 2019-04-10
### Added
- [Mongo] Add option to skip index update. This is used when 2 entites are mapped on the same collection, and these entites have a different field declaration (bizref use case)

## [13.18.0] - 2019-03-01
### Changed
- Update pycommon-test to version 5.2.0
- Update mongomock to version 3.16.0
- Update marshmallow to version 2.19.2
- Update SQLAlchemy to version 1.3.1
- Update marshmallow_sqlalchemy to version 0.16.1

### Added
- Version is now publicly available.

## [13.17.0] - 2019-02-22
### Added
- Update dependencies to latest version (pycommon_test 5.1.0, pycommon_error 2.17.0).
- Update dependencies to latest version (marshmallow 2.18.1, SQLAlchemy 1.2.18, marshmallow_sqlalchemy 0.16.0).

## [13.16.1] - 2019-02-14
### Fixed
- [SQLAlchemy] In case of multiple audit tables creation, the name of the enum action was the same for all tables. Resulting in an error at table creation.

## [13.16.0] - 2019-02-13
### Added
- [SQLAlchemy] It is now possible to perform GET queries using * as LIKE

## [13.15.0] - 2019-01-29
### Added
- Update dependencies to latest version (pycommon_test 4.10.1, pycommon_error 2.16.0).
- Update dependencies to latest version (marshmallow 2.18.0, SQLAlchemy 1.2.16).
- Add all dependencies for testing requirements.

### Fixed
- [SQLAlchemy] In case an audited model contains fields thanks to inheritance. Those fields were duplicated in audit model (resulting in audit failure).

## [13.14.0] - 2019-01-10
### Added
- Update dependencies to latest version (pycommon_test 4.10.0, pycommon_error 2.15.0).

## [13.13.0] - 2019-01-09
### Added
- [SQLAlchemy] Implement health check.

## [13.12.2] - 2018-12-20
### Fixed
- [SQLAlchemy] Commit after query.all and query.one_or_none.

## [13.12.0] - 2018-12-19
### Added
- Update dependencies to latest version (pycommon_test 4.8.0, pycommon_error 2.13.0).

## [13.11.1] - 2018-12-18
### Fixed
- [Mongo] Forbid spaces at the start and the end of column names.

## [13.11.0] - 2018-12-14
### Added
- Update dependencies to latest version (pycommon_test 4.7.0, pycommon_error 2.12.0).

## [13.10.0] - 2018-12-14
### Added
- Update dependencies to latest version (pycommon_test 4.6.0, pycommon_error 2.11.0).

## [13.9.0] - 2018-12-13
### Added
- Update dependencies to latest version (pycommon_test 4.5.0, pycommon_error 2.10.0).

## [13.8.0] - 2018-12-12
### Added
- Update dependencies to latest version (pycommon_test 4.4.0, pycommon_error 2.9.0).

## [13.7.0] - 2018-12-12
### Added
- Update dependencies to latest version (pycommon_test 4.3.0, pycommon_error 2.8.0, SQLAlchemy 1.2.15).

## [13.6.1] - 2018-12-10
### Fixed
- [Mongo] pymongo.Database is not hashable, while mongomock.Database is.

## [13.6.0] - 2018-12-10
### Added
- [Mongo] Log server information in debug.
- [Mongo] Retrieve server information only once.
- [Mongo] Allow client to set connect parameter.

## [13.5.0] - 2018-12-04
### Added
- Update dependencies to latest version (pycommon_test 4.1.0, pycommon_error 2.7.0).

## [13.4.0] - 2018-12-03
### Added
- Update dependencies to latest version (pycommon_test 4.0.0, pycommon_error 2.6.0).

## [13.3.0] - 2018-11-30
### Added
- Update dependencies to latest version.

## [13.2.0] - 2018-11-29
### Added
- Provide health_details in pycommon_database.

## [13.1.0] - 2018-11-29
### Added
- Update dependencies to latest version.

## [13.0.0] - 2018-11-28
### Changed
- [Mongo] Database health details now return a status string as first element instead of a boolean.

## [12.14.0] - 2018-11-28
### Added
- [Mongo] Allow to retrieve health details for a database.

## [12.13.0] - 2018-11-28
### Added
- Allow to retrieve GET url for a provided list of models.
- Allow to retrieve primary key field names from model.

## [12.12.0] - 2018-11-26
### Added
- Update dependencies to latest version.

## [12.11.2] - 2018-11-20
### Fixed
- [Mongo] Avoid connection upon creation of MongoClient (see https://stackoverflow.com/questions/30710427/pymongo-and-multiprocessing-serverselectiontimeouterror for details).

## [12.11.1] - 2018-11-16
### Fixed
- [Mongo] Prevent creation of audit* or counters collection only.

## [12.11.0] - 2018-11-16
### Added
- Update dependencies to latest version (impact SQLAlchemy users by providing new features)

## [12.10.0] - 2018-11-16
### Added
- [Mongo] Allow to validate rollback on model.
- Update pycommon_test to 2.0.0

## [12.9.0] - 2018-11-15
### Added
- [Mongo] Allow to retrieve last version of a document on a versioned collection (even if removed).

### Fixed
- [Mongo] Stop using deprecated count method and use count_documents instead.

## [12.8.0] - 2018-11-12
### Added
- Avoid the need to import from database module.
- [Mongo] Avoid deserialization step in post_many and put_many in case at least one error already occurred.

### Fixed
- [Mongo] Faster validation.
- [Mongo] Only consider ISO8601 as valid date or date-time format.
- [Mongo] Forbid counters collection creation.
- [Mongo] Forbid audited collection names.
- [Mongo] Post/Put on model ensure that data is of proper type.
- [Mongo] Query on default values are now returning expected content even if default value is not set in database.

## [12.7.5] - 2018-11-07
### Fixed
- [Mongo] Avoid using deprecated methods.

## [12.7.4] - 2018-10-30
### Fixed
- Update dependencies to latest version.

## [12.7.3] - 2018-10-15
### Fixed
- Check field_type is a subclass of datetime allows mocking datetime.

## [12.7.2] - 2018-10-11
### Fixed
- Update dependencies to latest version.

## [12.7.1] - 2018-10-08
### Fixed
- [Mongo] Revision was shared even if model was not versioned.

## [12.7.0] - 2018-10-05
### Added
- [Mongo] Differentiate get and delete validation to allow custom behavior in one case and not another.

## [12.6.0] - 2018-10-04
### Added
- Update dependencies to latest version.

## [12.5.0] - 2018-10-04
### Added
- [SqlAlchemy] add the order_by feature as a parameter for the service

## [12.4.1] - 2018-10-01
### Fixed
- Update dependencies to latest version.

## [12.4.0] - 2018-09-19
### Added
- [Mongo] It is now possible to avoid logging unknown fields.

## [12.3.0] - 2018-09-19
### Added
- [Mongo] ListColumn can now be sorted.

## [12.2.1] - 2018-09-14
### Fixed
- [Mongo] Allow to create fields storing None (to be able to retrieve None in case model is not retrievable on GET).

## [12.2.0] - 2018-09-13
### Added
- [Mongo] Add current_revision method to VersionedCRUDModel to retrieve current revision.

## [12.1.0] - 2018-08-31
### Added
- [Mongo] Add validate_and_deserialize_insert and validate_and_deserialize_update methods to CRUDModel

## [12.0.3] - 2018-08-30
### Fixed
- Update dependencies to latest version

## [12.0.2] - 2018-08-28
### Fixed
- [Mongo] If a field is a primary key and does not have a default value, it is not allowed to perform an update without this field, even if it is auto_incremented.
- [Mongo] Column.is_nullable is now a private field (for insert and update)

## [12.0.1] - 2018-08-27
### Fixed
- Update dependencies

## [12.0.0] - 2018-08-24
### Changed
- [Mongo] default_value must now be a default value, to provide a function use the new get_default_value parameter.
- [Mongo] DictColumn fields must now be dictionary, to provide a function use the new get_fields parameter.
- [Mongo] DictColumn index_fields must now be dictionary, to provide a function use the new get_index_fields parameter.

### Added
- [Mongo] Providing methods for a default value, DictColumn fields or index_fields no longer requires to handle empty as a normal case, received data will always be client sent data.
- [Mongo] Allow to provide min_length and max_length for dict columns.

## [11.0.0] - 2018-08-23
### Changed
- Remove flask_restplus_error and rely on pycommon-error module instead.

### Added
- [Mongo] In case an index cannot be created using partialFilterExpression, try without (even if version should allow it as some services such as Azure Cosmos DB do not implement the API properly).
- [Mongo] Log all server information.

## [10.12.5] - 2018-08-22
### Fixed
- [Mongo] Check server version before trying to use partialFilterExpression.

## [10.12.4] - 2018-08-22
### Fixed
- [Mongo] Handle connection string with parameters.
- [Mongo] Allow to provide MongoClient options in load.
- Update dependencies to latest version

## [10.12.3] - 2018-08-22
### Fixed
- [Mongo] Handle indexes without key or name.

## [10.12.2] - 2018-08-21
### Fixed
- primary_key fields were not automatically indexed as unique.
- Allow to provide a custom example value for every field.
- Return proper error in case values provided to Controller are not of the correct type.

## [10.12.1] - 2018-08-20
### Fixed
- Update dependencies to latest version.

## [10.12.0] - 2018-08-14
### Added
- [Mongo] Convert int and float to string if needed

## [10.11.1] - 2018-08-10
### Fixed
- Update dependencies to latest version.

## [10.11.0] - 2018-06-19
### Added
- [Mongo] TLS dependencies are now retrieved.

## [10.10.1] - 2018-05-30
### Fixed
- [SQLAlchemy] Postgres filter and offset are now applied.

## [10.10.0] - 2018-05-24
### Added
- [Mongo] Dump in a specified directory instead of memory.
- [Mongo] Restore from a specified directory instead of memory.
- [Mongo] Remove list_content feature.

### Security
- [Mongo] Non ISO-8601 date/time might be considered as valid if matching a commonly-used formatting.

## [10.9.4] - 2018-05-24
### Added
- [Mongo] Convert strings to int or float if needed

## [10.9.3] - 2018-05-17
### Fixed
- [Mongo] Dump per collection.
- [Mongo] Add list_content feature.

## [10.9.2] - 2018-05-17
### Fixed
- Provide errors that can be automatically parsed.

## [10.9.1] - 2018-05-14
### Added
- [Mongo] New dump and restore features to handle backups.

## [10.9.0] - 2018-05-15
### Added
- [Mongo] Allow to fail if keys are unknown (default to skip field)
- [SQLAlchemy] Audit is now using a single auto incremented key (revision)

## [10.7.0] - 2018-05-04
### Added
- Expose field names via the Controller.get_field_names() method.

## [10.6.1] - 2018-05-03
### Fixed
- [Mongo] Setting the example values taking into account the min_value and max_value and min_length and max_length.

## [10.6.0] - 2018-05-03
### Added
- [Mongo] Introduce min_value and max_value for int and float Column.
- [Mongo] Introduce min_length and max_length for str and list Column and ListColumn.

## [10.5.0] - 2018-04-26
### Changed
- [Mongo] Collection related counters are reset when delete_all is called on that collection.

## [10.4.0] - 2018-04-26
### Changed
- [Mongo] Collections indexes are now created only when missing or when update is needed.

### Fixed
- Versioning use negative value for valid fields instead of None value. Collection indexes for Versionned collections focus only on valid records using partial indexes.

## [10.3.0] - 2018-04-23
### Changed
- SQLAlchemy delete are now slower as a SELECT is performed before sending the actual DELETE.

### Added
- Allow to send multiple values when sending a query (GET or DELETE requests usually).

## [10.2.1] - 2018-04-20
### Fixed
- [Mongo] Increase robustness to allow columns not present in the column list to be present in the database

## [10.2.0] - 2018-04-12
### Added
- Controller.update_many method is now available

### Fixed
- [Mongo] Allow empty dictionary on insert and update if it should be allowed.

## [10.1.0] - 2018-03-15
### Added
- [Mongo] In case of a versioned model, audit is now performed in a single collection referencing table name and revision for each record.
- [Mongo] Choices are now available for float fields.

### Fixed
- [Mongo] Versioned models now store a shared (across all models) revision (as an integer) instead of the modification date time for each action.
- [Mongo] Rollback action is now audited.
- [Mongo] Default value was not sent back to the user on ListColumn (always an empty list).
- [Mongo] The whole document was not provided back to the list item serialization and deserialization, avoiding user custom action in some cases.
- [Mongo] Non nullable field values that were not set in database were not provided back to the controller.
- [Mongo] Update failure because of already existing index was not handled properly.
- [Mongo] Float fields were not considered as valid in case an int value was sent.
- [Mongo] Missing required fields on queries were still considered as valid.

### Security
- [Mongo] Update can violate unique index constraint and still succeed (at least with mongomock).

## [10.0.0] - 2018-03-14
### Changed
- Optional dependencies should be chosen upon installation thanks to mongo and sqlalchemy extra requires.
- CRUDModel class is now within database_sqlalchemy instead of database module.
- SQLAlchemy audit is no longer created for a model using the extra "audit" Controller.model method parameter. Use CRUDModel.audit() instead.

### Added
- Add support for Mongo database.
- All SQLAlchemy create_engine parameters can now be provided to database.load function
- A single commit is now performed for the requested action and related audit.
- Audit is now containing user name (if a user was stored within flask.g.current_user) if authentication is activated thanks to pycommon_server

### Fixed
- SQLAlchemy audit filtering on select was not working (always returning all records)

## [9.3.1] - 2018-01-18
### Fixed
- Use correct datetime iso format in sync with latest version of marshmallow_sqlalchemy (yyyy-mm-ddThh:mm:ss+00:00)
- Fix the unitests that were failing because of this

## [9.3.0] - 2018-01-08
### Added
- Database connection pool is now recycled after 1 minute by default (instead of never).
- Introduce pool_recycle parameter for database.load method allowing to specify this parameter.

### Fixed
- Increment marshmallow_sqlalchemy to 0.13.2
- Database connection is now closed on DBAPI error (mostly disconnection).

## [9.2.1] - 2017-12-04
### Fixed
- Fix marshaling of validation model failure for post_many.

## [9.2.0] - 2017-11-30
### Added
- Manage schema with SQLite.

## [9.1.0] - 2017-11-29
### Added
- SQLAlchemy model field declaration order is now kept.

## [9.0.0] - 2017-11-23
### Added
- Stop adding custom model for post.
- If field is explicitly set as autoincrement in the model, it will be tagged as a read only field in the service.

## [8.4.1] - 2017-11-21
### Added
- Add SQL Server to exception for retrieving metadata.
- Add SQL Server to exception for usage of limit/offset.

## [8.3.0] - 2017-11-13
### Added
- Better error message in case CRUDController.model was not called.

## [8.2.0] - 2017-11-13
### Added
- All CRUDController methods are now class methods to avoid useless instantiation of a controller class at runtime.
- CRUDController.post_list method renamed into post_many.

### Fixed
- Allow to use in memory database from multiple threads (if supported by the underlying database).

### Changed
## [8.1.0] - 2017-11-13
### Added
- CRUDModel.add_all method has been added.
- CRUDController.post_list method has been added.

## [8.0.0] - 2017-11-06
### Changed
- Remove the extra parameter that was introduced in 7.2.0 to handle SyBase need for quoting characters.

## [7.2.1] - 2017-11-06
### Fixed
- Provide a proper exception in case database could not be reached once model is created.

## [7.2.0] - 2017-11-03
### Added
- Sybase Column name with uppercase letters need to be quoted with []

## [7.1.0] - 2017-11-01
### Added
- Audit now manage key with auto-incremented value and field with default value.
- Audit raises error in case of missing data.

## [7.0.3] - 2017-10-23
### Fixed
- Default value provided for Swagger fields is now properly typed.

## [7.0.2] - 2017-10-20
### Fixed
- Handle date and datetime following ISO8601 in CRUD parsers.
- Remove support for time in CRUD parsers as there is no concrete example of use for now.
- Provide default value for a field in Swagger.

## [7.0.1] - 2017-10-17
### Fixed
- Use a different model name for POST if model differs from PUT.

## [7.0.0] - 2017-10-17
### Fixed
- Do not provide useless JSON request parser. Provide expected models for POST and PUT instead.

## [6.0.1] - 2017-10-17
### Fixed
- Views were still created on database.load.

## [6.0.0] - 2017-10-17
### Changed
- ModelDescriptionController class does not exists anymore, CRUDController now provides the model_description functionality.

## [5.1.4] - 2017-10-17
### Fixed
- Views are not considered as tables anymore.

## [5.1.3] - 2017-10-17
### Fixed
- Offset will not be provided anymore for Sybase models.

## [5.1.2] - 2017-10-16
### Fixed
- If no data is provided on insert or update requests, then an error 4** will be sent to the client instead of a 500.
- If multiple results could be found for a get single request, then an error 4** will be sent to the client instead of a 500.

## [5.1.1] - 2017-10-16
### Fixed
- Date and DateTime fields were not deserialized on update, preventing update on those fields.

## [5.1.0] - 2017-10-13
### Added
- Introduce audit parser and marshaller.

## [5.0.0] - 2017-10-13
### Changed
- Renamed a lot of methods and attributes and change the behavior of CRUD*. Have a look at the python template for the update procedure.

## [4.0.0] - 2017-10-12
### Changed
- create_if_needed parameter removed from the database.load function.

## [3.3.0] - 2017-10-12
### Added
- database.CRUDModel.add and database.CRUDModel.update methods now return inserted and updated models.
- database.CRUDController.post and database.CRUDController.put methods now return inserted and updated models.

## [3.2.0] - 2017-10-09
### Added
- Add Limit and Offset filter for pagination purpose in the GUI

## [3.0.1] - 2017-10-06
### Changed
- Fix Enum field not properly defined

## [3.0.0] - 2017-10-06
### Changed
- database.load_from renamed into database.load

## [2.2.0] - 2017-10-06
### Added
- Sample value for fields are now depending on the field type.
- Introduce a namespace method to CRUDController to initialize everything related to namespace (such as a JSON model)

## [2.1.0] - 2017-10-05
### Added
- CRUDController model method now provide the ability to also handle audit.

## [2.0.0] - 2017-10-02
### Changed
- sybase_url function do not exists anymore now that load_from can guess how to encode provided URL.

## [1.9.0] - 2017-10-02
### Added
- Add a user friendly error message in case python type cannot be guessed due to a parameter not containing columns.
- Add more test cases.

## [1.8.0] - 2017-10-02
### Added
- controller delete method now return the number of removed rows (instead of None)

## [1.7.0] - 2017-10-02
### Added
- remove now returns the number of removed rows (was None previously)
- load_from now display more logs on what was done (to investigate issues related to DB loading)
- load_from now return user friendly errors when provided data is invalid
- reset now log when finished and force binding once again
- Introduce test cases

### Fixed
- Properly handle a get request that should return more than one result.
- Properly reject add / update with empty data
- Validate content type on a update

## [1.6.1] - 2017-09-28
### Fixed
- ModelCouldNotBeFound error is now properly forwarded to client.

## [1.6.0] - 2017-09-28
### Added
- CRUDModel now provide an update method.
- CRUDController now handle PUT and POST differently.

## [1.5.1] - 2017-09-28
### Fixed
- Validation errors are now logged with more details on server side and properly propagated to clients.

## [1.5.0] - 2017-09-28
### Added
- Validation errors are now properly handled.

## [1.4.2] - 2017-09-27
### Fixed
- Avoid failure in case there is no schema in table_args__.

## [1.4.1] - 2017-09-27
### Fixed
- Python type was not properly extracted from SQL Alchemy field.

## [1.4.0] - 2017-09-27
### Changed
- A setter is now provide to set model on controllers and should be used to benefit from enhanced performances.

### Added
- Controllers now provide a full default behavior for GET.

## [1.3.0] - 2017-09-27
### Added
- Provide a ModelDescriptionController class to return a description of a CRUDModel.

## [1.2.0] - 2017-09-27
### Added
- Provide a CRUDController class to interact with a CRUDModel.

## [1.1.0] - 2017-09-27
### Added
- Dependencies are now set to flask-restplus 0.10.1 and marshmallow_sqlalchemy 0.13.1.

## [1.0.0] - 2017-09-27
### Changed
- Initial release.

[Unreleased]:https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.19.0...HEAD
[13.19.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.18.0...v13.19.0
[13.18.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.17.0...v13.18.0
[13.17.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.16.1...v13.17.0
[13.16.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.16.0...v13.16.1
[13.16.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.15.0...v13.16.0
[13.15.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.14.0...v13.15.0
[13.14.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.13.0...v13.14.0
[13.13.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.12.2...v13.13.0
[13.12.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.12.0...v13.12.2
[13.12.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.11.1...v13.12.0
[13.11.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.11.0...v13.11.1
[13.11.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.10.0...v13.11.0
[13.10.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.9.0...v13.10.0
[13.9.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.8.0...v13.9.0
[13.8.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.7.0...v13.8.0
[13.7.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.6.1...v13.7.0
[13.6.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.6.0...v13.6.1
[13.6.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.5.0...v13.6.0
[13.5.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.4.0...v13.5.0
[13.4.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.3.0...v13.4.0
[13.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.2.0...v13.3.0
[13.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.1.0...v13.2.0
[13.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v13.0.0...v13.1.0
[13.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.14.0...v13.0.0
[12.14.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.13.0...v12.14.0
[12.13.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.12.0...v12.13.0
[12.12.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.11.2...v12.12.0
[12.11.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.11.1...v12.11.2
[12.11.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.11.0...v12.11.1
[12.11.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.10.0...v12.11.0
[12.10.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.9.0...v12.10.0
[12.9.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.8.0...v12.9.0
[12.8.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.7.5...v12.8.0
[12.7.5]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.7.4...v12.7.5
[12.7.4]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.7.3...v12.7.4
[12.7.3]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.7.2...v12.7.3
[12.7.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.7.1...v12.7.2
[12.7.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.7.0...v12.7.1
[12.7.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.6.0...v12.7.0
[12.6.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.5.0...v12.6.0
[12.5.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.4.1...v12.5.0
[12.4.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.4.0...v12.4.1
[12.4.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.3.0...v12.4.0
[12.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.2.1...v12.3.0
[12.2.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.2.0...v12.2.1
[12.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.1.0...v12.2.0
[12.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.0.3...v12.1.0
[12.0.3]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.0.2...v12.0.3
[12.0.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.0.1...v12.0.2
[12.0.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v12.0.0...v12.0.1
[12.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v11.0.0...v12.0.0
[11.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.12.5...v11.0.0
[10.12.5]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.12.4...v10.12.5
[10.12.4]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.12.3...v10.12.4
[10.12.3]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.12.2...v10.12.3
[10.12.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.12.1...v10.12.2
[10.12.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.12.0...v10.12.1
[10.12.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.11.1...v10.12.0
[10.11.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.11.0...v10.11.1
[10.11.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.10.1...v10.11.0
[10.10.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.10.0...v10.10.1
[10.10.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.9.4...v10.10.0
[10.9.4]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.9.3...v10.9.4
[10.9.3]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.9.2...v10.9.3
[10.9.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.9.1...v10.9.2
[10.9.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.9.0...v10.9.1
[10.9.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.7.0...v10.9.0
[10.7.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.6.1...v10.7.0
[10.6.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.6.0...v10.6.1
[10.6.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.5.0...v10.6.0
[10.5.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.4.0...v10.5.0
[10.4.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.3.0...v10.4.0
[10.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.2.1...v10.3.0
[10.2.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.2.0...v10.2.1
[10.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.1.0...v10.2.0
[10.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v10.0.0...v10.1.0
[10.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v9.3.1...v10.0.0
[9.3.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v9.3.0...v9.3.1
[9.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v9.2.1...v9.3.0
[9.2.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v9.2.0...v9.2.1
[9.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v9.1.0...v9.2.0
[9.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v9.0.0...v9.1.0
[9.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v8.4.1...v9.0.0
[8.4.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v8.3.0...v8.4.1
[8.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v8.2.0...v8.3.0
[8.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v8.1.0...v8.2.0
[8.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v8.0.0...v8.1.0
[8.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.2.1...v8.0.0
[7.2.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.2.0...v7.2.1
[7.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.1.0...v7.2.0
[7.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.0.3...v7.1.0
[7.0.3]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.0.2...v7.0.3
[7.0.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.0.1...v7.0.2
[7.0.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v7.0.0...v7.0.1
[7.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v6.0.1...v7.0.0
[6.0.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v6.0.0...v6.0.1
[6.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v5.1.4...v6.0.0
[5.1.4]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v5.1.3...v5.1.4
[5.1.3]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v5.1.2...v5.1.3
[5.1.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v5.1.1...v5.1.2
[5.1.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v5.1.0...v5.1.1
[5.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v5.0.0...v5.1.0
[5.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v4.0.0...v5.0.0
[4.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v3.3.0...v4.0.0
[3.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v3.2.0...v3.3.0
[3.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v3.0.1...v3.2.0
[3.0.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v2.2.0...v3.0.0
[2.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.9.0...v2.0.0
[1.9.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.6.1...v1.7.0
[1.6.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.5.1...v1.6.0
[1.5.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.4.2...v1.5.0
[1.4.2]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.tools.digital.engie.com/GEM-Py/pycommon-database/releases/tag/v1.0.0
