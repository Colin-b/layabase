# Python Common Database Changelog #

List all changes in various categories:
* Release notes: Contains all worth noting changes (breaking changes mainly)
* Enhancements
* Bug fixes
* Known issues

## Version 10.1.0 (2018-03-15) ##

### Enhancements ###

- [Mongo] In case of a versioned model, audit is now performed in a single collection referencing table name and revision for each record.

### Bug fixes ###

- [Mongo] Versioned models now store a shared (across all models) revision (as an integer) instead of the modification date time for each action.
- [Mongo] Rollback action is now audited.
- [Mongo] Default value was not sent back to the user on ListColumn (always an empty list).
- [Mongo] The whole document was not provided back to the list item serialization and deserialization, avoiding user custom action in some cases.
- [Mongo] Non nullable field values that were not set in database were not provided back to the controller.
- [Mongo] Update failure because of already existing index was not handled properly.

### Known issues ###

- [Mongo] Update can violate unique index constraint and still succeed (at least with mongomock).

## Version 10.0.0 (2018-03-14) ##

### Release notes ###

- Optional dependencies should be chosen upon installation thanks to mongo and sqlalchemy extra requires.
- CRUDModel class is now within database_sqlalchemy instead of database module.
- SQLAlchemy audit is no longer created for a model using the extra "audit" Controller.model method parameter. Use CRUDModel.audit() instead.

### Enhancements ###

- Add support for Mongo database.
- All SQLAlchemy create_engine parameters can now be provided to database.load function
- A single commit is now performed for the requested action and related audit.
- Audit is now containing user name (if a user was stored within flask.g.current_user) if authentication is activated thanks to pycommon_server

### Bug fixes ###

- SQLAlchemy audit filtering on select was not working (always returning all records)

## Version 9.3.1 (2018-01-18) ##

### Bug fixes ###

- Use correct datetime iso format in sync with latest version of marshmallow_sqlalchemy (yyyy-mm-ddThh:mm:ss+00:00)
- Fix the unitests that were failing because of this

## Version 9.3.0 (2018-01-08) ##

### Enhancements ###

- Database connection pool is now recycled after 1 minute by default (instead of never).
- Introduce pool_recycle parameter for database.load method allowing to specify this parameter.

### Bug fixes ###

- Increment marshmallow_sqlalchemy to 0.13.2
- Database connection is now closed on DBAPI error (mostly disconnection).

## Version 9.2.1 (2017-12-04) ##

### Bug fixes ###

- Fix marshaling of validation model failure for post_many.

## Version 9.2.0 (2017-11-30) ##

### Enhancements ###

- Manage schema with SQLite.

## Version 9.1.0 (2017-11-29) ##

### Enhancements ###

- SQLAlchemy model field declaration order is now kept.

## Version 9.0.0 (2017-11-23) ##

### Enhancements ###

- Stop adding custom model for post.
- If field is explicitly set as autoincrement in the model, it will be tagged as a read only field in the service.

## Version 8.4.1 (2017-11-21) ##

### Enhancements ###

- Add SQL Server to exception for retrieving metadata.
- Add SQL Server to exception for usage of limit/offset.

## Version 8.3.0 (2017-11-13) ##

### Enhancements ###

- Better error message in case CRUDController.model was not called.

## Version 8.2.0 (2017-11-13) ##

### Enhancements ###

- All CRUDController methods are now class methods to avoid useless instantiation of a controller class at runtime.
- CRUDController.post_list method renamed into post_many.

### Bug fixes ###

- Allow to use in memory database from multiple threads (if supported by the underlying database).

### Release notes ###

## Version 8.1.0 (2017-11-13) ##

### Enhancements ###

- CRUDModel.add_all method has been added.
- CRUDController.post_list method has been added.

## Version 8.0.0 (2017-11-06) ##

### Release notes ###

- Remove the extra parameter that was introduced in 7.2.0 to handle SyBase need for quoting characters.

## Version 7.2.1 (2017-11-06) ##

### Bug fixes ###

- Provide a proper exception in case database could not be reached once model is created.

## Version 7.2.0 (2017-11-03) ##

### Enhancements ###

- Sybase Column name with uppercase letters need to be quoted with []

## Version 7.1.0 (2017-11-01) ##

### Enhancements ###

- Audit now manage key with auto-incremented value and field with default value.
- Audit raises error in case of missing data.

## Version 7.0.3 (2017-10-23) ##

### Bug fixes ###

- Default value provided for Swagger fields is now properly typed.

## Version 7.0.2 (2017-10-20) ##

### Bug fixes ###

- Handle date and datetime following ISO8601 in CRUD parsers.
- Remove support for time in CRUD parsers as there is no concrete example of use for now.
- Provide default value for a field in Swagger.

## Version 7.0.1 (2017-10-17) ##

### Bug fixes ###

- Use a different model name for POST if model differs from PUT.

## Version 7.0.0 (2017-10-17) ##

### Bug fixes ###

- Do not provide useless JSON request parser. Provide expected models for POST and PUT instead.

## Version 6.0.1 (2017-10-17) ##

### Bug fixes ###

- Views were still created on database.load.

## Version 6.0.0 (2017-10-17) ##

### Release notes ###

- ModelDescriptionController class does not exists anymore, CRUDController now provides the model_description functionality.

## Version 5.1.4 (2017-10-17) ##

### Bug fixes ###

- Views are not considered as tables anymore.

## Version 5.1.3 (2017-10-17) ##

### Bug fixes ###

- Offset will not be provided anymore for Sybase models.

## Version 5.1.2 (2017-10-16) ##

### Bug fixes ###

- If no data is provided on insert or update requests, then an error 4** will be sent to the client instead of a 500.
- If multiple results could be found for a get single request, then an error 4** will be sent to the client instead of a 500.

## Version 5.1.1 (2017-10-16) ##

### Bug fixes ###

- Date and DateTime fields were not deserialized on update, preventing update on those fields.

## Version 5.1.0 (2017-10-13) ##

### Enhancements ###

- Introduce audit parser and marshaller.

## Version 5.0.0 (2017-10-13) ##

### Release notes ###

- Renamed a lot of methods and attributes and change the behavior of CRUD*. Have a look at the python template for the update procedure.

## Version 4.0.0 (2017-10-12) ##

### Release notes ###

- create_if_needed parameter removed from the database.load function.

## Version 3.3.0 (2017-10-12) ##

### Enhancements ###

- database.CRUDModel.add and database.CRUDModel.update methods now return inserted and updated models.
- database.CRUDController.post and database.CRUDController.put methods now return inserted and updated models.

## Version 3.2.0 (2017-10-09) ##

### Enhancements ###

- Add Limit and Offset filter for pagination purpose in the GUI

## Version 3.0.1 (2017-10-06) ##

### Release notes ###

- Fix Enum field not properly defined

## Version 3.0.0 (2017-10-06) ##

### Release notes ###

- database.load_from renamed into database.load

## Version 2.2.0 (2017-10-06) ##

### Enhancements ###

- Sample value for fields are now depending on the field type.
- Introduce a namespace method to CRUDController to initialize everything related to namespace (such as a JSON model)

## Version 2.1.0 (2017-10-05) ##

### Enhancements ###

- CRUDController model method now provide the ability to also handle audit.

## Version 2.0.0 (2017-10-02) ##

### Release notes ###

- sybase_url function do not exists anymore now that load_from can guess how to encode provided URL.

## Version 1.9.0 (2017-10-02) ##

### Enhancements ###

- Add a user friendly error message in case python type cannot be guessed due to a parameter not containing columns.
- Add more test cases.

## Version 1.8.0 (2017-10-02) ##

### Enhancements ###

- controller delete method now return the number of removed rows (instead of None)

## Version 1.7.0 (2017-10-02) ##

### Enhancements ###

- remove now returns the number of removed rows (was None previously)
- load_from now display more logs on what was done (to investigate issues related to DB loading)
- load_from now return user friendly errors when provided data is invalid
- reset now log when finished and force binding once again
- Introduce test cases

### Bug fixes ###

- Properly handle a get request that should return more than one result.
- Properly reject add / update with empty data
- Validate content type on a update

## Version 1.6.1 (2017-09-28) ##

### Bug fixes ###

- ModelCouldNotBeFound error is now properly forwarded to client.

## Version 1.6.0 (2017-09-28) ##

### Enhancements ###

- CRUDModel now provide an update method.
- CRUDController now handle PUT and POST differently.

## Version 1.5.1 (2017-09-28) ##

### Bug fixes ###

- Validation errors are now logged with more details on server side and properly propagated to clients.

## Version 1.5.0 (2017-09-28) ##

### Enhancements ###

- Validation errors are now properly handled.

## Version 1.4.2 (2017-09-27) ##

### Bug fixes ###

- Avoid failure in case there is no schema in table_args__.

## Version 1.4.1 (2017-09-27) ##

### Bug fixes ###

- Python type was not properly extracted from SQL Alchemy field.

## Version 1.4.0 (2017-09-27) ##

### Release notes ###

- A setter is now provide to set model on controllers and should be used to benefit from enhanced performances.

### Enhancements ###

- Controllers now provide a full default behavior for GET.

## Version 1.3.0 (2017-09-27) ##

### Enhancements ###

- Provide a ModelDescriptionController class to return a description of a CRUDModel.

## Version 1.2.0 (2017-09-27) ##

### Enhancements ###

- Provide a CRUDController class to interact with a CRUDModel.

## Version 1.1.0 (2017-09-27) ##

### Enhancements ###

- Dependencies are now set to flask-restplus 0.10.1 and marshmallow_sqlalchemy 0.13.1.

## Version 1.0.0 (2017-09-27) ##

### Release notes ###

- Initial release.
