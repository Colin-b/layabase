# Python Common Database Changelog #

List all changes in various categories:
* Release notes: Contains all worth noting changes (breaking changes mainly)
* Enhancements
* Bug fixes
* Known issues

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
