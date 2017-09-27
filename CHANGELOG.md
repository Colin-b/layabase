# Python Common Database Changelog #

List all changes in various categories:
* Release notes: Contains all worth noting changes (breaking changes mainly)
* Enhancements
* Bug fixes
* Known issues

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
