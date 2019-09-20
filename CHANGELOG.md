# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- You can now query mongo on something else than equality: >, >=, <, <=. Refer to documentation for details.

### Changed
- As a result of the fact that it's possible to query on non-equality, the field type is now string for float, date, datetime and int within the OpenAPI definition
- Update [marshmallow](https://marshmallow.readthedocs.io/en/latest/changelog.html) to version 3.2.0

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
- Update pymongo to version 3.9.0
- Update [SQLAlchemy](https://docs.sqlalchemy.org/en/13/changelog/index.html) to version 1.3.7
- Update [marshmallow](https://marshmallow.readthedocs.io/en/latest/changelog.html) to version 3.0.0

## [1.0.0] - 2019-08-01
### Changed
- Initial release.

[Unreleased]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.1.1...HEAD
[1.1.1]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.1.0...v1.1.1
[1.1.0]:https://github.tools.digital.engie.com/gempy/layabase/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.tools.digital.engie.com/gempy/layabase/releases/tag/v1.0.0
