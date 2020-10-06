# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- `required_on_query` should be set within `layabase` key inside info (was linked to `marshmallow` key previously).
- `allow_comparison_signs` should be set within `layabase` key inside info (was linked to `marshmallow` key previously).
- `interpret_star_character` should be set within `layabase` key inside info (was linked to `marshmallow` key previously).
- `controller.query_get_parser` has been replaced by `controller.flask_restx.query_get_parser`.
- `controller.query_delete_parser` has been replaced by `controller.flask_restx.query_delete_parser`.
- `controller.query_rollback_parser` has been replaced by `controller.flask_restx.query_rollback_parser`.
- `controller.query_get_history_parser` has been replaced by `controller.flask_restx.query_get_history_parser`.
- `controller.query_get_audit_parser` has been replaced by `controller.flask_restx.query_get_audit_parser`.
- `controller.namespace` has been replaced by `controller.flask_restx.init_models`.
- `controller.json_post_model` has been replaced by `controller.flask_restx.json_post_model`.
- `controller.json_put_model` has been replaced by `controller.flask_restx.json_put_model`.
- `controller.get_response_model` has been replaced by `controller.flask_restx.get_response_model`.
- `controller.get_history_response_model` has been replaced by `controller.flask_restx.get_history_response_model`.
- `controller.get_audit_response_model` has been replaced by `controller.flask_restx.get_audit_response_model`.
- `controller.get_model_description_response_model` has been replaced by `controller.flask_restx.get_model_description_response_model`.
- [Mongo] Update of document(s) now raise a ValidationFailed instead of a ModelCouldNotBeFound in case the document(s) to update do not exists.
- [SQLAlchemy] Update of row(s) now raise a ValidationFailed instead of a ModelCouldNotBeFound in case the row(s) to update do not exists.
- [SQLAlchemy] Model schema now returns a `SQLAlchemyAutoSchema` instance instead of the deprecated `ModelSchema`.
- [SQLAlchemy] Iterate over SQLAlchemy fields to find the one required on queries instead of creating a Marshmallow schema.
- Update [marshmallow_sqlalchemy](https://marshmallow-sqlalchemy.readthedocs.io/en/latest/changelog.html) version from `0.21.*` to `0.23.*`.
- Update [black](https://pypi.org/project/black/) version from `master` to `20.8b1`.
- `flask-restx` is now an optional dependency.
- `layaberr.ValidationFailed` are not sent anymore, `layabase.ValidationFailed` are sent instead.

### Fixed
- [Mongo] Fix adding field to parser adding only one level of multilevel DictColumn.
- [Mongo] Insertion of document(s) via controller is now faster (removing duplicated check for auto incremented fields and useless dict(s) creation).
- [SQLAlchemy] Insertion of document(s) via model now ensure that auto incremented fields will be skipped. It was only performed when using controller.

### Removed
- `flask-restplus` is not supported anymore. `flask-restx` is used instead.
- `layaberr` is not a dependency anymore.

## [3.5.1] - 2020-01-30
### Fixed
- [SQLAlchemy] Flask-RestPlus argument parsers (for GET and DELETE queries) are now restricting values in case the underlying field is of Enum type.

## [3.5.0] - 2020-01-07
### Changed
- Update [marshmallow_sqlalchemy](https://marshmallow-sqlalchemy.readthedocs.io/en/latest/changelog.html) to version 0.21.*

## [3.4.0] - 2019-12-02
### Added
- Initial release.

[Unreleased]: https://github.com/Colin-b/layabase/compare/v4.0.0...HEAD
[4.0.0]: https://github.com/Colin-b/layabase/compare/v3.5.1...v4.0.0
[3.5.1]: https://github.com/Colin-b/layabase/compare/v3.5.0...v3.5.1
[3.5.0]: https://github.com/Colin-b/layabase/compare/v3.4.0...v3.5.0
[3.4.0]: https://github.com/Colin-b/layabase/releases/tag/v3.4.0
