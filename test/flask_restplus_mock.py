from flask_restplus import fields as flask_rest_plus_fields


class TestAPI:
    """
    Mock a Flask RestPlus API object.
    """

    @classmethod
    def model(cls, name, fields):
        """
        Return a mock of a model containing the following extra fields:

            - name: Provided model name.
            - fields: All Flask RestPlus fields provided during model creation.
            - fields_required: Dictionary of field name: required boolean.
            - fields_example: Dictionary of field name: example value.
            - fields_description: Dictionary of field name: description string.
            - fields_enum: Dictionary of field name: enum choices.
            - fields_default: Dictionary of field name: default value.
            - fields_readonly: Dictionary of field name: read only boolean.
            - fields_flask_type: Dictionary of field name: flask type string representation.
        """

        def _fields_values(all_fields: dict, value_from_field) -> dict:
            values = {}

            for field_name, field in all_fields.items():
                if isinstance(field, flask_rest_plus_fields.Nested):
                    values[field_name] = (
                        value_from_field(field),
                        _fields_values(field.nested.fields, value_from_field),
                    )
                elif isinstance(field, flask_rest_plus_fields.List):
                    values[field_name] = (
                        value_from_field(field),
                        _fields_values(
                            {f"{field_name}_inner": field.container}, value_from_field
                        ),
                    )
                else:
                    values[field_name] = value_from_field(field)

            return values

        from collections import OrderedDict

        property_list = []
        for field_name in fields:
            if hasattr(fields[field_name], "readonly") and fields[field_name].readonly:
                property_list.append(tuple((field_name, {"readOnly": True})))
            else:
                property_list.append(tuple((field_name, {})))

        model = lambda: None
        # This is used to ignore read only fields
        setattr(model, "_schema", {"properties": OrderedDict(property_list)})
        # Those are set to be able to test the content that was provided to this method
        setattr(model, "fields", fields)
        setattr(
            model,
            "fields_required",
            _fields_values(fields, lambda field: field.required),
        )
        setattr(
            model, "fields_example", _fields_values(fields, lambda field: field.example)
        )
        setattr(
            model,
            "fields_description",
            _fields_values(fields, lambda field: field.description),
        )
        setattr(
            model,
            "fields_enum",
            _fields_values(
                fields, lambda field: field.enum if hasattr(field, "enum") else None
            ),
        )
        setattr(
            model, "fields_default", _fields_values(fields, lambda field: field.default)
        )
        setattr(
            model,
            "fields_readonly",
            _fields_values(fields, lambda field: field.readonly),
        )
        setattr(
            model,
            "fields_flask_type",
            _fields_values(fields, lambda field: field.__class__.__name__),
        )
        setattr(model, "name", name)

        return model
