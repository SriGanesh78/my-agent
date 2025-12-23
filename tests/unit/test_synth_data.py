# mypy: disable-error-code="no-untyped-call"

import json

import pytest

from app.synth_data.codegen import generate_faker_python_script
from app.synth_data.models import Schema


def test_schema_validation_fk_references_existing() -> None:
    schema = Schema.model_validate(
        {
            "name": "test",
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "user_id", "type": "int", "primary_key": True},
                        {"name": "email", "type": "str", "unique": True},
                    ],
                },
                {
                    "name": "orders",
                    "columns": [
                        {"name": "order_id", "type": "int", "primary_key": True},
                        {
                            "name": "user_id",
                            "type": "int",
                            "foreign_key": {
                                "ref_table": "users",
                                "ref_column": "user_id",
                            },
                        },
                    ],
                },
            ],
        }
    )
    assert schema.table("users").primary_keys()[0].name == "user_id"


def test_schema_validation_rejects_unknown_fk_table() -> None:
    with pytest.raises(ValueError):
        Schema.model_validate(
            {
                "name": "bad",
                "tables": [
                    {
                        "name": "orders",
                        "columns": [
                            {"name": "order_id", "type": "int", "primary_key": True},
                            {
                                "name": "user_id",
                                "type": "int",
                                "foreign_key": {
                                    "ref_table": "users",
                                    "ref_column": "user_id",
                                },
                            },
                        ],
                    }
                ],
            }
        )


def test_codegen_embeds_schema_and_table_order() -> None:
    schema = Schema.model_validate(
        {
            "name": "test",
            "tables": [
                {
                    "name": "users",
                    "columns": [{"name": "user_id", "type": "int", "primary_key": True}],
                },
                {
                    "name": "orders",
                    "columns": [
                        {"name": "order_id", "type": "int", "primary_key": True},
                        {
                            "name": "user_id",
                            "type": "int",
                            "foreign_key": {
                                "ref_table": "users",
                                "ref_column": "user_id",
                            },
                        },
                    ],
                },
            ],
        }
    )

    script = generate_faker_python_script(schema=schema, rows_per_table=3, seed=1)
    assert "from faker import Faker" in script
    assert "TABLE_ORDER" in script
    assert '"name": "test"' in script

    # Ensure the embedded schema is valid JSON inside the script.
    start = script.index("SCHEMA = ") + len("SCHEMA = ")
    end = script.index("\nTABLE_ORDER")
    embedded = script[start:end].strip()
    parsed = json.loads(embedded)
    assert parsed["name"] == "test"


