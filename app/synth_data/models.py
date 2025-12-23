from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from pydantic import BaseModel, Field, model_validator


ScalarType = Literal["int", "str", "float", "bool", "date", "datetime"]


class ForeignKey(BaseModel):
    """Represents a foreign key constraint."""

    ref_table: str = Field(..., description="Referenced table name")
    ref_column: str = Field(..., description="Referenced column name (usually PK)")
    on_delete: Literal["restrict", "cascade", "set_null"] | None = None


class Column(BaseModel):
    """Represents a table column."""

    name: str
    type: ScalarType = "str"
    description: str | None = None

    # Constraints
    primary_key: bool = False
    foreign_key: ForeignKey | None = None
    nullable: bool = False
    unique: bool = False

    # Generation hints
    faker: str | None = Field(
        default=None,
        description=(
            "Optional Faker provider name (e.g. 'email', 'name', 'company', 'uuid4')."
        ),
    )

    @model_validator(mode="after")
    def _pk_fk_are_exclusive(self) -> Column:
        if self.primary_key and self.foreign_key is not None:
            raise ValueError("A column cannot be both primary_key and foreign_key.")
        if self.primary_key and self.nullable:
            raise ValueError("Primary key columns cannot be nullable.")
        return self


class Table(BaseModel):
    name: str
    description: str | None = None
    columns: list[Column]

    @model_validator(mode="after")
    def _validate_table(self) -> Table:
        if not self.columns:
            raise ValueError("Table must have at least one column.")

        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate column names found in table '{self.name}'.")

        if not any(c.primary_key for c in self.columns):
            raise ValueError(f"Table '{self.name}' must have a primary key column.")

        return self

    def primary_keys(self) -> list[Column]:
        return [c for c in self.columns if c.primary_key]

    def foreign_keys(self) -> list[Column]:
        return [c for c in self.columns if c.foreign_key is not None]


class Schema(BaseModel):
    name: str = "synthetic_dataset"
    description: str | None = None
    tables: list[Table]

    @model_validator(mode="after")
    def _validate_schema(self) -> Schema:
        if not self.tables:
            raise ValueError("Schema must contain at least one table.")
        tnames = [t.name for t in self.tables]
        if len(tnames) != len(set(tnames)):
            raise ValueError("Duplicate table names found in schema.")

        table_by_name = {t.name: t for t in self.tables}
        for t in self.tables:
            for col in t.columns:
                if col.foreign_key is None:
                    continue
                fk = col.foreign_key
                if fk.ref_table not in table_by_name:
                    raise ValueError(
                        f"FK '{t.name}.{col.name}' references unknown table '{fk.ref_table}'."
                    )
                ref_table = table_by_name[fk.ref_table]
                if fk.ref_column not in {c.name for c in ref_table.columns}:
                    raise ValueError(
                        f"FK '{t.name}.{col.name}' references unknown column "
                        f"'{fk.ref_table}.{fk.ref_column}'."
                    )
        return self

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(name)

    def dependency_edges(self) -> Iterable[tuple[str, str]]:
        """Yields (child_table, parent_table) edges based on FKs."""
        for t in self.tables:
            for col in t.foreign_keys():
                assert col.foreign_key is not None
                yield (t.name, col.foreign_key.ref_table)


