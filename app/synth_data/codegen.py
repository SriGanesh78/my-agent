from __future__ import annotations

import json
from collections import defaultdict, deque

from app.synth_data.models import Schema


def _toposort_tables(schema: Schema) -> list[str]:
    tables = [t.name for t in schema.tables]
    indeg: dict[str, int] = {t: 0 for t in tables}
    out: dict[str, set[str]] = {t: set() for t in tables}

    for child, parent in schema.dependency_edges():
        if child == parent:
            raise ValueError(f"Self-referential FK detected on table '{child}'.")
        if parent not in indeg or child not in indeg:
            continue
        if parent not in out[parent]:
            out[parent].add(child)
            indeg[child] += 1

    q: deque[str] = deque([t for t in tables if indeg[t] == 0])
    ordered: list[str] = []
    while q:
        t = q.popleft()
        ordered.append(t)
        for child in out[t]:
            indeg[child] -= 1
            if indeg[child] == 0:
                q.append(child)

    if len(ordered) != len(tables):
        remaining = [t for t in tables if t not in ordered]
        raise ValueError(
            "Cyclic FK dependencies detected; cannot safely order tables: "
            + ", ".join(remaining)
        )
    return ordered


def generate_faker_python_script(
    *,
    schema: Schema,
    rows_per_table: int = 100,
    seed: int = 42,
) -> str:
    """Generate an executable Python script (as text) that writes CSV data.

    The generated script:
    - Generates tables in FK dependency order (parents first)
    - Ensures FK values always reference existing parent PKs
    - Writes one CSV per table to ./out/<table>.csv
    """
    if rows_per_table <= 0:
        raise ValueError("rows_per_table must be > 0")

    order = _toposort_tables(schema)
    schema_json = schema.model_dump(mode="json")
    schema_json_str = json.dumps(schema_json, indent=2, sort_keys=True)

    # Note: we embed schema_json_str into the script for traceability.
    return f'''\
#!/usr/bin/env python3
"""
Auto-generated synthetic data generator.

Schema name: {schema.name}
Rows per table: {rows_per_table}
Seed: {seed}
"""

from __future__ import annotations

import csv
import os
import random
import uuid
from datetime import date, datetime

from faker import Faker


SCHEMA = {schema_json_str}
TABLE_ORDER = {order!r}


def _ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def _infer_provider(col_name: str) -> str | None:
    n = col_name.lower()
    if n in ("id",) or n.endswith("_id"):
        return "uuid4"
    if "email" in n:
        return "email"
    if n in ("first_name", "firstname"):
        return "first_name"
    if n in ("last_name", "lastname", "surname"):
        return "last_name"
    if "name" in n:
        return "name"
    if "company" in n or "employer" in n:
        return "company"
    if "phone" in n or "mobile" in n:
        return "phone_number"
    if "address" in n:
        return "street_address"
    if "city" in n:
        return "city"
    if "state" in n or "province" in n:
        return "state"
    if "country" in n:
        return "country"
    if "zip" in n or "postal" in n:
        return "postcode"
    if n.endswith("_at") or "timestamp" in n or "datetime" in n:
        return "date_time"
    if "date" in n:
        return "date_object"
    return None


def _generate_scalar(fake: Faker, col: dict, *, unique_tracker: dict[str, set]) -> object:
    # Prefer explicit faker hint.
    provider = col.get("faker") or _infer_provider(col["name"])
    typ = col.get("type", "str")
    key = col["name"]

    def gen() -> object:
        if provider and hasattr(fake, provider):
            v = getattr(fake, provider)()
            # Normalize uuid type to string for CSV.
            if isinstance(v, uuid.UUID):
                return str(v)
            if isinstance(v, (datetime, date)):
                return v.isoformat()
            return v

        if typ == "int":
            return random.randint(1, 10_000_000)
        if typ == "float":
            return round(random.random() * 10_000, 2)
        if typ == "bool":
            return bool(random.getrandbits(1))
        if typ == "date":
            return fake.date_object().isoformat()
        if typ == "datetime":
            return fake.date_time().isoformat()
        # default: str
        return fake.word()

    if col.get("unique"):
        seen = unique_tracker.setdefault(key, set())
        for _ in range(1000):
            v = gen()
            if v not in seen:
                seen.add(v)
                return v
        raise RuntimeError(f"Failed to generate unique value for column: {{key}}")

    return gen()


def _pk_generator(col: dict, row_idx: int) -> object:
    typ = col.get("type", "int")
    if typ == "int":
        return row_idx + 1
    # default to uuid string for non-int pk
    return str(uuid.uuid4())


def generate(out_dir: str = "out") -> None:
    fake = Faker()
    Faker.seed({seed})
    random.seed({seed})
    _ensure_out_dir(out_dir)

    # Track generated PKs for FK assignment.
    pk_values: dict[str, list[object]] = {{}}

    # Track per-table unique values.
    unique_tracker_by_table: dict[str, dict[str, set]] = {{}}

    tables_by_name = {{t["name"]: t for t in SCHEMA["tables"]}}

    for table_name in TABLE_ORDER:
        table = tables_by_name[table_name]
        cols = table["columns"]

        pk_cols = [c for c in cols if c.get("primary_key")]
        if len(pk_cols) != 1:
            raise RuntimeError(
                f"Only single-column primary keys are supported (table={{table_name}})."
            )
        pk_col = pk_cols[0]

        out_path = os.path.join(out_dir, f"{{table_name}}.csv")
        unique_tracker = unique_tracker_by_table.setdefault(table_name, {{}})

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[c["name"] for c in cols])
            writer.writeheader()

            pk_values.setdefault(table_name, [])

            for i in range({rows_per_table}):
                row: dict[str, object] = {{}}
                for c in cols:
                    cname = c["name"]
                    if c.get("primary_key"):
                        row[cname] = _pk_generator(c, i)
                        continue

                    fk = c.get("foreign_key")
                    if fk is not None:
                        parent_table = fk["ref_table"]
                        candidates = pk_values.get(parent_table, [])
                        if not candidates:
                            raise RuntimeError(
                                f"FK {{table_name}}.{{cname}} references "
                                f"{{parent_table}} but no parent rows exist."
                            )
                        row[cname] = random.choice(candidates)
                        continue

                    if c.get("nullable") and random.random() < 0.05:
                        row[cname] = ""
                    else:
                        row[cname] = _generate_scalar(fake, c, unique_tracker=unique_tracker)

                writer.writerow(row)
                pk_values[table_name].append(row[pk_col["name"]])

    print(f"Wrote CSV files to: {{os.path.abspath(out_dir)}}")


if __name__ == "__main__":
    generate()
'''


