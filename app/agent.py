# ruff: noqa
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
from zoneinfo import ZoneInfo

from google.adk.agents import Agent
from google.adk.apps.app import App
from google.adk.models import Gemini
from google.genai import types

from app.synth_data.codegen import generate_faker_python_script
from app.synth_data.models import Schema


def get_weather(query: str) -> str:
    """Simulates a web search. Use it get information on weather.

    Args:
        query: A string containing the location to get weather information for.

    Returns:
        A string with the simulated weather information for the queried location.
    """
    if "sf" in query.lower() or "san francisco" in query.lower():
        return "It's 60 degrees and foggy."
    return "It's 90 degrees and sunny."


def get_current_time(query: str) -> str:
    """Simulates getting the current time for a city.

    Args:
        city: The name of the city to get the current time for.

    Returns:
        A string with the current time information.
    """
    if "sf" in query.lower() or "san francisco" in query.lower():
        tz_identifier = "America/Los_Angeles"
    else:
        return f"Sorry, I don't have timezone information for query: {query}."

    tz = ZoneInfo(tz_identifier)
    now = datetime.datetime.now(tz)
    return f"The current time for query {query} is {now.strftime('%Y-%m-%d %H:%M:%S %Z%z')}"


def generate_synth_data_script(
    schema_json: str, rows_per_table: int = 100, seed: int = 42
) -> str:
    """Generate an executable faker-based Python script from an inferred schema.

    Args:
        schema_json: A JSON string that matches the `Schema` model. The agent should
            infer this from the user's business case (tables, columns, PKs, FKs).
        rows_per_table: Number of rows to generate per table.
        seed: RNG seed for deterministic output.

    Returns:
        A Python script (as text) that generates CSV files in ./out.
    """
    parsed = json.loads(schema_json)
    schema = Schema.model_validate(parsed)
    return generate_faker_python_script(schema=schema, rows_per_table=rows_per_table, seed=seed)


root_agent = Agent(
    name="synthetic_data_agent",
    model=Gemini(
        model="gemini-3-flash-preview",
        retry_options=types.HttpRetryOptions(attempts=3),
    ),
    instruction=(
        "You are an AI-driven synthetic data generator.\n"
        "\n"
        "When the user provides a raw business use case, you must:\n"
        "- Infer a normalized relational database schema (tables + attributes)\n"
        "- Define Primary Keys and Foreign Keys to ensure referential integrity\n"
        "- Output TWO artifacts:\n"
        "  1) `schema_json`: strict JSON matching this shape:\n"
        "     {\n"
        '       "name": "...",\n'
        '       "description": "...",\n'
        '       "tables": [\n'
        "         {\n"
        '           "name": "table_name",\n'
        '           "description": "...",\n'
        '           "columns": [\n'
        "             {\n"
        '               "name": "col_name",\n'
        '               "type": "int|str|float|bool|date|datetime",\n'
        '               "description": "...",\n'
        '               "primary_key": true|false,\n'
        '               "foreign_key": null OR {"ref_table": "...", "ref_column": "...", "on_delete": "restrict|cascade|set_null"|null},\n'
        '               "nullable": true|false,\n'
        '               "unique": true|false,\n'
        '               "faker": null OR "faker_provider_name"\n'
        "             }\n"
        "           ]\n"
        "         }\n"
        "       ]\n"
        "     }\n"
        "  2) A runnable Python script created by calling the tool `generate_synth_data_script`.\n"
        "\n"
        "Rules:\n"
        "- Every table must have exactly one primary key column.\n"
        "- Every foreign key must reference an existing table and column.\n"
        "- Prefer IDs as `int` PKs; use `*_id` FKs.\n"
        "- Keep data privacy-compliant: do not include real personal data.\n"
        "\n"
        "Process:\n"
        "- First, infer schema_json.\n"
        "- Then call `generate_synth_data_script(schema_json=..., rows_per_table=..., seed=...)`.\n"
        "- Finally, return the schema_json and the generated script.\n"
    ),
    tools=[generate_synth_data_script, get_weather, get_current_time],
)

app = App(root_agent=root_agent, name="app")
