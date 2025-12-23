# ruff: noqa
"""ADK loader compatibility agent.

When running `adk web app`, ADK treats each subdirectory under `app/` as a
candidate agent (e.g. `app/synth_data`). This file prevents "No root_agent found"
errors if `synth_data` is selected in the playground UI.

Primary agent remains `app/agent.py`.
"""

import json

from google.adk.agents import Agent
from google.adk.models import Gemini
from google.genai import types

from .codegen import generate_faker_python_script
from .models import Schema


def generate_synth_data_script(
    schema_json: str, rows_per_table: int = 100, seed: int = 42
) -> str:
    """Generate an executable faker-based Python script from an inferred schema."""
    parsed = json.loads(schema_json)
    schema = Schema.model_validate(parsed)
    return generate_faker_python_script(schema=schema, rows_per_table=rows_per_table, seed=seed)


root_agent = Agent(
    name="synth_data_agent",
    model=Gemini(
        model="gemini-3-flash-preview",
        retry_options=types.HttpRetryOptions(attempts=3),
    ),
    instruction=(
        "This is a helper agent for synthetic data generation.\n"
        "If you intended to use the main agent, select the parent agent instead.\n"
        "Provide `schema_json` and I will return an executable faker script."
    ),
    tools=[generate_synth_data_script],
)


