"""Data contracts shared by the bundled runtime."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PipelineConfig:
    """A pipeline as declared in decision.json plus its source code."""

    operators: list[dict[str, Any]]
    field_flow: str
    code: str
    rationale: str
