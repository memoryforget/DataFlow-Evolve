"""Build evidence-scoped diagnostic tasks for a host-managed fresh sub-agent.

The MCP harness prepares the task; Claude Code or Codex starts and waits for
its native sub-agent. The parent saves the returned JSON diagnosis.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .execution_observation import OBSERVATION_FILENAME

DIAGNOSTIC_FILENAME = "pipeline_diagnostic.json"
_MAX_PROMPT_CHARS = 80_000
_MAX_DECLARED_OPERATORS = 20


def latest_review_record(iteration_dir: str | Path) -> dict[str, Any] | None:
    """Last record of the iteration's review.jsonl, the verdict currently in force."""
    path = Path(iteration_dir) / "review.jsonl"
    try:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except OSError:
        return None
    for line in reversed(lines):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            return record
    return None


def _review_context(record: dict[str, Any] | None, issues: list[str]) -> dict[str, Any]:
    """Review evidence as the original passes it: hard metrics, then criteria feedback."""
    result = (record or {}).get("result") or {}
    hard = result.get("hard_metrics") or {}
    context: dict[str, Any] = {
        "validation_metrics": {
            key: hard.get(key)
            for key in ("dup_rate", "null_rate", "parse_valid_rate", "contamination_rate")
        },
    }
    feedback = result.get("domain_feedback")
    if isinstance(feedback, dict) and feedback.get("review_mode") == "criteria":
        context.update({
            "review_mode": "criteria",
            "quality_criteria_feedback": feedback,
            "issues": list(result.get("issues") or issues),
        })
    else:
        # A review that produced no verdict, or a record written before the
        # criteria feedback was stored, still carries its issues.
        context["issues"] = list(result.get("issues") or issues)
    if record and record.get("status") == "error":
        context["review_status"] = "error: the review LLM produced no verdict"
    return context


def _declared_operators(iter_dir: Path) -> list[dict[str, Any]]:
    try:
        decision = json.loads((iter_dir / "decision.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    ops = decision.get("ops") if isinstance(decision, dict) else None
    if not isinstance(ops, list):
        return []
    return [item for item in ops[:_MAX_DECLARED_OPERATORS] if isinstance(item, dict)]


def build_diagnostic_context(
    iteration_dir: str | Path,
    task_description: str,
    review_issues: list[str] | None = None,
    max_prompt_chars: int = _MAX_PROMPT_CHARS,
    *,
    domain_skill_paths: list[str] | None = None,
    execution_observation_path: str | None = None,
    execution_error: str | None = None,
    quality_criteria: list[str] | None = None,
    target_schema: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the read-only diagnostic prompt of the original framework.

    Returns:
      prompt_text       — the diagnostic prompt to reason over
      observation_path  — execution_observation.json, or None if absent
      source_files      — artifacts the agent may inspect read-only
      output_path       — where the agent saves its JSON diagnosis
    """
    iter_dir = Path(iteration_dir).resolve()
    observation = iter_dir / OBSERVATION_FILENAME
    observation_path = str(observation) if observation.is_file() else None
    # The latest attempt may have failed before replacing a previous receipt.
    latest_evidence = Path(execution_observation_path) if execution_observation_path else None
    if latest_evidence and latest_evidence.is_file():
        if observation_path and latest_evidence.stat().st_mtime_ns > observation.stat().st_mtime_ns:
            observation_path = None
    elif execution_error:
        observation_path = None
    evidence_paths = [observation_path] if observation_path else []
    if latest_evidence and latest_evidence.is_file() and str(latest_evidence) not in evidence_paths:
        evidence_paths.append(str(latest_evidence))
    domain_paths = list(dict.fromkeys(str(Path(path).resolve()) for path in (domain_skill_paths or [])))
    source_files = [
        str(path) for path in (
            iter_dir / "pipeline.py",
            iter_dir / "decision.json",
            *sorted((iter_dir / "operators").rglob("*.py")),
        )
        if path.is_file()
    ]
    review_context = _review_context(
        None if execution_error else latest_review_record(iter_dir), list(review_issues or [])
    )

    prompt = (
        "You are an independent pipeline execution diagnostic sub-agent.\n"
        "Use only this task and its listed evidence; inspect artifacts read-only and return JSON "
        "to the parent agent.\n\n"
        "Judge whether the executed pipeline matches its declared design, including operator "
        "responsibilities, filtering rules and quotas. Use the framework-generated "
        "execution_observation.json as the primary execution evidence, and only when needed "
        "inspect the listed pipeline.py, operators/ and decision.json read-only.\n"
        "Compare actual row counts, retention rates, fields, low-cardinality field distributions "
        "and bounded samples step by step, and locate the first operator whose actual result "
        "departs from its declaration.\n"
        "Do not modify, create, delete or execute pipeline files, and do not read the full input "
        "or output JSONL; use only the bounded samples in execution_observation.json.\n"
        "Check execution mechanisms and domain-rule compliance. Record evidence gaps when "
        "the current attempt has only failure logs; ignore receipts from earlier attempts.\n\n"
        "Return exactly one JSON object in this format (keep the key names):\n"
        '{"status":"ok","summary":"...","findings":[{"step":"...",'
        '"severity":"low|medium|high","evidence":"...","diagnosis":"...",'
        '"recommended_action":"..."}],"contract_mismatches":[]}\n\n'
        f"Current iteration directory: {iter_dir}\n"
        f"Execution observation file: {observation_path or 'missing'}\n"
        "Artifacts you may inspect read-only:\n"
        + "\n".join(f"- {path}" for path in source_files)
        + "\nExecution evidence for the current attempt:\n"
        + "\n".join(f"- {path}" for path in evidence_paths)
        + "\nDomain skill files (read for applicable rules):\n"
        + "\n".join(f"- {path}" for path in domain_paths)
        + "\nCurrent execution error:\n"
        + str(execution_error or "none")
        + "\nQuality requirements and target schema:\n"
        + json.dumps({"quality_criteria": quality_criteria or [], "target_schema": target_schema or {}}, ensure_ascii=False)
        + "\n\nTask:\n"
        + str(task_description)
        + "\nDeclared operators:\n"
        + json.dumps(_declared_operators(iter_dir), ensure_ascii=False, default=str)
        + "\nReview evidence (context only, not a scoring target):\n"
        + json.dumps(review_context, ensure_ascii=False, default=str)
        + "\n"
    )
    limit = max(1000, int(max_prompt_chars))
    if len(prompt) > limit:
        marker = "\n[diagnostic prompt truncated by the harness]\n"
        prompt = prompt[: max(0, limit - len(marker))] + marker
    return {
        "prompt_text": prompt,
        "observation_path": observation_path,
        "source_files": source_files,
        "evidence_paths": evidence_paths,
        "domain_skill_paths": domain_paths,
        "output_path": str(iter_dir / DIAGNOSTIC_FILENAME),
    }
