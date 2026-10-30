"""Compact evolution history, as the original controller fed it to every
PipelineAgent prompt (controller._evolution_history + prompts._history_block).

Accepted iterations carry only a summary. Rejected ones also carry their
execution observation and the saved diagnosis, so the agent does not repeat a
strategy that already failed. Prior pipeline code and full reviews are left out.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .diagnostic import DIAGNOSTIC_FILENAME

_VISIBLE_ENTRIES = 12


def _compact_text(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _compact_diagnostic(value: Any) -> dict[str, Any] | None:
    """Keep only small, mechanism-oriented evidence."""
    if not isinstance(value, dict):
        return None
    output: dict[str, Any] = {}
    if value.get("status"):
        output["status"] = value["status"]
    if value.get("summary"):
        output["summary"] = _compact_text(value["summary"], 500)
    findings = value.get("findings")
    if isinstance(findings, list):
        output["findings"] = [
            {
                key: _compact_text(item.get(key, ""), 280)
                for key in ("step", "severity", "evidence", "diagnosis", "recommended_action")
                if item.get(key) not in (None, "")
            }
            for item in findings[:3]
            if isinstance(item, dict)
        ]
    mismatches = value.get("contract_mismatches")
    if isinstance(mismatches, list):
        output["contract_mismatches"] = [_compact_text(item, 300) for item in mismatches[:3]]
    if value.get("error"):
        output["error"] = _compact_text(value["error"], 400)
    return output or None


def evolution_history(state: Any) -> dict[str, Any]:
    """Finished iterations, oldest first; the latest twelve are listed."""
    entries: list[dict[str, Any]] = []
    for it in sorted(state.iterations, key=lambda item: item.iteration):
        if it.iteration >= state.next_iteration:
            continue  # still being repaired or re-reviewed
        directory = Path(it.directory)
        decision = _read_json(directory / "decision.json") or {}
        ops = decision.get("ops") if isinstance(decision.get("ops"), list) else []
        outcome = "accepted" if it.accepted_as_incumbent else "rejected"
        entry: dict[str, Any] = {
            "iteration": it.iteration,
            "outcome": outcome,
            "review_score": it.review_score,
            "passed": bool(it.passed),
            "operators": [
                str(op.get("name", "?")) if isinstance(op, dict) else str(op) for op in ops[:8]
            ],
            "decision": _compact_text(decision.get("reason"), 240),
            "issues": (
                [_compact_text(issue, 160) for issue in it.issues[:2]]
                if it.review_score is not None
                else [_compact_text(it.execution_error or "execution failed", 160)]
            ),
        }
        if outcome == "rejected":
            entry["execution_observation"] = it.execution_observation_path
            diagnostic = _compact_diagnostic(_read_json(directory / DIAGNOSTIC_FILENAME))
            if diagnostic:
                entry["pipeline_diagnostic"] = diagnostic
        entries.append(entry)
    visible = entries[-_VISIBLE_ENTRIES:]
    return {
        "note": (
            "Do not repeat a strategy that was already rejected. Accepted entries are "
            "summaries only; a rejected entry's observation and diagnosis are there to "
            "avoid repeating its failure, not a pipeline to inherit. The parent to build "
            "on is the incumbent."
        ),
        "earlier_entries_omitted": len(entries) - len(visible),
        "entries": visible,
    }


def write_history_snapshot(workspace: Any) -> dict[str, Any]:
    """Recompute the history and save it as evolution_history.json in the
    workspace, for people to audit and for an agent to read from disk. The
    tool responses stay authoritative: they are recomputed on every call."""
    history = evolution_history(workspace.state())
    try:
        (workspace.root / "evolution_history.json").write_text(
            json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass  # a convenience copy; never a reason to fail a submit
    return history
