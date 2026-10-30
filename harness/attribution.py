"""Downstream bad-case attribution, ported from the original framework
(prompts.build_downstream_attribution_prompt + agents/downstream_attribution).

An optional LLM call made by the harness, not a spawned agent. It reads the
raw bad cases an eval script returned and reduces them to a data-side
diagnosis. The raw cases stay in the job directory for human audit; only the
diagnosis reaches the agent that writes pipelines, so benchmark questions and
answers never flow into the data it generates.
"""
from __future__ import annotations

import json
import math
import time
import urllib.error
from typing import Any

from .reviewer import _call_llm, _extract_json

_BAD_CASE_RESPONSE_HEAD_CHARS = 1200
_BAD_CASE_RESPONSE_TAIL_CHARS = 1800
_RUNAWAY_RESPONSE_HEAD_CHARS = 500
_RUNAWAY_RESPONSE_TAIL_CHARS = 500
_RAW_TEXT_LIMIT = 20000

ALLOWED_CATEGORIES = {
    "missing_context",
    "unsupported_answer",
    "reasoning_depth",
    "coverage_gap",
    "data_noise",
    "format_or_parsing",
    "distribution_mismatch",
    "other",
}


def _compact(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _clean_list(value: Any, limit: int, item_limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    return [text for text in (_compact(item, item_limit) for item in value[:limit]) if text]


def _bounded_response(response: str, length_truncated: bool) -> tuple[str, int]:
    """Head and tail of a long model response; the full text stays in result.json."""
    head = _RUNAWAY_RESPONSE_HEAD_CHARS if length_truncated else _BAD_CASE_RESPONSE_HEAD_CHARS
    tail = _RUNAWAY_RESPONSE_TAIL_CHARS if length_truncated else _BAD_CASE_RESPONSE_TAIL_CHARS
    if len(response) <= head + tail:
        return response, 0
    omitted = len(response) - head - tail
    marker = (f"\n\n...[{omitted} chars omitted from prompt; full response retained for "
              "human audit only]...\n\n")
    return response[:head] + marker + response[-tail:], omitted


def _review_text(record: dict[str, Any] | None) -> str:
    result = (record or {}).get("result") or {}
    feedback = result.get("domain_feedback") or {}
    if result.get("review_score") is None:
        return "No static review record."
    issues = "; ".join(str(i) for i in (result.get("issues") or [])[:4]) or "none"
    if feedback.get("review_mode") == "criteria":
        rate = float(feedback.get("criteria_met_rate", result["review_score"]))
        return (
            f"review_mode=criteria; review_score={float(result['review_score']):.4f}; "
            f"criteria_met_rate={rate:.3f}; "
            f"criteria_assessments={json.dumps(feedback.get('criteria_assessments', []), ensure_ascii=False)}; "
            f"critical_failures={json.dumps(feedback.get('critical_failures', []), ensure_ascii=False)}; "
            f"issues={issues}"
        )
    return f"review_score={float(result['review_score']):.4f}; issues={issues}"


def _count(item: dict[str, Any], key: str) -> Any:
    value = item.get(key)
    return value if value is not None else "unknown"


def build_prompt(
    *,
    task_description: str,
    target_schema: Any,
    quality_criteria: list[str],
    operators: list[str],
    review_record: dict[str, Any] | None,
    checkpoint: dict[str, Any],
    benchmarks: dict[str, dict[str, Any]],
    deltas_previous: dict[str, float],
    deltas_baseline: dict[str, float],
    max_findings: int,
    max_actions: int,
) -> tuple[str, set[str]]:
    """The original attribution prompt, and the evidence ids it may cite."""
    lines = [
        "You are a downstream evaluation attributor: attribute model benchmark failures to "
        "causes that the data pipeline can fix.",
        "Do not treat any single benchmark question as training data, and do not restate or "
        "hardcode questions or answers.",
        "Distinguish data_side=true (fixable by data generation) from data_side=false (model, "
        "parser or infrastructure problems).",
        "Prefer reusable patterns across several cases; an isolated single case may deserve a "
        "lower confidence.",
        "Every finding must cite evidence_ids and give an actionable filtering, context, "
        "rewriting, coverage or difficulty strategy.",
        "Target task:",
        task_description,
        f"Target schema: {target_schema}",
        f"Quality criteria: {quality_criteria}",
        f"Current pipeline operators: {'>'.join(operators) or 'unknown'}",
        f"Current static review: {_review_text(review_record)}",
        f"Downstream checkpoint: stage={checkpoint.get('stage')}, "
        f"checkpoint={checkpoint.get('checkpoint_iteration')}, "
        f"incumbent={checkpoint.get('incumbent_iteration')}, model={checkpoint.get('model') or 'unknown'}",
    ]
    evidence_ids: set[str] = set()
    for name, item in benchmarks.items():
        lines.append(
            f"\n### benchmark={name}; metric={item['metric']}; score={item['score']:.6f}; "
            f"incorrect={_count(item, 'incorrect_count')}; wrong={_count(item, 'wrong_answer_count')}; "
            f"parse_failure={_count(item, 'parse_failure_count')}; "
            f"runaway={_count(item, 'runaway_count')}; "
            f"delta_previous={deltas_previous.get(name, 'unknown')}; "
            f"delta_baseline={deltas_baseline.get(name, 'unknown')}"
        )
        for index, case in enumerate(item.get("bad_cases") or [], start=1):
            runaway = case["length_truncated"] or case["finish_reason"] not in {"", "stop"}
            response, omitted = _bounded_response(case["model_response"], runaway)
            case_id = f"{name}#{index}"
            evidence_ids.add(case_id)
            tokens = case["response_token_count"]
            lines.extend([
                f"Evidence {case_id}:",
                f"Dimension: {case['dimension'] or 'unspecified'}",
                f"Question: {_compact(case['question'], 1200)}",
                "Response metadata: "
                f"tokens={tokens if tokens is not None else 'unknown'}, "
                f"finish_reason={case['finish_reason'] or 'unknown'}, "
                f"length_truncated={str(case['length_truncated']).lower()}, "
                f"omitted_chars={omitted}",
                "Model response (bounded):",
                response,
                f"Parsed answer: {_compact(case['parsed_answer'], 800) or '[empty]'}",
                f"Correct answer: {_compact(case['correct_answer'], 800) or '[not provided]'}",
            ])
    example = sorted(evidence_ids)[:1]
    lines.extend([
        "",
        "Output only a JSON object, no markdown:",
        json.dumps({
            "summary": "cross-case conclusion aimed at the data pipeline",
            "findings": [{
                "category": "missing_context",
                "data_side": True,
                "confidence": 0.0,
                "evidence_ids": example,
                "diagnosis": "the reusable failure pattern and how the evidence shows it",
                "recommended_actions": ["a concrete pipeline change"],
            }],
            "non_data_causes": ["model or infrastructure causes that must not be fixed in the data"],
            "recommended_pipeline_changes": ["1-3 changes in priority order"],
        }, ensure_ascii=False),
        f"JSON field constraints: at most {max_findings} findings; at most {max_actions} "
        "recommended_actions per finding; confidence is 0-1; evidence_ids must cite the Evidence "
        "above; recommended_actions may only describe data pipeline actions. If the evidence is "
        "insufficient, return empty findings and say why.",
    ])
    return "\n".join(lines) + "\n", evidence_ids


def parse(raw: Any, evidence_ids: set[str], max_findings: int, max_actions: int) -> dict[str, Any]:
    """Validate the attributor's JSON the way the original agent does."""
    if not isinstance(raw, dict):
        raise ValueError("attribution response must be a JSON object")
    findings: list[dict[str, Any]] = []
    raw_findings = raw.get("findings", [])
    if isinstance(raw_findings, list):
        for item in raw_findings[:max_findings]:
            if not isinstance(item, dict):
                continue
            category = _compact(item.get("category"), 64).lower()
            diagnosis = _compact(item.get("diagnosis"), 900)
            actions = _clean_list(item.get("recommended_actions"), max_actions, 500)
            if not diagnosis and not actions:
                continue
            try:
                confidence = float(item.get("confidence", 0.0))
            except (TypeError, ValueError):
                confidence = 0.0
            if not math.isfinite(confidence):
                confidence = 0.0
            findings.append({
                "category": category if category in ALLOWED_CATEGORIES else "other",
                "data_side": bool(item.get("data_side", True)),
                "confidence": round(max(0.0, min(1.0, confidence)), 4),
                "evidence_ids": [e for e in _clean_list(item.get("evidence_ids"), 8, 80)
                                 if e in evidence_ids],
                "diagnosis": diagnosis,
                "recommended_actions": actions,
            })
    summary = _compact(raw.get("summary"), 1200)
    non_data = _clean_list(raw.get("non_data_causes"), 6, 500)
    if not summary and findings:
        summary = "; ".join(f["diagnosis"] for f in findings[:3] if f["diagnosis"])
    if not summary and not findings and not non_data:
        raise ValueError("attribution response contained no usable diagnosis")
    return {
        "summary": summary,
        "findings": findings,
        "non_data_causes": non_data,
        "recommended_pipeline_changes": _clean_list(
            raw.get("recommended_pipeline_changes"), max_actions, 600),
    }


def for_agent(report: dict[str, Any]) -> dict[str, Any]:
    """What the pipeline-writing agent sees: actions of non-data findings are withheld."""
    findings = []
    for item in report["findings"]:
        item = dict(item)
        if not item["data_side"]:
            item["recommended_actions"] = []
            item["note"] = "non-data cause; do not modify the pipeline for it"
        findings.append(item)
    return {
        "status": "ok",
        "summary": report["summary"],
        "findings": findings,
        "non_data_causes": report["non_data_causes"],
        "recommended_pipeline_changes": report["recommended_pipeline_changes"],
        "note": (
            "Evidence ids are opaque audit references, not data you can open. Improve the "
            "raw-data pipeline from cross-case patterns only; never hardcode a benchmark "
            "question or answer."
        ),
    }


def attribute(
    prompt: str,
    evidence_ids: set[str],
    *,
    api_url: str,
    api_key: str,
    model: str,
    max_findings: int,
    max_actions: int,
    max_retries: int = 2,
    timeout_sec: float = 600.0,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Call the attribution LLM until one reply parses; returns (report, attempts)."""
    attempts: list[dict[str, Any]] = []
    for number in range(1, max_retries + 2):
        record: dict[str, Any] = {"attempt": number, "status": "ok", "http_status": None,
                                  "finish_reason": None, "usage": None, "error": None,
                                  "raw_text": None}
        started = time.perf_counter()
        try:
            reply = _call_llm(api_url, api_key, model, prompt, timeout_sec)
        except urllib.error.HTTPError as exc:
            body = exc.read()[:2000].decode("utf-8", errors="replace")
            record.update(status="http_error", http_status=exc.code, error=f"HTTP {exc.code}: {body}")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            record.update(status="transport_error", error=f"{type(exc).__name__}: {exc}")
        else:
            record.update(http_status=reply["http_status"], finish_reason=reply["finish_reason"],
                          usage=reply["usage"], raw_text=reply["text"][:_RAW_TEXT_LIMIT])
            try:
                report = parse(_extract_json(reply["text"]), evidence_ids, max_findings, max_actions)
            except ValueError as exc:
                record.update(status="invalid", error=str(exc))
            else:
                record["latency_seconds"] = round(time.perf_counter() - started, 3)
                attempts.append(record)
                return report, attempts
        record["latency_seconds"] = round(time.perf_counter() - started, 3)
        attempts.append(record)
    return None, attempts
