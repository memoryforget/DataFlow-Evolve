"""Criteria-mode review, ported from DataFlow-Evolve's ReviewAgent (criteria mode only).

One LLM call judges a bounded random sample against every quality criterion
and returns evidence per criterion. The comparable score is the fraction of
criteria marked met. Deterministic full-file checks run without an LLM and act
as gates rather than score penalties, exactly as in the original criteria mode:

  dup_rate            max(whole-record dup rate, worst content-field dup rate)
  null_rate           empty required-field slots / (parseable rows x fields)
  parse_valid_rate    rows that parse as JSON objects / all rows
  contamination_rate  rows sharing a normalized n-gram with a reference set

passed = criteria score >= pass_rate, no critical failures, every criterion
assessed, null_rate <= max_null_rate, contamination_rate <= max_contam_rate,
and the file is non-empty.

A judge call that never yields a valid criteria object after its retries is a
review failure (status="error"), not a quality verdict: the executor neither
compares nor accepts it. Every attempt is recorded in the evidence file with
its raw reply, HTTP status, finish_reason and token usage.
"""
from __future__ import annotations

import hashlib
import json
import random
import re
import statistics
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .decontamination import build as build_contamination_index
from .telemetry.pipeline_usage import normalize_usage

_RAW_TEXT_LIMIT = 20000


@dataclass
class ReviewResult:
    review_score: float
    passed: bool
    issues: list[str] = field(default_factory=list)
    domain_feedback: dict[str, Any] = field(default_factory=dict)
    hard_metrics: dict[str, float] = field(default_factory=dict)
    status: str = "ok"                    # ok | error (judge never produced a verdict)
    llm: dict[str, Any] = field(default_factory=dict)


# ── sampling ──────────────────────────────────────────────────────────────────

def _sample_jsonl(path: str, n: int, seed: int | None) -> str:
    """Reservoir sample: one pass, O(n) memory, every non-empty line equally likely."""
    p = Path(path)
    if not p.exists():
        return ""
    rng = random.Random(seed)
    reservoir: list[str] = []
    seen = 0
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            seen += 1
            if len(reservoir) < n:
                reservoir.append(line)
            else:
                j = rng.randint(0, seen - 1)
                if j < n:
                    reservoir[j] = line
    return "\n".join(reservoir)


# ── deterministic full-file checks ────────────────────────────────────────────

# A field whose median length is below this is a label (an answer number, a
# difficulty level) and naturally repeats, so it is excluded from field dups.
CONTENT_FIELD_MIN_MEDIAN_LEN = 40
_LENGTH_SAMPLE_CAP = 4096


def _clip01(value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    return min(1.0, max(0.0, f))


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def _scan_validation_metrics(path: str, required_fields: list[str]) -> dict[str, Any]:
    """Whole-record hashes miss one question paired with differently worded
    answers; field hashes miss identical rows with distinct questions. The two
    do not cover each other, so dup_rate takes the larger."""
    total = parseable = null_slots = field_slots = 0
    hashes: set[str] = set()
    field_hashes: dict[str, set[str]] = {f: set() for f in required_fields}
    field_lengths: dict[str, list[int]] = {f: [] for f in required_fields}
    p = Path(path)
    if p.exists():
        with p.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                total += 1
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    obj = None
                if isinstance(obj, dict):
                    parseable += 1
                    canonical = json.dumps(obj, sort_keys=True, ensure_ascii=False)
                    hashes.add(hashlib.md5(canonical.encode("utf-8")).hexdigest())
                    for fld in required_fields:
                        field_slots += 1
                        value = obj.get(fld)
                        if _is_empty(value):
                            null_slots += 1
                        text = _as_text(value)
                        field_hashes[fld].add(hashlib.md5(text.encode("utf-8")).hexdigest())
                        if len(field_lengths[fld]) < _LENGTH_SAMPLE_CAP:
                            field_lengths[fld].append(len(text))
                else:
                    hashes.add(hashlib.md5(line.encode("utf-8")).hexdigest())
    if total == 0:
        return {"dup_rate": 0.0, "dup_field": "", "null_rate": 0.0,
                "parse_valid_rate": 0.0, "total": 0}
    record_dup = _clip01(1.0 - len(hashes) / total)
    field_dup, dup_field = 0.0, ""
    if parseable:
        for fld, fhashes in field_hashes.items():
            lengths = field_lengths[fld]
            if not lengths or statistics.median(lengths) < CONTENT_FIELD_MIN_MEDIAN_LEN:
                continue
            dup = 1.0 - len(fhashes) / parseable
            if dup > field_dup:
                field_dup, dup_field = dup, fld
    return {
        "dup_rate": max(record_dup, _clip01(field_dup)),
        "dup_field": dup_field if field_dup > record_dup else "",
        "null_rate": _clip01(null_slots / field_slots) if field_slots else 0.0,
        "parse_valid_rate": _clip01(parseable / total),
        "total": total,
    }


# ── LLM criteria judging ──────────────────────────────────────────────────────

CRITERIA_REVIEW_PROMPT = """You are a data quality reviewer. Judge the candidate data only against the task's quality criteria and the bounded random sample below.
Judge only what the sample shows; do not extrapolate the sample to frequencies over the whole dataset. Full-file structure, duplicates and contamination are checked separately by the harness.

Target task: {task_description}
Expected schema: {target_schema}
Quality criteria (assess each one, in order):
{quality_criteria}

Data sample (one JSON object per line):
{samples}

Output only this JSON object:
{{
  "criteria_assessments": [
    {{"criterion_index": 1, "met": true, "evidence": "concrete evidence from the sample", "suggestion": "task-level suggestion when improvement is needed"}}
  ],
  "critical_failures": ["errors with clear sample evidence that are serious enough to reject the candidate"]
}}
criteria_assessments must cover every criterion; criterion_index starts at 1. The harness scores the proportion of criteria with met=true.
met means the sample evidence supports the criterion. For a criterion that needs whole-dataset statistics, say "the sample cannot prove full coverage" in evidence instead of inventing a whole-dataset conclusion.
Use an empty array when there are no critical failures. Output nothing besides the JSON object."""


def _extract_json(text: str) -> Any | None:
    """First JSON object in free text: fenced block, whole text, or first balanced {...}."""
    if not text:
        return None
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    candidates = [fence.group(1).strip()] if fence else []
    candidates.append(text.strip())
    start = text.find("{")
    if start >= 0:
        depth, in_str, escape = 0, False, False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidates.append(text[start:i + 1])
                    break
    for cand in candidates:
        try:
            return json.loads(cand)
        except (json.JSONDecodeError, ValueError):
            continue
    return None


def _call_llm(api_url: str, api_key: str, model: str, prompt: str,
              timeout_sec: float) -> dict[str, Any]:
    """POST one chat completion to an OpenAI-compatible endpoint; raises on failure."""
    url = api_url.strip().rstrip("/")
    if url.endswith("/v1"):
        url += "/chat/completions"
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"},
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        status = resp.status
        data = json.loads(resp.read())
    if not isinstance(data, dict):
        raise ValueError("response body is not a JSON object")
    choice = (data.get("choices") or [{}])[0]
    content = (choice.get("message") or {}).get("content")
    if isinstance(content, list):  # some gateways return content parts
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return {
        "text": content if isinstance(content, str) else "",
        "finish_reason": choice.get("finish_reason"),
        "usage": normalize_usage(data.get("usage")),
        "model": data.get("model"),
        "http_status": status,
    }


def _parse_criteria(raw: Any, n_criteria: int, pass_rate: float) -> tuple[float, bool, dict, list[str]]:
    if not isinstance(raw, dict):
        raise ValueError("criteria review response must be a JSON object")
    assessments = raw.get("criteria_assessments")
    if not isinstance(assessments, list) or not assessments:
        raise ValueError("criteria_assessments must be a non-empty list")
    indices: set[int] = set()
    clean: list[dict[str, Any]] = []
    for item in assessments:
        if not isinstance(item, dict):
            raise ValueError("criteria_assessments entries must be objects")
        index, met = item.get("criterion_index"), item.get("met")
        if isinstance(index, bool) or not isinstance(index, int) or index < 1 or index in indices:
            raise ValueError("criterion_index must be a unique positive integer")
        if not isinstance(met, bool):
            raise ValueError("criteria_assessments.met must be boolean")
        indices.add(index)
        clean.append({
            "criterion_index": index,
            "met": met,
            "evidence": str(item.get("evidence") or "")[:1500],
            "suggestion": str(item.get("suggestion") or "")[:1000],
        })
    critical = raw.get("critical_failures", [])
    if not isinstance(critical, list) or not all(isinstance(c, str) for c in critical):
        raise ValueError("critical_failures must be a list of strings")
    failures = [c.strip()[:1000] for c in critical if c.strip()]
    score = sum(item["met"] for item in clean) / len(clean)
    passed = score >= pass_rate and not failures
    issues = list(failures)
    if indices != set(range(1, n_criteria + 1)):
        passed = False
        issues.append("the review LLM did not assess every quality criterion")
    feedback = {
        "review_mode": "criteria",
        "criteria_met_rate": score,
        "criteria_assessments": clean,
        "critical_failures": failures,
    }
    return score, passed, feedback, issues


class CriteriaReviewer:
    def __init__(
        self,
        api_url: str,
        api_key: str,
        model: str,
        sample_size: int = 80,
        sample_seed: int | None = 42,
        pass_rate: float = 0.6,
        max_null_rate: float = 0.5,
        max_retries: int = 2,
        request_timeout_sec: float = 600.0,
        decontam_files: list[str] | None = None,
        decontam_fields: list[str] | None = None,
        ngram_size: int = 13,
        max_contam_rate: float = 0.01,
    ) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.model = model
        self.sample_size = sample_size
        self.sample_seed = sample_seed
        self.pass_rate = _clip01(pass_rate)
        self.max_null_rate = _clip01(max_null_rate)
        self.max_retries = max(0, max_retries)
        self.request_timeout_sec = request_timeout_sec
        self.decontam_files = list(decontam_files or [])
        self.decontam_fields = decontam_fields or None
        self.ngram_size = ngram_size
        self.max_contam_rate = max_contam_rate

    def _judge(self, prompt: str, n_criteria: int) -> tuple[tuple | None, list[dict[str, Any]]]:
        """Call the review LLM until one reply is a valid criteria object.

        Returns the parsed verdict (or None if every attempt failed) and one
        record per attempt, so a failure can be diagnosed after the fact.
        """
        attempts: list[dict[str, Any]] = []
        for number in range(1, self.max_retries + 2):
            record: dict[str, Any] = {"attempt": number, "status": "ok", "http_status": None,
                                      "finish_reason": None, "usage": None, "error": None,
                                      "raw_text": None}
            started = time.perf_counter()
            try:
                reply = _call_llm(self.api_url, self.api_key, self.model, prompt,
                                  self.request_timeout_sec)
            except urllib.error.HTTPError as exc:
                body = exc.read()[:2000].decode("utf-8", errors="replace")
                record.update(status="http_error", http_status=exc.code,
                              error=f"HTTP {exc.code}: {body}")
            except (urllib.error.URLError, OSError, ValueError) as exc:
                record.update(status="transport_error", error=f"{type(exc).__name__}: {exc}")
            else:
                text = reply["text"]
                record.update(http_status=reply["http_status"],
                              finish_reason=reply["finish_reason"], usage=reply["usage"],
                              raw_text=text[:_RAW_TEXT_LIMIT])
                parsed = _extract_json(text)
                if not isinstance(parsed, dict):
                    record.update(status="no_json" if parsed is None else "not_object",
                                  error=f"no JSON object in the reply: {text[:200]!r}")
                else:
                    try:
                        verdict = _parse_criteria(parsed, n_criteria, self.pass_rate)
                    except ValueError as exc:
                        record.update(status="invalid_criteria", error=str(exc))
                    else:
                        record["latency_seconds"] = round(time.perf_counter() - started, 3)
                        attempts.append(record)
                        return verdict, attempts
            record["latency_seconds"] = round(time.perf_counter() - started, 3)
            attempts.append(record)
        return None, attempts

    def review(
        self,
        dataset_path: str,
        quality_criteria: list[str],
        target_schema: dict[str, str],
        task_description: str,
        evidence_path: str | None = None,
    ) -> ReviewResult:
        if not quality_criteria:
            return ReviewResult(0.0, False, ["criteria review needs at least one quality criterion"],
                                status="error")
        if not (self.api_url and self.model):
            return ReviewResult(0.0, False, ["review LLM is not configured (review_api_url / review_model)"],
                                status="error")

        samples_text = _sample_jsonl(dataset_path, self.sample_size, self.sample_seed)
        if not samples_text:
            return ReviewResult(0.0, False, ["dataset is empty or unreadable"])

        prompt = CRITERIA_REVIEW_PROMPT.format(
            task_description=task_description,
            target_schema=json.dumps(target_schema, ensure_ascii=False),
            quality_criteria="\n".join(f"{i}. {c}" for i, c in enumerate(quality_criteria, 1)),
            samples=samples_text,
        )
        verdict, attempts = self._judge(prompt, len(quality_criteria))
        status = "ok" if verdict is not None else "error"
        if verdict is None:
            last = attempts[-1]
            score, passed, feedback = 0.0, False, {"review_mode": "criteria"}
            issues = [
                f"review LLM produced no valid verdict after {len(attempts)} attempt(s); "
                f"last attempt {last['status']}: {last['error']}"
            ]
        else:
            score, passed, feedback, issues = verdict
        tokens = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        for attempt in attempts:
            for key in tokens:
                tokens[key] += int((attempt.get("usage") or {}).get(key) or 0)
        llm = {
            "attempts": len(attempts),
            "failed_attempts": sum(a["status"] != "ok" for a in attempts),
            "attempt_statuses": [a["status"] for a in attempts],
            "tokens": tokens,
        }

        # Deterministic gates. They block acceptance and explain why, but do not
        # rescale the criteria score (criteria mode in the original framework).
        required = list(target_schema)
        metrics = _scan_validation_metrics(dataset_path, required)
        hard: dict[str, Any] = {k: metrics[k] for k in ("dup_rate", "null_rate", "parse_valid_rate")}
        if metrics["dup_field"]:
            hard["dup_field"] = metrics["dup_field"]
        if metrics["total"] == 0:
            passed, score = False, 0.0
        if required and metrics["null_rate"] > self.max_null_rate:
            passed = False
            issues.append(
                f"output does not match the target schema: required fields {required} are empty in "
                f"{metrics['null_rate']:.1%} of slots (limit {self.max_null_rate:.1%})"
            )
        index = build_contamination_index(self.decontam_files, n=self.ngram_size,
                                          fields=self.decontam_fields)
        if index.missing:
            issues.append(f"decontamination reference files not found: {index.missing}")
        contam = index.scan(dataset_path, fields=self.decontam_fields)
        hard["contamination_rate"] = contam.rate
        if contam.hits and contam.rate > self.max_contam_rate:
            passed = False
            issues.append(
                f"{contam.rate:.1%} of rows overlap a reference benchmark (limit "
                f"{self.max_contam_rate:.1%}); this is test-set leakage. Matching snippets: "
                f"{'; '.join(contam.examples)}"
            )

        result = ReviewResult(
            review_score=_clip01(score),
            passed=passed,
            issues=issues,
            domain_feedback=feedback,
            hard_metrics=hard,
            status=status,
            llm=llm,
        )
        if evidence_path:
            record = {
                "dataset_path": dataset_path,
                "reviewed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "status": status,
                "prompt": prompt,
                "attempts": attempts,
                "result": {
                    "review_score": result.review_score,
                    "passed": result.passed,
                    "issues": result.issues,
                    "hard_metrics": result.hard_metrics,
                    "domain_feedback": result.domain_feedback,
                },
            }
            with open(evidence_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        return result
