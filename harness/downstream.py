"""Downstream evaluation jobs for the DataFlow-Evolve plugin.

A workspace has any number of feedback sources. Each is an eval script (the
user's own, or one the agent wrote with the user before the experiment) that
reports named scores: benchmark accuracy after SFT on GPUs, an LLM-API judge
scoring the data, anything else. Jobs run as detached processes and may run in
parallel; the harness does not limit how many. Where each job runs (which GPUs,
via the env passed at trigger time) is the agent's decision.

Script protocol:
  argv     <command> --request <job_dir>/request.json --result <job_dir>/result.json
           (or wherever the command places "{request}" / "{result}")
  request  source, stage ("baseline" | "periodic"), training_performed,
           dataset_path, the source's benchmarks and sampling, and the task.
           A periodic job evaluates the incumbent dataset. A baseline evaluates
           the starting point: the untrained model for a source that trains
           (dataset_path null), the raw input corpus for one that scores data.
  result   {"status": "ok"|"failed", "model", "benchmarks": {name: {"score",
           "metric", optional counts, optional "bad_cases": [...]}}, optional
           "summary" / "recommended_pipeline_changes" (the script's own
           analysis), "artifacts", "error"}

Every layer above the scores is optional. The harness computes score deltas
itself, per source, against the previous result and the baseline, and only
between results evaluated with the same settings. Raw bad cases are audit-only:
they stay in the job directory, and the agent sees at most the attribution
report built from them (harness/attribution.py).
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from . import attribution as attr
from .diagnostic import latest_review_record
from .workspace import DownstreamJob, FeedbackSource, Workspace, WorkspaceState

_COUNT_FIELDS = ("incorrect_count", "wrong_answer_count", "parse_failure_count", "runaway_count",
                 "correct_count", "total_count", "question_count", "n_sampling")
_SCRIPT_DELTA_FIELDS = ("score_deltas", "delta_baseline", "delta_previous")
_GUIDANCE = (
    "delta_previous is the change against this source's previous result and delta_baseline "
    "against its baseline; positive means better. Raw bad cases are audit-only and stay in "
    "the job directory for people: do not open result.json or attribution.json there. "
    "Improve the raw-data pipeline from cross-case patterns; never hardcode a benchmark "
    "question or answer."
)


def protocol_fingerprint(source: FeedbackSource) -> str:
    """Identity of the settings that make two results of a source comparable."""
    payload = {
        "command": list(source.command),
        "trains_model": source.trains_model,
        "benchmarks": list(source.benchmarks),
        "sampling_mode": source.sampling_mode,
        "subset_size": source.subset_size,
        "sampling_seed": source.sampling_seed,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _job_dir(workspace: Workspace, job_id: str) -> Path:
    return workspace.root / "downstream_jobs" / job_id


def _iteration_dir(state: WorkspaceState, iteration: int | None) -> Path | None:
    for it in state.iterations:
        if iteration is not None and it.iteration == iteration:
            return Path(it.directory)
    return None


def _argv(command: list[str], request: Path, result: Path) -> list[str]:
    if any("{request}" in part or "{result}" in part for part in command):
        return [part.replace("{request}", str(request)).replace("{result}", str(result))
                for part in command]
    return [*command, "--request", str(request), "--result", str(result)]


def unfinished_jobs(workspace: Workspace) -> list[DownstreamJob]:
    """Jobs still pending or running, after detecting dead or timed-out processes."""
    jobs = [refresh_job(workspace, job) for job in workspace.state().downstream_jobs]
    return [job for job in jobs if job.status in ("pending", "running")]


def missing_baselines(state: WorkspaceState) -> list[str]:
    """Sources that asked for a baseline and have no read baseline result yet."""
    done = {job.source for job in state.downstream_jobs
            if job.stage == "baseline" and job.status == "ready"}
    return [name for name, source in state.feedback_sources.items()
            if source.baseline and name not in done]


def trigger_downstream_eval(
    workspace: Workspace,
    source_name: str,
    *,
    stage: str = "periodic",
    env: dict[str, str] | None = None,
    secrets: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Start one async eval job of a source. Jobs of other sources, or of other
    stages, may already be running: the agent decides what runs in parallel."""
    state = workspace.state()
    source = state.feedback_sources.get(source_name)
    if source is None:
        return {"status": "error",
                "error": f"no feedback source {source_name!r}; configured: "
                         f"{sorted(state.feedback_sources) or 'none'}"}
    if stage not in ("baseline", "periodic"):
        return {"status": "error", "error": "stage must be 'baseline' or 'periodic'"}
    if stage == "periodic":
        dataset = state.incumbent_dataset_path
        if not dataset or not Path(dataset).exists():
            return {"status": "error",
                    "error": "no incumbent dataset yet; a periodic eval evaluates the incumbent"}
        iteration = state.incumbent_iteration
    else:
        dataset = None if source.trains_model else state.input_path
        iteration = None
    for job in unfinished_jobs(workspace):
        if (job.source, job.stage, job.iteration) == (source_name, stage, iteration):
            return {"status": "already_running", "job_id": job.job_id, "source": source_name,
                    "stage": stage, "env": job.env,
                    "message": "the same evaluation is already running; poll it instead"}

    job = workspace.register_downstream_job(
        source=source_name, stage=stage, iteration=iteration, dataset_path=dataset,
        checkpoint_iteration=state.next_iteration, protocol=protocol_fingerprint(source),
        env=dict(env or {}),
    )
    job_dir = _job_dir(workspace, job.job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    iteration_dir = _iteration_dir(state, iteration)
    request = {
        "source": source_name,
        "stage": stage,
        "training_performed": stage == "periodic" and source.trains_model,
        "checkpoint_iteration": state.next_iteration,
        "incumbent_iteration": iteration,
        "dataset_path": dataset,
        "pipeline_path": str(iteration_dir / "pipeline.py") if iteration_dir else None,
        "expected_benchmarks": source.benchmarks,
        "bad_cases_per_benchmark": source.bad_cases_per_benchmark,
        "sampling_mode": source.sampling_mode,
        "subset_size": source.subset_size,
        "sampling_seed": source.sampling_seed,
        "task": {
            "task_description": state.task_description,
            "quality_criteria": state.quality_criteria,
            "target_schema": state.target_schema,
            "input_contract": state.input_contract,
            "output_artifact_fields": state.output_artifact_fields,
            "media_policy": state.media_policy,
            "media_base_dir": str(Path(dataset).resolve().parent) if dataset else None,
        },
    }
    request_path, result_path = job_dir / "request.json", job_dir / "result.json"
    request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")

    # Keys reach the process environment only; the recorded env holds placement.
    process_env = {**os.environ, **(secrets or {}), **job.env}
    # Detached, so a long SFT run outlives this tool call.
    try:
        with open(job_dir / "stdout.log", "w") as out, open(job_dir / "stderr.log", "w") as err:
            proc = subprocess.Popen(
                _argv(source.command, request_path, result_path),
                cwd=str(workspace.root), env=process_env, stdout=out, stderr=err,
                start_new_session=True,
            )
    except OSError as exc:
        workspace.update_downstream_job(
            job.job_id, status="failed", error=str(exc), finished_at=time.time()
        )
        return {"job_id": job.job_id, "status": "failed", "source": source_name,
                "stage": stage, "error": str(exc)}
    workspace.update_downstream_job(job.job_id, status="running", pid=proc.pid)
    return {
        "job_id": job.job_id,
        "status": "running",
        "source": source_name,
        "stage": stage,
        "env": job.env,
        "timeout_sec": source.timeout_sec,
        "instruction": (
            "Poll rsi_get_downstream_result (without job_id it polls every unfinished job) "
            "until each is ready or failed. rsi_submit_pipeline refuses new pipelines while "
            "any job is unfinished."
        ),
    }


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # A finished child stays a zombie until reaped; reap it so it reads as dead.
    try:
        reaped, _ = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return True
    return reaped == 0


def refresh_job(workspace: Workspace, job: DownstreamJob) -> DownstreamJob:
    """Mark a running job failed if its process died without a result, or if it
    exceeded its source's user-set timeout. No timeout applies when none was set."""
    if job.status not in ("pending", "running"):
        return job
    job_dir = _job_dir(workspace, job.job_id)
    if (job_dir / "result.json").exists():
        return job
    source = workspace.state().feedback_sources.get(job.source)
    timeout = source.timeout_sec if source else None
    if timeout and time.time() - job.started_at > timeout and _pid_alive(job.pid):
        try:
            os.killpg(job.pid, signal.SIGTERM)
        except OSError:
            pass
        return workspace.update_downstream_job(
            job.job_id, status="failed", finished_at=time.time(),
            error=f"timed out after {timeout}s; see {job_dir / 'stderr.log'}",
        ) or job
    if job.pid and not _pid_alive(job.pid):
        return workspace.update_downstream_job(
            job.job_id, status="failed", finished_at=time.time(),
            error=f"eval process exited without writing result.json; see {job_dir / 'stderr.log'}",
        ) or job
    return job


# ── result validation ─────────────────────────────────────────────────────────

def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _bad_case(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("not a JSON object")
    for key in ("question", "model_response"):
        if not isinstance(raw.get(key), str) or not raw[key].strip():
            raise ValueError(f"{key} must be a non-empty string")
    tokens = raw.get("response_token_count")
    if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
        tokens = None
    return {
        "question": raw["question"],
        "model_response": raw["model_response"],
        "parsed_answer": _text(raw.get("parsed_answer")),
        "correct_answer": _text(raw.get("correct_answer")),   # optional: judged evals have none
        "response_token_count": tokens,
        "finish_reason": _text(raw.get("finish_reason")),
        "length_truncated": raw.get("length_truncated") is True,
        "dimension": _text(raw.get("dimension")),
    }


def validate_result(data: Any, expected: list[str], max_cases: int
                    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Scores are required; counts and bad cases are optional. A malformed bad
    case is skipped with a warning instead of failing the whole evaluation."""
    if not isinstance(data, dict):
        raise ValueError("result.json root must be a JSON object")
    raw = data.get("benchmarks")
    if not isinstance(raw, dict) or not raw:
        raise ValueError("benchmarks must be a non-empty JSON object")
    missing = [name for name in expected if name not in raw]
    if missing:
        raise ValueError(f"missing benchmark results: {', '.join(missing)}")
    warnings = [f"benchmark {name!r} is not configured for this source and was ignored"
                for name in raw if name not in expected]
    benchmarks: dict[str, dict[str, Any]] = {}
    for name in expected:
        item = raw[name]
        if not isinstance(item, dict):
            raise ValueError(f"benchmark {name!r} must be a JSON object")
        score = item.get("score")
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
            raise ValueError(f"benchmark {name!r} score must be a finite number")
        entry: dict[str, Any] = {"score": float(score), "metric": _text(item.get("metric")) or "score"}
        for key in _COUNT_FIELDS:
            value = item.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                entry[key] = value
        raw_cases = item.get("bad_cases")
        if raw_cases is not None and not isinstance(raw_cases, list):
            warnings.append(f"{name}.bad_cases is not a list and was ignored")
            raw_cases = []
        cases: list[dict[str, Any]] = []
        for index, raw_case in enumerate(raw_cases or []):
            try:
                cases.append(_bad_case(raw_case))
            except ValueError as exc:
                warnings.append(f"{name}.bad_cases[{index}] skipped: {exc}")
        if len(cases) > max_cases:
            warnings.append(f"{name}: {len(cases) - max_cases} bad case(s) beyond "
                            f"bad_cases_per_benchmark={max_cases} ignored")
            cases = cases[:max_cases]
        entry["bad_cases"] = cases
        benchmarks[name] = entry
    return benchmarks, warnings


# ── feedback ──────────────────────────────────────────────────────────────────

def _deltas(state: WorkspaceState, job: DownstreamJob, scores: dict[str, float]
            ) -> tuple[dict[str, float], dict[str, float], dict[str, str]]:
    """Deltas against this source's previous read result and its baseline, both
    started before this job, and only when evaluated with the same settings."""
    earlier: list[DownstreamJob] = []
    for other in state.downstream_jobs:
        if other.job_id == job.job_id:
            break
        if other.source == job.source and other.status == "ready":
            earlier.append(other)

    def against(other: DownstreamJob | None, absent: str) -> tuple[dict[str, float], str]:
        if other is None:
            return {}, absent
        if other.protocol != job.protocol:
            return {}, (f"not comparable with job {other.job_id}: this source's script, "
                        "benchmarks or sampling changed since")
        previous = other.result["scores"]
        return ({name: round(score - previous[name], 6)
                 for name, score in scores.items() if name in previous},
                f"vs job {other.job_id} ({other.stage}, incumbent {other.iteration})")

    delta_previous, note_previous = against(earlier[-1] if earlier else None,
                                            "no earlier result of this source")
    if job.stage == "baseline":
        delta_baseline, note_baseline = {}, "this is the baseline"
    else:
        baselines = [other for other in earlier if other.stage == "baseline"]
        delta_baseline, note_baseline = against(baselines[-1] if baselines else None,
                                                "no baseline result of this source")
    return delta_previous, delta_baseline, {"delta_previous": note_previous,
                                            "delta_baseline": note_baseline}


def _attribution(workspace: Workspace, job: DownstreamJob, source: FeedbackSource,
                 data: dict[str, Any], benchmarks: dict[str, dict[str, Any]],
                 deltas: tuple, api_key: str) -> dict[str, Any]:
    state = workspace.state()
    cases = sum(len(item["bad_cases"]) for item in benchmarks.values())
    if not cases:
        return {"status": "no_bad_cases",
                "note": "the eval script returned no bad cases; scores and deltas are the feedback"}
    if not (source.attribution and state.attribution_api_url and state.attribution_model):
        return {"status": "not_configured",
                "note": (f"the eval script returned {cases} bad case(s); they are kept for audit "
                         "only. Attribution for this source (rsi_configure_feedback) would turn "
                         "them into a data-side diagnosis.")}
    # The iteration that was evaluated, not whatever is incumbent at poll time
    iteration_dir = _iteration_dir(state, job.iteration)
    operators: list[str] = []
    review_record = None
    if iteration_dir is not None:
        try:
            decision = json.loads((iteration_dir / "decision.json").read_text(encoding="utf-8"))
            operators = [str(op["name"]) for op in decision["ops"][:12]]
        except (OSError, ValueError, KeyError, TypeError):
            operators = []
        review_record = latest_review_record(iteration_dir)
    prompt, evidence_ids = attr.build_prompt(
        task_description=state.task_description,
        target_schema=state.target_schema,
        quality_criteria=state.quality_criteria,
        operators=operators,
        review_record=review_record,
        checkpoint={"stage": job.stage, "checkpoint_iteration": job.checkpoint_iteration,
                    "incumbent_iteration": job.iteration, "model": data.get("model")},
        benchmarks=benchmarks,
        deltas_previous=deltas[0],
        deltas_baseline=deltas[1],
        max_findings=state.attribution_max_findings,
        max_actions=state.attribution_max_actions,
    )
    report, attempts = attr.attribute(
        prompt, evidence_ids,
        api_url=state.attribution_api_url, api_key=api_key, model=state.attribution_model,
        max_findings=state.attribution_max_findings, max_actions=state.attribution_max_actions,
    )
    evidence_path = _job_dir(workspace, job.job_id) / "attribution.json"
    evidence_path.write_text(json.dumps({
        "job_id": job.job_id, "source": job.source, "stage": job.stage, "prompt": prompt,
        "attempts": attempts, "result": report,
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tokens = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    for attempt in attempts:
        for key in tokens:
            tokens[key] += int((attempt.get("usage") or {}).get(key) or 0)
    llm = {"attempts": len(attempts),
           "failed_attempts": sum(a["status"] != "ok" for a in attempts), "tokens": tokens}
    if report is None:
        last = attempts[-1]
        return {"status": "failed", "llm": llm,
                "error": (f"the attribution LLM produced no usable diagnosis after "
                          f"{len(attempts)} attempt(s); last attempt {last['status']}: "
                          f"{last['error']}"),
                "note": "an infrastructure failure, not a data signal"}
    return {**attr.for_agent(report), "llm": llm}


def _feedback(workspace: Workspace, job: DownstreamJob, data: dict[str, Any], api_key: str
              ) -> dict[str, Any]:
    state = workspace.state()
    source = state.feedback_sources[job.source]
    benchmarks, warnings = validate_result(data, source.benchmarks,
                                           source.bad_cases_per_benchmark)
    scores = {name: item["score"] for name, item in benchmarks.items()}
    delta_previous, delta_baseline, comparability = _deltas(state, job, scores)
    try:
        attribution = _attribution(workspace, job, source, data, benchmarks,
                                   (delta_previous, delta_baseline), api_key)
    except Exception as exc:  # noqa: BLE001 - attribution must never lose the scores
        attribution = {"status": "failed", "error": f"attribution error: {exc}",
                       "note": "an infrastructure failure, not a data signal"}
    ignored = [key for key in _SCRIPT_DELTA_FIELDS if key in data]
    if ignored:
        warnings.append(f"{', '.join(ignored)} from the script ignored: the harness computes "
                        "deltas itself")
    feedback: dict[str, Any] = {
        "job_id": job.job_id,
        "status": "ready",
        "source": job.source,
        "stage": job.stage,
        "checkpoint_iteration": job.checkpoint_iteration,
        "incumbent_iteration": job.iteration,
        "model": _text(data.get("model")),
        "scores": scores,
        "benchmarks": {
            name: {**{k: v for k, v in item.items() if k != "bad_cases"},
                   "bad_case_count": len(item["bad_cases"])}
            for name, item in benchmarks.items()
        },
        "delta_previous": delta_previous,
        "delta_baseline": delta_baseline,
        "comparability": comparability,
        "attribution": attribution,
        "warnings": warnings,
        "guidance": _GUIDANCE,
    }
    changes = data.get("recommended_pipeline_changes")
    analysis = {
        "summary": _text(data.get("summary")),
        "recommended_pipeline_changes": (
            [_text(item) for item in changes if _text(item)] if isinstance(changes, list) else []
        ),
    }
    if analysis["summary"] or analysis["recommended_pipeline_changes"]:
        feedback["script_analysis"] = analysis
    artifacts = data.get("artifacts")
    if isinstance(artifacts, dict):
        feedback["artifacts"] = {str(k): str(v) for k, v in artifacts.items()}
    return feedback


def _failure(job: DownstreamJob, error: str | None) -> dict[str, Any]:
    if job.stage == "baseline":
        instruction = (
            f"The baseline of source {job.source!r} failed. This is an infrastructure problem, "
            "not a data signal. Tell the user; they decide whether to retry "
            f"(rsi_trigger_downstream_eval with source={job.source!r}, stage='baseline') or go "
            f"on without it (rsi_configure_feedback with source={job.source!r}, baseline=false)."
        )
    else:
        instruction = (
            f"The evaluation of source {job.source!r} failed. This is an infrastructure "
            "problem, not a data signal, so do not change the pipeline because of it. Tell "
            "the user; they decide whether to retry."
        )
    return {"job_id": job.job_id, "status": "failed", "source": job.source, "stage": job.stage,
            "error": error, "instruction": instruction}


def get_downstream_result(workspace: Workspace, job_id: str, *,
                          attribution_api_key: str = "") -> dict[str, Any]:
    """Poll a job. The first poll after result.json appears validates it, computes
    deltas, runs attribution if configured, and stores the feedback; later polls
    return the stored feedback without calling the attribution model again."""
    job = workspace.get_downstream_job(job_id)
    if not job:
        return {"status": "error", "error": f"job {job_id} not found"}
    job = refresh_job(workspace, job)
    if job.status == "failed":
        return _failure(job, job.error)
    if job.status == "ready":
        return job.result

    job_dir = _job_dir(workspace, job_id)
    result_path = job_dir / "result.json"
    if not result_path.exists():
        return {"job_id": job_id, "status": job.status, "source": job.source,
                "stage": job.stage, "env": job.env,
                "elapsed_sec": round(time.time() - job.started_at, 1)}

    try:
        data = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        error = f"result.json parse error: {exc}"
    else:
        if not isinstance(data, dict) or data.get("status") != "ok":
            error = (data.get("error") if isinstance(data, dict) else None) or "unknown error"
        else:
            try:
                feedback = _feedback(workspace, job, data, attribution_api_key)
            except ValueError as exc:
                error = f"invalid result.json: {exc}"
            else:
                (job_dir / "feedback.json").write_text(
                    json.dumps(feedback, ensure_ascii=False, indent=2), encoding="utf-8")
                workspace.update_downstream_job(job_id, status="ready", result=feedback,
                                                finished_at=time.time())
                return feedback
    job = workspace.update_downstream_job(job_id, status="failed", error=error,
                                          finished_at=time.time()) or job
    return _failure(job, error)


def poll_downstream(workspace: Workspace, *, attribution_api_key: str = "") -> dict[str, Any]:
    """Poll every unfinished job at once; finished ones return their feedback."""
    pending = [job.job_id for job in workspace.state().downstream_jobs
               if job.status in ("pending", "running")]
    jobs = [get_downstream_result(workspace, job_id, attribution_api_key=attribution_api_key)
            for job_id in pending]
    unfinished = [job["job_id"] for job in jobs if job["status"] in ("pending", "running")]
    return {
        "status": "running" if unfinished else "done",
        "unfinished": unfinished,
        "jobs": jobs,
        "missing_baselines": missing_baselines(workspace.state()),
        "note": (None if pending else
                 "no unfinished jobs; read an earlier result with its job_id"),
    }
