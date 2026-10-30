"""Pipeline execution harness for DataFlow-Evolve plugin.

After the user's agent writes pipeline.py + operators/ + decision.json into
iteration_dir, this module handles:
  1. Artifact validation (file existence, forbidden imports)
  2. Compile check   (DF_COMPILE_ONLY=1 subprocess)
  3. Execution       (full subprocess run)
  4. Criteria review (harness/reviewer.py)
  5. Incumbent comparison  ((passed, score) rule from original controller)

Every HTTP call the pipeline makes to an LLM is recorded, without prompts or
outputs, in {iteration_dir}/sessions/pipeline/pipeline_llm_calls.jsonl by the
telemetry bootstrap, and summarized in pipeline_llm_token_usage.json.

A review that never produces a verdict (status "error") is not a quality
result: the candidate is not compared, not accepted, and the iteration does
not advance.

Repair loop (execution_repairs):
  When execution fails, the harness returns structured repair feedback (traceback,
  diagnostics, step-level artifacts) so the user's agent can fix pipeline.py and
  operators/ in-place and resubmit the same iteration_dir. The iteration counter
  is NOT advanced until the pipeline succeeds or the repair budget is exhausted.
  This mirrors the original framework's self_correct loop in controller.py.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dataflow_evolve.models import PipelineConfig
from dataflow_evolve.step_cache import (
    MANIFEST_FILENAME,
    StepReusePlan,
    prepare_step_reuse,
    write_step_manifest,
)

from .execution_observation import write_execution_observation
from .reviewer import CriteriaReviewer, ReviewResult
from .telemetry.pipeline_usage import (
    ATTEMPT_ENV,
    BOOTSTRAP_DIR,
    EXECUTION_ID_ENV,
    ITERATION_ENV,
    USAGE_PATH_ENV,
    compact_usage,
    write_pipeline_usage_summary,
)
from .runtime_environment import pipeline_pythonpath
from .workspace import IterationSummary, Workspace

_REPO_ROOT = Path(__file__).resolve().parents[1]


# ── result contracts ──────────────────────────────────────────────────────────

@dataclass
class ExecutionResult:
    success: bool
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0
    wall_time_seconds: float = 0.0
    error: str | None = None
    stage: str = "execution"  # artifact_validation | compile | execution | output_validation


@dataclass
class RepairFeedback:
    """Structured feedback for the agent to self-correct a failed pipeline.

    Mirrors the information supplied to build_selfcorrect_prompt() in the
    original framework (prompts.py). The agent should:
      1. Read traceback and diagnostics
      2. Read the log paths if more context is needed
      3. Fix pipeline.py / operators/ in the SAME iteration_dir
      4. Re-call rsi_submit_pipeline with the same iteration_dir
    """
    failure_stage: str          # artifact_validation | compile | execution | output_validation
    traceback: str              # bounded error text (<=32k chars)
    diagnostics: str            # step-level artifact diagnostics (<=64k chars)
    iteration_dir: str
    repairs_used: int
    repairs_remaining: int
    log_paths: dict[str, str] = field(default_factory=dict)


@dataclass
class SubmitResult:
    review_score: float | None
    passed: bool
    issues: list[str]
    domain_feedback: dict[str, Any]
    execution_error: str | None
    execution_observation: str | None
    failure_stage: str | None
    incumbent_updated: bool
    incumbent_score: float | None
    next_iteration_dir: str
    iteration: int
    # Populated when execution fails AND repairs remain
    repair_feedback: RepairFeedback | None = None
    # Calls, failures and tokens of the pipeline's own LLM calls this iteration
    pipeline_llm_usage: dict[str, Any] | None = None
    # Review judge attempts, failed attempts and tokens
    review_llm: dict[str, Any] | None = None
    # How many leading operators were reused from the incumbent's step cache
    step_reuse: dict[str, Any] | None = None
    # Final dataset of this iteration, once execution produced one
    dataset_path: str | None = None


# ── validation ────────────────────────────────────────────────────────────────

_FORBIDDEN_IMPORTS = {"dataflow"}
_REQUIRED_ENTRY = "pipeline.py"
_REQUIRED_DECISION = "decision.json"


def _check_artifacts(iteration_dir: Path) -> str | None:
    """Return an error string if required files are missing or invalid."""
    pipeline_path = iteration_dir / _REQUIRED_ENTRY
    if not pipeline_path.exists():
        return f"pipeline.py not found in {iteration_dir}"
    decision_path = iteration_dir / _REQUIRED_DECISION
    if not decision_path.exists():
        return f"decision.json not found in {iteration_dir}"
    try:
        tree = ast.parse(pipeline_path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return f"pipeline.py syntax error: {exc}"
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [
                (a.name if isinstance(node, ast.Import) else (node.module or ""))
                for a in getattr(node, "names", [])
            ]
            for name in names:
                top = name.split(".")[0] if name else ""
                if top in _FORBIDDEN_IMPORTS:
                    return f"pipeline.py imports forbidden package '{top}' (not installed)"
    try:
        json.loads(decision_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return f"decision.json is not valid JSON: {exc}"
    return None


def _run_subprocess(
    iteration_dir: Path,
    env: dict[str, str],
    timeout_sec: int | None,
    label: str,
) -> ExecutionResult:
    pipeline_path = iteration_dir / _REQUIRED_ENTRY
    cmd = [sys.executable, str(pipeline_path)]
    start = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(iteration_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        wall = time.perf_counter() - start
        return ExecutionResult(
            success=proc.returncode == 0,
            stdout=proc.stdout[-32768:] if proc.stdout else "",
            stderr=proc.stderr[-32768:] if proc.stderr else "",
            returncode=proc.returncode,
            wall_time_seconds=wall,
            error=None if proc.returncode == 0 else f"{label} failed (exit {proc.returncode})",
            stage=label,
        )
    except subprocess.TimeoutExpired as exc:
        return ExecutionResult(
            success=False,
            wall_time_seconds=time.perf_counter() - start,
            error=f"{label} timed out after {timeout_sec}s",
            stage=label,
        )
    except Exception as exc:
        return ExecutionResult(
            success=False,
            wall_time_seconds=time.perf_counter() - start,
            error=f"{label} subprocess error: {exc}",
            stage=label,
        )


_STEP_RE = re.compile(r"_step(\d+)\.jsonl$", re.IGNORECASE)
_OPS_KEYS = ("ops", "operators", "operator_list", "pipeline_ops")
_PURPOSE_KEYS = ("purpose", "description", "desc", "role")
_RATIONALE_KEYS = ("reason", "rationale", "design_rationale", "reasoning")
_FLOW_KEYS = ("field_flow", "fields_flow", "flow")


def _step_number(path: Path) -> int:
    match = _STEP_RE.search(path.name)
    return int(match.group(1)) if match else -1


def _fresh_step_files(cache: Path, since: float) -> list[Path]:
    """Step files written by the run that started at ``since``, in step order."""
    if not cache.is_dir():
        return []
    fresh = []
    for path in cache.glob("pipeline_step_step*.jsonl"):
        try:
            if path.is_file() and path.stat().st_mtime >= since - 1.0:
                fresh.append(path)
        except OSError:
            continue
    return sorted(fresh, key=_step_number)


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _first_present(d: dict[str, Any], keys: tuple[str, ...], default: Any = "") -> Any:
    """First non-empty value; decision.json is LLM-written, so key names drift."""
    for key in keys:
        value = d.get(key)
        if value:
            return value
    return default


def _load_config(iter_dir: Path) -> PipelineConfig:
    """decision.json + pipeline.py as a PipelineConfig, with the original parser's key aliases."""
    decision = _read_json_object(iter_dir / _REQUIRED_DECISION) or {}
    raw_ops = _first_present(decision, _OPS_KEYS, default=[])
    if isinstance(raw_ops, dict):
        raw_ops = [{"name": k, **(v if isinstance(v, dict) else {})} for k, v in raw_ops.items()]
    operators: list[dict[str, Any]] = []
    if isinstance(raw_ops, (list, tuple)):
        for op in raw_ops:
            if isinstance(op, str):
                operators.append({"name": op, "custom": True, "purpose": "", "params": {}})
            elif isinstance(op, dict):
                operators.append({
                    "name": _first_present(op, ("name", "operator", "class"), "?"),
                    "custom": op.get("custom", True),
                    "purpose": _first_present(op, _PURPOSE_KEYS),
                    "params": op.get("params", {}),
                })
    try:
        code = (iter_dir / _REQUIRED_ENTRY).read_text(encoding="utf-8")
    except OSError:
        code = ""
    return PipelineConfig(
        operators=operators,
        field_flow=str(_first_present(decision, _FLOW_KEYS)),
        code=code,
        rationale=str(_first_present(decision, _RATIONALE_KEYS)),
    )


def _incumbent_dir(state: Any, iter_dir: Path) -> Path | None:
    """Directory of the current incumbent: the only parent whose steps may be reused."""
    if state.incumbent_iteration is None:
        return None
    for it in state.iterations:
        if it.iteration == state.incumbent_iteration:
            parent = Path(it.directory).resolve()
            return None if parent == iter_dir else parent
    return None


def _reuse_summary(plan: StepReusePlan | None) -> dict[str, Any] | None:
    if plan is None:
        return None
    return {
        "reused_operators": plan.prefix_count,
        "total_operators": len(plan.operator_names),
        "reason": plan.reason,
    }


def _find_final_dataset(iteration_dir: Path) -> str | None:
    """Return path to the last pipeline_step_*.jsonl in cache/."""
    manifest = _read_json_object(iteration_dir / MANIFEST_FILENAME)
    final = (manifest or {}).get("final_dataset_path")
    if final and Path(final).is_file():
        return str(final)
    cache = iteration_dir / "cache"
    if not cache.is_dir():
        return None
    steps = sorted(cache.glob("pipeline_step_step*.jsonl"), key=_step_number)
    return str(steps[-1]) if steps else None


def _source_fingerprint(iter_dir: Path) -> str:
    """Hash of the files that define a pipeline. Recorded when its dataset is
    generated, so a later review can tell whether the dataset still matches."""
    digest = hashlib.sha256()
    files = [iter_dir / _REQUIRED_ENTRY, iter_dir / _REQUIRED_DECISION,
             *sorted((iter_dir / "operators").rglob("*.py"))]
    for path in files:
        if path.is_file():
            digest.update(path.relative_to(iter_dir).as_posix().encode("utf-8") + b"\0")
            digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()


def _run_review(
    state: Any,
    dataset_path: str,
    iter_dir: Path,
    *,
    api_url: str,
    api_key: str,
    model: str,
    sample_size: int,
    sample_seed: int,
    pass_rate: float,
    decontam_files: list[str] | None,
    ngram_size: int,
    max_contam_rate: float,
) -> ReviewResult:
    reviewer = CriteriaReviewer(
        api_url=api_url,
        api_key=api_key,
        model=model,
        sample_size=sample_size,
        sample_seed=sample_seed,
        pass_rate=pass_rate,
        decontam_files=decontam_files or [],
        ngram_size=ngram_size,
        max_contam_rate=max_contam_rate,
    )
    return reviewer.review(
        dataset_path=dataset_path,
        quality_criteria=state.quality_criteria,
        target_schema=state.target_schema,
        task_description=state.task_description,
        evidence_path=str(iter_dir / "review.jsonl"),
    )


_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_MAX_TRACEBACK_CHARS = 32 * 1024
_MAX_DIAGNOSTICS_CHARS = 64 * 1024


def _bounded(text: str | None, limit: int) -> str:
    """Mirror the original framework's _bounded_text helper in execution_wrapper.py."""
    s = _ANSI_RE.sub("", str(text or ""))
    if len(s) <= limit:
        return s
    marker = f"\n...[truncated to {limit} chars]...\n"
    budget = max(0, limit - len(marker))
    head = budget // 3
    return s[:head] + marker + s[-(budget - head):]


def _build_step_diagnostics(iter_dir: Path, exec_result: ExecutionResult) -> str:
    """Build a step-level diagnostics string from cache artifacts.

    Mirrors the diagnostic section supplied to build_selfcorrect_prompt() in
    the original framework (prompts.py / execution_wrapper.py):
    - Lists cache steps found (or missing)
    - Reports row counts where readable
    - Tails stderr for progress noise removal
    """
    lines: list[str] = []

    cache = iter_dir / "cache"
    if cache.is_dir():
        steps = sorted(cache.glob("pipeline_step_step*.jsonl"))
        if steps:
            lines.append("=== Cache steps produced ===")
            for p in steps:
                try:
                    row_count = sum(1 for ln in p.open(encoding="utf-8") if ln.strip())
                    lines.append(f"  {p.name}: {row_count} rows")
                except OSError:
                    lines.append(f"  {p.name}: (unreadable)")
        else:
            lines.append("=== Cache steps produced: none ===")
    else:
        lines.append("=== cache/ directory not found ===")

    # Stderr tail (strip progress bars)
    stderr = exec_result.stderr or ""
    stderr_clean = "\n".join(
        ln for ln in stderr.splitlines()
        if not re.search(r"(?:\d{1,3}%\||it/s|Generating responses)", ln)
    )
    if stderr_clean.strip():
        lines.append("\n=== stderr (tail) ===")
        lines.append(stderr_clean[-8192:])

    return "\n".join(lines)


def _repair_feedback(
    iter_dir: Path,
    stage: str,
    exec_result: ExecutionResult,
    repairs_used: int,
    execution_repairs: int,
) -> RepairFeedback:
    """Build structured repair feedback mirroring build_selfcorrect_prompt."""
    traceback_text = _bounded(
        exec_result.stderr or exec_result.error or "", _MAX_TRACEBACK_CHARS
    )
    diagnostics_text = _bounded(
        _build_step_diagnostics(iter_dir, exec_result), _MAX_DIAGNOSTICS_CHARS
    )
    # Collect log paths (attempt_*.log files)
    log_paths: dict[str, str] = {}
    for log_file in sorted(iter_dir.glob("attempt_*.log")):
        log_paths[log_file.name] = str(log_file)

    return RepairFeedback(
        failure_stage=stage,
        traceback=traceback_text,
        diagnostics=diagnostics_text,
        iteration_dir=str(iter_dir),
        repairs_used=repairs_used,
        repairs_remaining=max(0, execution_repairs - repairs_used),
        log_paths=log_paths,
    )


# ── incumbent comparison (same rule as controller._is_better) ─────────────────

def _is_better(challenger_score: float, challenger_passed: bool,
               incumbent_score: float | None, incumbent_passed: bool | None) -> bool:
    """Rank by (passed, score); a tie goes to the newer challenger. With no
    incumbent yet, any reviewed candidate is accepted, so a failing first
    candidate still gives the agent a parent to improve on."""
    if incumbent_score is None:
        return True
    return (challenger_passed, challenger_score) >= (bool(incumbent_passed), incumbent_score)


# ── main entry point ──────────────────────────────────────────────────────────

def submit_pipeline(
    workspace: Workspace,
    iteration_dir: str | Path,
    *,
    compile_timeout_sec: int = 120,
    execution_timeout_sec: int | None = None,
    execution_repairs: int = 2,          # repair budget (default matches framework)
    entry_path: str | None = None,
    cache_dir: str | None = None,
    env_extra: dict[str, str] | None = None,
    review_api_url: str = "",
    review_api_key: str = "",
    review_model: str = "",
    review_sample_size: int = 80,
    review_sample_seed: int = 42,
    review_pass_rate: float = 0.6,
    review_decontam_files: list[str] | None = None,
    review_ngram_size: int = 13,
    review_max_contam_rate: float = 0.01,
) -> SubmitResult:
    iter_dir = Path(iteration_dir).resolve()
    state = workspace.state()
    execution_repairs = max(0, execution_repairs)

    # Find existing summary for this directory to track repairs_used
    existing = next(
        (it for it in state.iterations
         if Path(it.directory).resolve() == iter_dir),
        None,
    )
    repairs_used = existing.repairs_used if existing else 0

    # Resolve entry path and cache dir
    resolved_entry = entry_path or state.input_path
    resolved_cache = cache_dir or str(iter_dir / "cache")

    # Build env for subprocess. PYTHONPATH carries the repo root, so generated
    # pipelines import the bundled dataflow_evolve runtime without a pip install,
    # and the telemetry bootstrap, so every LLM call is recorded.
    calls_path = iter_dir / "sessions" / "pipeline" / "pipeline_llm_calls.jsonl"
    usage_summary_path = iter_dir / "pipeline_llm_token_usage.json"
    base_env = dict(os.environ)
    base_env.update({
        "ENTRY_PATH": resolved_entry,
        "CACHE_DIR": resolved_cache,
        "ITERATION_DIR": str(iter_dir),
        "PYTHONPATH": pipeline_pythonpath(),
        USAGE_PATH_ENV: str(calls_path),
        EXECUTION_ID_ENV: uuid.uuid4().hex,
        ITERATION_ENV: iter_dir.name,
        ATTEMPT_ENV: str(repairs_used + 1),
    })
    # Compile-only is harness-internal; never let the caller's shell turn the
    # formal run into a second preflight.
    base_env.pop("DF_COMPILE_ONLY", None)
    if env_extra:
        base_env.update(env_extra)

    reuse_plan: StepReusePlan | None = None  # resolved once the artifacts validate

    def _pipeline_usage() -> dict[str, Any] | None:
        """Summarize this iteration's pipeline LLM calls, repair attempts included.
        Accounting never changes the outcome, so a write failure returns None."""
        try:
            summary = write_pipeline_usage_summary(calls_path, usage_summary_path)
        except (OSError, ValueError):
            return None
        return compact_usage(summary, calls_path)

    # ── helper: return failure (with or without repair feedback) ─────────────

    def _fail(
        stage: str,
        error: str,
        obs: str | None,
        exec_res: ExecutionResult | None = None,
        budget_exhausted: bool = False,
        usage: dict[str, Any] | None = None,
    ) -> SubmitResult:
        """Record failure; advance iteration only when budget is exhausted."""
        nonlocal repairs_used
        summary = IterationSummary(
            iteration=state.next_iteration,
            directory=str(iter_dir),
            execution_error=error,
            execution_observation_path=obs,
            repairs_used=repairs_used,
            pipeline_llm_usage=usage,
        )
        workspace.record_iteration(summary)

        repair_fb: RepairFeedback | None = None
        if exec_res is not None and not budget_exhausted:
            repair_fb = _repair_feedback(
                iter_dir, stage, exec_res, repairs_used, execution_repairs
            )

        if budget_exhausted or exec_res is None:
            # Budget exhausted or non-repairable stage → advance to next iter
            ws_state = workspace.state()
            ws_state.next_iteration += 1
            workspace.save(ws_state)
        else:
            # Repair still possible → stay on current iteration_dir
            ws_state = workspace.state()

        _, next_dir = workspace.next_iteration_dir()
        return SubmitResult(
            review_score=None,
            passed=False,
            issues=[error],
            domain_feedback={},
            execution_error=error,
            execution_observation=obs,
            failure_stage=stage,
            incumbent_updated=False,
            incumbent_score=ws_state.incumbent_score,
            # If repair remains: stay on same dir; else advance
            next_iteration_dir=(
                str(iter_dir) if repair_fb is not None else str(next_dir)
            ),
            iteration=state.next_iteration,
            repair_feedback=repair_fb,
            step_reuse=_reuse_summary(reuse_plan),
            pipeline_llm_usage=usage,
        )

    # ── 1. Artifact validation (not repairable via retry; always advance) ────

    artifact_error = _check_artifacts(iter_dir)
    if artifact_error:
        return _fail("artifact_validation", artifact_error, None,
                     exec_res=None, budget_exhausted=True)

    # ── 2. Compile check ─────────────────────────────────────────────────────

    # Step reuse is decided here, by the framework, from the incumbent's step
    # manifest; the pipeline only consumes the plan through the environment.
    config = _load_config(iter_dir)
    try:
        reuse_plan = prepare_step_reuse(
            config, iter_dir,
            parent_iteration_dir=_incumbent_dir(state, iter_dir),
            raw_entry_path=resolved_entry,
            pipeline_filename=_REQUIRED_ENTRY,
        )
    except (OSError, ValueError) as exc:
        # Reuse is an optimization: any doubt means a full run from the raw input.
        raw = str(Path(resolved_entry).resolve())
        reuse_plan = StepReusePlan(
            [str(op.get("name", "?")) for op in config.operators], 0, raw, raw,
            reason=f"reuse plan unavailable ({exc}); full run from raw input",
        )
    base_env.update(reuse_plan.env())

    compile_env = {**base_env, "DF_COMPILE_ONLY": "1"}
    compile_result = _run_subprocess(iter_dir, compile_env, compile_timeout_sec, "compile")
    if not compile_result.success:
        obs = _write_observation(iter_dir, compile_result, stage="compile",
                                 attempt=repairs_used)
        budget_exhausted = repairs_used >= execution_repairs
        repairs_used += 1
        return _fail("compile", compile_result.error or "compile failed",
                     obs, exec_res=compile_result,
                     budget_exhausted=budget_exhausted, usage=_pipeline_usage())

    # ── 3. Execute ───────────────────────────────────────────────────────────

    exec_started = time.time()
    exec_result = _run_subprocess(iter_dir, base_env, execution_timeout_sec, "execution")
    obs = _write_observation(iter_dir, exec_result, stage="execution",
                             attempt=repairs_used)
    usage = _pipeline_usage()
    if not exec_result.success:
        budget_exhausted = repairs_used >= execution_repairs
        repairs_used += 1
        return _fail("execution", exec_result.error or "execution failed",
                     obs, exec_res=exec_result,
                     budget_exhausted=budget_exhausted, usage=usage)

    # ── 4. Step manifest and final dataset ───────────────────────────────────────────────

    fresh_steps = _fresh_step_files(Path(resolved_cache), since=exec_started)
    dataset_path = None
    failure = None
    if reuse_plan.fully_reused:
        dataset_path = reuse_plan.entry_path
    elif fresh_steps and fresh_steps[-1].stat().st_size > 0:
        dataset_path = str(fresh_steps[-1])
    if dataset_path and config.operators:
        try:
            write_step_manifest(
                config, iter_dir,
                raw_entry_path=resolved_entry,
                pipeline_filename=_REQUIRED_ENTRY,
                plan=reuse_plan,
                executed_artifacts=fresh_steps,
                final_dataset_path=dataset_path,
            )
        except (OSError, ValueError) as exc:
            failure = f"step manifest validation failed: {exc}"
    if dataset_path and not failure:
        try:
            obs = str(write_execution_observation(
                iter_dir, config=config, raw_entry_path=resolved_entry,
                final_dataset_path=dataset_path, status="ok"))
        except (OSError, ValueError, TypeError):
            pass  # evidence for the agent, never a reason to fail the run
    if not dataset_path or failure:
        err = failure or "No pipeline_step_*.jsonl found in cache/ after execution"
        budget_exhausted = repairs_used >= execution_repairs
        repairs_used += 1
        return _fail("output_validation", err, obs,
                     exec_res=exec_result,
                     budget_exhausted=budget_exhausted, usage=usage)

    # ── 5. Criteria review ────────────────────────────────────────────────────

    fingerprint = _source_fingerprint(iter_dir)
    review = _run_review(
        state, dataset_path, iter_dir,
        api_url=review_api_url, api_key=review_api_key, model=review_model,
        sample_size=review_sample_size, sample_seed=review_sample_seed,
        pass_rate=review_pass_rate, decontam_files=review_decontam_files,
        ngram_size=review_ngram_size, max_contam_rate=review_max_contam_rate,
    )

    # ── 6. Review failure: no verdict, so nothing to compare or accept ────────

    if review.status == "error":
        workspace.record_iteration(IterationSummary(
            iteration=state.next_iteration, directory=str(iter_dir),
            review_status="error", issues=review.issues,
            execution_observation_path=obs, repairs_used=repairs_used,
            dataset_path=dataset_path, pipeline_llm_usage=usage,
            source_fingerprint=fingerprint,
        ))
        return SubmitResult(
            review_score=None,
            passed=False,
            issues=review.issues,
            domain_feedback={},
            execution_error=None,
            execution_observation=obs,
            failure_stage="review",
            incumbent_updated=False,
            incumbent_score=state.incumbent_score,
            next_iteration_dir=str(iter_dir),
            iteration=state.next_iteration,
            pipeline_llm_usage=usage,
            review_llm=review.llm,
            step_reuse=_reuse_summary(reuse_plan),
            dataset_path=dataset_path,
        )

    # ── 7. Incumbent comparison ───────────────────────────────────────────────

    cur_iter = state.next_iteration
    updated = _is_better(
        review.review_score, review.passed,
        state.incumbent_score, state.incumbent_passed,
    )
    if updated:
        workspace.update_incumbent(cur_iter, review.review_score, review.passed, dataset_path)

    summary = IterationSummary(
        iteration=cur_iter, directory=str(iter_dir),
        review_score=review.review_score, passed=review.passed,
        accepted_as_incumbent=updated,
        issues=review.issues,
        execution_observation_path=obs,
        repairs_used=repairs_used,
        review_status="ok",
        dataset_path=dataset_path,
        pipeline_llm_usage=usage,
        source_fingerprint=fingerprint,
    )
    workspace.record_iteration(summary)
    workspace.advance_iteration()
    _, next_dir = workspace.next_iteration_dir()

    return SubmitResult(
        review_score=review.review_score,
        passed=review.passed,
        issues=review.issues,
        domain_feedback=review.domain_feedback,
        execution_error=None,
        execution_observation=obs,
        failure_stage=None,
        incumbent_updated=updated,
        incumbent_score=workspace.state().incumbent_score,
        next_iteration_dir=str(next_dir),
        iteration=cur_iter,
        repair_feedback=None,
        step_reuse=_reuse_summary(reuse_plan),
        pipeline_llm_usage=usage,
        review_llm=review.llm,
        dataset_path=dataset_path,
    )


def _recompute_incumbent(workspace: Workspace) -> None:
    """Make the best reviewed iteration the incumbent: (passed, score), ties to
    the later iteration. That is what accepting challengers with >= one at a
    time produces, so a re-scored past iteration lands where it would have."""
    best: tuple[tuple, IterationSummary, str] | None = None
    for it in workspace.state().iterations:
        if it.review_status != "ok" or it.review_score is None:
            continue
        dataset = it.dataset_path
        if not dataset or not Path(dataset).exists():
            continue
        key = (bool(it.passed), it.review_score, it.iteration)
        if best is None or key > best[0]:
            best = (key, it, dataset)
    if best is not None:
        _, it, dataset = best
        workspace.update_incumbent(it.iteration, it.review_score, bool(it.passed), dataset)


def review_iteration(
    workspace: Workspace,
    iteration_dir: str | Path,
    *,
    review_api_url: str = "",
    review_api_key: str = "",
    review_model: str = "",
    review_sample_size: int = 80,
    review_sample_seed: int = 42,
    review_pass_rate: float = 0.6,
    review_decontam_files: list[str] | None = None,
    review_ngram_size: int = 13,
    review_max_contam_rate: float = 0.01,
) -> dict[str, Any]:
    """Review an already generated dataset again without re-running its pipeline.

    For an iteration whose review produced no verdict, a verdict completes it
    exactly as a successful submit would (compared, maybe accepted, iteration
    advanced). For a completed iteration the new verdict replaces the old one
    and the incumbent is recomputed. A failed re-review changes nothing.
    """
    iter_dir = Path(iteration_dir).resolve()
    state = workspace.state()
    summary = next(
        (it for it in state.iterations if Path(it.directory).resolve() == iter_dir), None
    )
    if summary is None:
        return {"status": "error",
                "error": "this iteration was never submitted; use rsi_submit_pipeline"}
    dataset_path = summary.dataset_path
    if summary.execution_error or not dataset_path or not Path(dataset_path).exists():
        return {"status": "error",
                "error": "this iteration has no generated dataset (its pipeline did not "
                         "execute successfully); fix it and resubmit with rsi_submit_pipeline"}
    if summary.source_fingerprint is None or _source_fingerprint(iter_dir) != summary.source_fingerprint:
        return {"status": "error",
                "error": "pipeline.py, decision.json or operators/ changed after this dataset "
                         "was generated, so the dataset no longer matches the recorded "
                         "pipeline; resubmit with rsi_submit_pipeline to regenerate it"}

    review = _run_review(
        state, dataset_path, iter_dir,
        api_url=review_api_url, api_key=review_api_key, model=review_model,
        sample_size=review_sample_size, sample_seed=review_sample_seed,
        pass_rate=review_pass_rate, decontam_files=review_decontam_files,
        ngram_size=review_ngram_size, max_contam_rate=review_max_contam_rate,
    )
    previous = {"review_status": summary.review_status,
                "review_score": summary.review_score, "passed": summary.passed}
    pending = summary.iteration == state.next_iteration
    result: dict[str, Any] = {
        "iteration": summary.iteration,
        "previous": previous,
        "review_llm": review.llm,
        "evidence": str(iter_dir / "review.jsonl"),
    }
    if review.status == "error":
        _, next_dir = workspace.next_iteration_dir()
        result.update(status="review_error", issues=review.issues,
                      next_iteration_dir=str(iter_dir) if pending else str(next_dir))
        return result

    old_incumbent = state.incumbent_iteration
    summary.review_score = review.review_score
    summary.passed = review.passed
    summary.issues = review.issues
    summary.review_status = "ok"
    summary.dataset_path = dataset_path
    workspace.record_iteration(summary)
    if pending:
        workspace.advance_iteration()
    _recompute_incumbent(workspace)
    new_state = workspace.state()
    _, next_dir = workspace.next_iteration_dir()
    result.update(
        status="ok",
        review_score=review.review_score,
        passed=review.passed,
        issues=review.issues,
        domain_feedback=review.domain_feedback,
        incumbent_iteration=new_state.incumbent_iteration,
        incumbent_score=new_state.incumbent_score,
        incumbent_changed=new_state.incumbent_iteration != old_incumbent,
        next_iteration_dir=str(next_dir),
    )
    return result


def _write_observation(
    iter_dir: Path, result: ExecutionResult, stage: str, attempt: int = 0
) -> str | None:
    obs = {
        "stage": stage,
        "attempt": attempt,
        "success": result.success,
        "returncode": result.returncode,
        "wall_time_seconds": result.wall_time_seconds,
        "stdout_tail": result.stdout[-4096:],
        "stderr_tail": result.stderr[-4096:],
        "error": result.error,
    }
    # Name includes attempt so retries don't overwrite the first failure
    fname = f"execution_tail_attempt{attempt:02d}.json" if attempt > 0 else "execution_tail.json"
    path = iter_dir / fname
    try:
        path.write_text(json.dumps(obs, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)
    except OSError:
        return None
