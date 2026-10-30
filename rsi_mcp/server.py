"""MCP server for DataFlow-Evolve plugin.

Exposes the RSI harness as MCP tools so the user's Claude Code or Codex
session can orchestrate data RSI through natural language.

Tools (stdio transport):
  rsi_init                    — initialise workspace from natural-language task
  rsi_add_domain_skill        — register a domain skill written by the host agent
  rsi_configure_feedback      — configure one of many named downstream feedback sources
  rsi_generate_eval_script    — write a protocol skeleton for the host agent to complete
  rsi_submit_pipeline         — compile + execute + review a pipeline iteration
  rsi_review_iteration        — review an executed iteration again, without re-running it
  rsi_diagnose_pipeline       — the read-only mechanism diagnosis prompt
  rsi_trigger_downstream_eval — start one async source job; jobs may run in parallel
  rsi_get_downstream_result   — read one job or poll all unfinished jobs
  rsi_get_status              — workspace / incumbent summary
  rsi_export_dataset          — copy incumbent dataset to a specified path
  rsi_set_credential_env      — point a workspace at other key env vars / credentials file
"""
from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import asdict
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

# Allow `python mcp/server.py` (script mode) and `python -m rsi_mcp.server`
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mcp.server.mcpserver import MCPServer
from mcp_types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field

from harness.runtime_environment import runtime_contract, check_environment
from dataflow_evolve.core.contracts import InputContract
from dataflow_evolve.media import MediaPolicy, validate_media_manifest, export_media_dataset
from harness.workspace import Workspace, WorkspaceState, FeedbackSource, init_workspace
from harness.executor import review_iteration as _review_iteration
from harness.executor import submit_pipeline as _submit_pipeline
from harness.diagnostic import build_diagnostic_context
from harness.history import evolution_history, write_history_snapshot
from harness.downstream import (
    get_downstream_result, poll_downstream, protocol_fingerprint,
    unfinished_jobs, missing_baselines, trigger_downstream_eval,
)
from harness.scaffold import (
    check_name, SKILL_GUIDE, register_domain_skill, scaffold_eval_script,
)


mcp = MCPServer("dataflow_evolve_mcp")

# These locks live in worker threads: cancelling an MCP request must not let
# another submit/review mutate the workspace while its worker is still running.
_iteration_locks: dict[str, Any] = {}
_iteration_locks_guard = threading.Lock()


def _run_iteration_operation(operation: Callable[..., str], params: Any) -> str:
    key = str(Path(params.workspace_dir).resolve())
    with _iteration_locks_guard:
        lock = _iteration_locks.setdefault(key, threading.Lock())
    if not lock.acquire(blocking=False):
        return json.dumps({
            "status": "workspace_busy",
            "instruction": "A submit or review is still running in this workspace. "
                           "Wait for it to finish before retrying; cancellation "
                           "does not terminate the running pipeline or review.",
        })
    try:
        return operation(params)
    finally:
        lock.release()


# ── required fields for init ──────────────────────────────────────────────────

_REQUIRED_FIELDS = {
    "task_description": "What is the goal of the dataset? (e.g. 'math tutoring SFT data')",
    "input_path": "Path to the raw input corpus JSONL file.",
    "review_api_url": "OpenAI-compatible API URL for the review/pipeline LLM (e.g. http://host:3000/v1/chat/completions).",
    "review_model": "Model name to use for criteria review.",
    "pipeline_api_url": "OpenAI-compatible API URL for the pipeline generation LLM.",
    "pipeline_model": "Model name for pipeline generation.",
    "workspace_dir": "Directory where the RSI workspace will be created.",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_workspace(workspace_dir: str) -> Workspace:
    ws = Workspace(workspace_dir)
    ws.load()
    return ws


def _read_credentials_file(path: str) -> dict[str, str]:
    """Parse a KEY=value file. Re-read on every call so edits apply without restart."""
    values: dict[str, str] = {}
    try:
        text = Path(path).expanduser().read_text(encoding="utf-8")
    except OSError:
        return values
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip('"').strip("'")
    return values


def _api_key(state: WorkspaceState, env_name: str) -> str:
    """Resolve a key by name: credentials file first, then the server process env."""
    if state.credentials_file:
        value = _read_credentials_file(state.credentials_file).get(env_name)
        if value:
            return value
    return os.environ.get(env_name, "")


def _chat_completions_url(url: str) -> str:
    """Accept an OpenAI base URL and return the endpoint the serving posts to."""
    url = url.strip().rstrip("/")
    return url + "/chat/completions" if url.endswith("/v1") else url


def _pipeline_llm_contract(state: WorkspaceState) -> dict[str, Any]:
    """What a generated pipeline needs to construct PipelineLLMServing."""
    return {
        "api_url": _chat_completions_url(state.pipeline_api_url),
        "model_name": state.pipeline_model,
        "key_name_of_api_key": state.pipeline_api_key_env,
        "max_workers": state.pipeline_max_workers,
        "note": (
            "Construct PipelineLLMServing in the Pipeline with exactly these values and inject "
            "it into LLM operators. The harness sets the key env var for the pipeline "
            "process; never write the key into pipeline.py."
        ),
    }


def _missing(state_kwargs: dict) -> list[dict]:
    return [
        {"field": k, "question": q}
        for k, q in _REQUIRED_FIELDS.items()
        if not state_kwargs.get(k)
    ]


# ── tools ─────────────────────────────────────────────────────────────────────

class RsiCheckEnvironmentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    modules: list[str] = Field(default_factory=lambda: ["dataflow_evolve"], max_length=20, description="Python module names to import in the pipeline execution environment.")
    timeout_sec: int = Field(default=30, ge=1, le=60, description="Per-module import timeout.")


@mcp.tool(name="rsi_check_environment",
          annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
async def rsi_check_environment(params: RsiCheckEnvironmentInput) -> str:
    """Check imports with the pipeline interpreter and PYTHONPATH, without installing packages."""
    result = await asyncio.to_thread(check_environment, params.modules, params.timeout_sec)
    return json.dumps(result, ensure_ascii=False)


class RsiInitInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_description: str = Field(default="", description="Natural-language goal of the dataset.")
    input_path: str = Field(default="", description="Path to raw input corpus JSONL.")
    review_api_url: str = Field(default="", description="API URL for review LLM.")
    review_api_key_env: str = Field(
        default="RSI_REVIEW_API_KEY",
        description="Name of the env var holding the review LLM API key. The key value itself is never stored.",
    )
    review_model: str = Field(default="", description="Review model name.")
    pipeline_api_url: str = Field(default="", description="API URL for pipeline LLM.")
    pipeline_api_key_env: str = Field(
        default="RSI_PIPELINE_API_KEY",
        description="Name of the env var holding the pipeline LLM API key. The key value itself is never stored.",
    )
    credentials_file: str = Field(
        default="",
        description=(
            "Optional path to a KEY=value file (chmod 600, outside the workspace). "
            "Re-read on every call, so editing it switches keys without restarting. "
            "Takes priority over the server process env."
        ),
    )
    pipeline_model: str = Field(default="", description="Pipeline model name.")
    pipeline_max_workers: int = Field(default=25, ge=1, description="LLM concurrent workers.")
    quality_criteria: list[str] = Field(default_factory=list, description="Quality criteria strings.")
    target_schema: dict[str, str] = Field(
        default_factory=lambda: {"messages": "list[dict]"},
        description="Target schema keys and types.",
    )
    input_contract: dict[str, Any] = Field(default_factory=lambda: {"modalities": ["text"]}, description="Input modalities, required_fields and artifact_fields mapping field names to image/video_reference. References may be lists.")
    output_artifact_fields: dict[str, str] = Field(default_factory=dict, description="Required output media fields and their artifact kinds; retain evidence for visual review.")
    media_policy: dict[str, Any] = Field(default_factory=dict, description="MediaPolicy settings: review_sample_size, video_frames, max_edge, max_images, max_payload_bytes, max_source_bytes, decode_timeout_sec.")
    provider: str = Field(default="claude", description="Agent provider: claude|codex|opencode.")
    workspace_dir: str = Field(default="", description="Workspace directory path.")


@mcp.tool(
    name="rsi_init",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False),
)
async def rsi_init(params: RsiInitInput) -> str:
    """Initialise a new RSI workspace.

    If any required field is missing, returns missing_fields and questions so
    the agent can ask the user. On success, returns the workspace directory and
    the first iteration_dir for the agent to write pipeline.py into.
    """
    kwargs = params.model_dump()
    missing = _missing(kwargs)
    if not kwargs["quality_criteria"]:
        missing.append({
            "field": "quality_criteria",
            "question": (
                "What makes a record good for this dataset? Give 3-6 checkable criteria; "
                "criteria review scores the fraction of them a sample meets."
            ),
        })
    if missing:
        return json.dumps({"status": "needs_input", "missing_fields": missing}, ensure_ascii=False)

    if not Path(kwargs["input_path"]).exists():
        return json.dumps({"status": "error", "error": f"input_path does not exist: {kwargs['input_path']}"})

    try:
        contract = InputContract.from_mapping(kwargs["input_contract"])
        policy = MediaPolicy.from_mapping(kwargs["media_policy"])
        InputContract.from_mapping({"modalities": ["text", "image", "video"], "artifact_fields": kwargs["output_artifact_fields"]})
        await asyncio.to_thread(validate_media_manifest, kwargs["input_path"], contract=contract, policy=policy)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return json.dumps({"status": "error", "error": f"input media validation failed: {exc}"})

    ws = init_workspace(
        kwargs["workspace_dir"],
        task_description=kwargs["task_description"],
        input_path=kwargs["input_path"],
        review_api_url=kwargs["review_api_url"],
        review_api_key_env=kwargs["review_api_key_env"],
        review_model=kwargs["review_model"],
        pipeline_api_url=kwargs["pipeline_api_url"],
        pipeline_api_key_env=kwargs["pipeline_api_key_env"],
        credentials_file=kwargs["credentials_file"],
        pipeline_model=kwargs["pipeline_model"],
        pipeline_max_workers=kwargs["pipeline_max_workers"],
        quality_criteria=kwargs["quality_criteria"],
        target_schema=kwargs["target_schema"],
        input_contract=contract.to_dict(),
        output_artifact_fields=kwargs["output_artifact_fields"],
        media_policy=policy.to_dict(),
        provider=kwargs["provider"],
    )
    _, first_iter_dir = ws.next_iteration_dir()
    first_iter_dir.mkdir(parents=True, exist_ok=True)

    return json.dumps({
        "status": "ok",
        "workspace_dir": str(ws.root),
        "first_iteration_dir": str(first_iter_dir),
        "runtime": runtime_contract(),
        "pipeline_llm": _pipeline_llm_contract(ws.state()),
        "input_contract": ws.state().input_contract,
        "output_artifact_fields": ws.state().output_artifact_fields,
        "media_policy": ws.state().media_policy,
        "instructions": (
            "Write pipeline.py, operators/, and decision.json into first_iteration_dir, "
            "then call rsi_submit_pipeline with that directory."
        ),
    }, ensure_ascii=False)


class RsiAddDomainSkillInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str
    name: str
    description: str
    body: str = Field(default="", description="Skill body written by the host agent. Omit to get writing guidance without creating a file.")


@mcp.tool(name="rsi_add_domain_skill",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_add_domain_skill(params: RsiAddDomainSkillInput) -> str:
    """Register a domain skill written by you, the host agent. Never calls an LLM.

    Without body, returns writing guidance. With body, writes SKILL.md with
    name/description frontmatter and registers it in the workspace.
    """
    ws = _load_workspace(params.workspace_dir)
    problem = check_name(params.name)
    if problem:
        return json.dumps({"status": "error", "error": problem})
    if not params.body.strip():
        return json.dumps({"status": "needs_content", "guide": SKILL_GUIDE,
                           "instruction": "Write the skill yourself, then call again with body."})
    return json.dumps(register_domain_skill(ws, params.name, params.description, params.body),
                      ensure_ascii=False)


class RsiConfigureFeedbackInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str
    source: str = Field(..., description="Unique feedback-source name, e.g. llm-quality or sft-math. Call once per source; updates affect only that source.")
    command: list[str] | None = Field(default=None, description="Script argv. Appends --request and --result, or substitutes {request}/{result}.")
    trains_model: bool | None = Field(default=None, description="Whether periodic runs train a model before scoring. GPU use is independent: a no-training evaluator may still need GPUs.")
    benchmarks: list[str] | None = Field(default=None, description="Names of scores reported by this source: benchmarks or custom metrics.")
    sampling_mode: str | None = None
    subset_size: int | None = Field(default=None, gt=0)
    sampling_seed: int | None = None
    baseline: bool | None = None
    attribution: bool | None = None
    resources: str | None = Field(default=None, description="Resources per job, e.g. '2 GPUs, 40GB each' or 'API only, 8 workers'. Agent plans parallelism; harness imposes no global cap.")
    api_key_envs: list[str] | None = Field(default=None, description="Credential env-var names injected into this source's subprocess, never values.")
    timeout_sec: int | None = Field(default=None, ge=0, description="0 disables timeout. Omitted keeps the source's setting.")
    bad_cases_per_benchmark: int | None = Field(default=None, ge=0)
    attribution_api_url: str | None = None
    attribution_model: str | None = None
    attribution_api_key_env: str | None = None
    remove: bool = False
    confirm: bool = False


def _command_problems(command: list[str], root: Path) -> list[str]:
    script = next((part for part in command if part.endswith((".py", ".sh"))), None)
    program = command[0] if command else ""
    target = script or (program if "/" in program else None)
    if target:
        path = Path(target).expanduser()
        if not (path if path.is_absolute() else root / path).is_file():
            return [f"script/program not found: {target}"]
    return []


@mcp.tool(name="rsi_configure_feedback",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_configure_feedback(params: RsiConfigureFeedbackInput) -> str:
    """Configure one of any number of named feedback sources. No LLM calls.

    Each source owns a script and named scores, sampling, baseline and optional
    attribution. It can call an API, run on CPUs/GPUs, or combine them. The host
    agent writes the script with the user or uses their existing script.
    Preview first; confirm only after user approval. Omitted update fields keep
    their saved values. Sources may run concurrently, placed by the agent.
    """
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    problem = check_name(params.source)
    if problem:
        return json.dumps({"status": "error", "error": problem})
    if any(j.source == params.source for j in unfinished_jobs(ws)):
        return json.dumps({"status": "error", "error": "Read all active jobs of this source before changing its configuration."})
    old = state.feedback_sources.get(params.source)
    if params.remove:
        if not params.confirm:
            return json.dumps({"status": "needs_confirmation", "remove": params.source})
        state.feedback_sources.pop(params.source, None)
        ws.save(state)
        return json.dumps({"status": "ok", "removed": params.source})
    supplied = params.model_dump(exclude_none=True, exclude_unset=True)
    values = asdict(old) if old else {"name": params.source}
    for key in FeedbackSource.__dataclass_fields__:
        if key in supplied and key != "name":
            values[key] = supplied[key]
    questions = []
    for key, question in {
        "command": "Use your existing script or let the host agent write one with you before the experiment? Supply its argv.",
        "trains_model": "Does this source train on the incumbent dataset before evaluating, or score data/model without training?",
        "benchmarks": "Which named metrics should this source report? Custom metrics and benchmarks are both supported.",
        "sampling_mode": "Evaluate full data or a fixed subset? For subset supply subset_size and sampling_seed.",
        "baseline": "Evaluate the starting point once before iteration 0? Training sources evaluate the untrained base model; data-scoring sources evaluate the raw corpus.",
        "attribution": "Enable optional bad-case attribution for this source? Scores and script analysis work without it.",
        "resources": "What does one run need: API calls, CPUs, GPU count/memory? The agent will choose which jobs can overlap.",
    }.items():
        if key not in values or (key in ("command", "benchmarks", "sampling_mode") and not values[key]):
            questions.append({"field": key, "question": question})
    if values.get("sampling_mode") == "subset":
        for key in ("subset_size", "sampling_seed"):
            if values.get(key) is None:
                questions.append({"field": key, "question": f"Choose {key} to keep the evaluated subset fixed."})
    url = params.attribution_api_url if params.attribution_api_url is not None else state.attribution_api_url
    model = params.attribution_model if params.attribution_model is not None else state.attribution_model
    key_env = params.attribution_api_key_env if params.attribution_api_key_env is not None else state.attribution_api_key_env
    if values.get("attribution") and not (url and model and key_env):
        questions.append({"field": "attribution_config", "question": "Supply attribution_api_url, attribution_model and attribution_api_key_env for the independent attribution LLM."})
    if questions:
        return json.dumps({"status": "needs_input", "source": params.source, "questions": questions}, ensure_ascii=False)
    if values["sampling_mode"] not in ("full", "subset"):
        return json.dumps({"status": "error", "error": "sampling_mode must be full or subset"})
    if len(set(values["benchmarks"])) != len(values["benchmarks"]) or not all(values["benchmarks"]):
        return json.dumps({"status": "error", "error": "score names must be non-empty and unique"})
    problems = _command_problems(values["command"], ws.root)
    if problems:
        return json.dumps({"status": "error", "error": "; ".join(problems)})
    if values["sampling_mode"] == "full":
        values["subset_size"] = None
    if values.get("timeout_sec") == 0:
        values["timeout_sec"] = None
    scripts = ws.eval_scripts_dir().resolve()
    values["script_origin"] = "user"
    for part in values["command"]:
        path = Path(part)
        path = path if path.is_absolute() else ws.root / path
        if path.resolve().is_relative_to(scripts):
            values["script_origin"] = "workspace"
            break
    source = FeedbackSource(**values)
    warnings = []
    if old and protocol_fingerprint(old) != protocol_fingerprint(source):
        warnings.append("This source's evaluation settings changed; deltas against earlier settings are not comparable.")
    preview = {"source": asdict(source), "attribution_model": model,
               "attribution_api_url": url, "attribution_api_key_env": key_env}
    if not params.confirm:
        return json.dumps({"status": "needs_confirmation", "config_preview": preview,
                           "warnings": warnings}, ensure_ascii=False)
    state.feedback_sources[params.source] = source
    state.attribution_api_url, state.attribution_model = url, model
    state.attribution_api_key_env = key_env
    ws.save(state)
    return json.dumps({"status": "ok", "config": preview, "warnings": warnings,
                       "missing_baselines": missing_baselines(state)}, ensure_ascii=False)


class RsiGenerateEvalScriptInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str
    name: str
    description: str
    trains_model: bool
    overwrite: bool = False


@mcp.tool(name="rsi_generate_eval_script",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_generate_eval_script(params: RsiGenerateEvalScriptInput) -> str:
    """Create an eval-script skeleton and return the protocol. Never calls an LLM.

    You, the host agent, implement the TODOs using the conversation with the
    user, inspect/test the script and obtain approval before registering it.
    User-supplied scripts do not need this tool.
    """
    ws = _load_workspace(params.workspace_dir)
    problem = check_name(params.name)
    if problem:
        return json.dumps({"status": "error", "error": problem})
    result = scaffold_eval_script(ws, params.name, params.description,
                                  params.trains_model, params.overwrite)
    result["instruction"] = "Implement this script yourself from the user conversation. No LLM was called. Test it and get user approval before registering a feedback source."
    return json.dumps(result, ensure_ascii=False)


class RsiSubmitPipelineInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str = Field(..., description="Workspace directory.")
    iteration_dir: str = Field(..., description="Path to iteration directory containing pipeline.py.")
    compile_timeout_sec: int = Field(default=120, description="Compile timeout in seconds.")
    execution_timeout_sec: int | None = Field(default=None, description="Execution timeout. None = no timeout.")
    env_extra: dict[str, str] = Field(default_factory=dict, description="Extra env vars for subprocess.")


@mcp.tool(name="rsi_submit_pipeline",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_submit_pipeline(params: RsiSubmitPipelineInput) -> str:
    """Compile, execute, and review a pipeline iteration.

    The agent must have already written pipeline.py + operators/ + decision.json
    into iteration_dir. Returns review score, pass/fail, issues, and next_iteration_dir.

    If a downstream eval is still running, or a requested baseline has no
    result yet, the pipeline is refused until that result has been read.
    """
    return await asyncio.to_thread(_run_iteration_operation, _submit_pipeline_sync, params)


def _submit_pipeline_sync(params: RsiSubmitPipelineInput) -> str:
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()

    active = unfinished_jobs(ws)
    if active:
        return json.dumps({
            "status": "downstream_eval_running",
            "jobs": [{"job_id": j.job_id, "source": j.source, "env": j.env} for j in active],
            "instruction": "Poll all unfinished feedback jobs before submitting the next pipeline.",
        })
    missing = missing_baselines(state)
    if missing:
        return json.dumps({"status": "downstream_baseline_pending", "sources": missing,
                           "instruction": "Run the baseline for each listed source, in parallel when resources permit, and consume their results first."})

    api_key = _api_key(state, state.review_api_key_env)
    # PipelineLLMServing reads its key from the env var named by
    # key_name_of_api_key, so expose the pipeline key under that name to the
    # pipeline subprocess only; it is never written to disk.
    env_extra = dict(params.env_extra or {})
    pipeline_key = _api_key(state, state.pipeline_api_key_env)
    if pipeline_key:
        env_extra[state.pipeline_api_key_env] = pipeline_key
    result = _submit_pipeline(
        ws,
        iteration_dir=params.iteration_dir,
        compile_timeout_sec=params.compile_timeout_sec,
        execution_timeout_sec=params.execution_timeout_sec,
        execution_repairs=2,  # repair budget (matches framework default)
        review_api_url=state.review_api_url,
        review_api_key=api_key,
        review_model=state.review_model,
        env_extra=env_extra,
    )
    response: dict[str, Any] = {
        "iteration": result.iteration,
        "review_score": result.review_score,
        "passed": result.passed,
        "issues": result.issues,
        "domain_feedback": result.domain_feedback,
        "execution_error": result.execution_error,
        "execution_observation": result.execution_observation,
        "failure_stage": result.failure_stage,
        "incumbent_updated": result.incumbent_updated,
        "incumbent_score": result.incumbent_score,
        "next_iteration_dir": result.next_iteration_dir,
        "pipeline_llm_usage": result.pipeline_llm_usage,
        "review_llm": result.review_llm,
        "step_reuse": result.step_reuse,
        "evolution_history": write_history_snapshot(ws),
    }
    if result.failure_stage == "review":
        response["review_error"] = {
            "dataset_path": result.dataset_path,
            "evidence": str(Path(params.iteration_dir) / "review.jsonl"),
            "instruction": (
                "The review LLM produced no verdict, so this is not a quality result: "
                "nothing was compared or accepted and the iteration did not advance. "
                "The generated data is kept. Check the attempts in review.jsonl "
                "(raw reply, HTTP status, finish_reason), then call rsi_review_iteration "
                "with this iteration_dir to review the same data again without "
                "re-running the pipeline."
            ),
        }
    # If repair_feedback is present: execution failed but repair budget remains
    if result.repair_feedback is not None:
        response["repair_feedback"] = {
            "failure_stage": result.repair_feedback.failure_stage,
            "traceback": result.repair_feedback.traceback,
            "diagnostics": result.repair_feedback.diagnostics,
            "repairs_used": result.repair_feedback.repairs_used,
            "repairs_remaining": result.repair_feedback.repairs_remaining,
            "log_paths": result.repair_feedback.log_paths,
            "instruction": (
                "Fix pipeline.py and/or operators/ in the SAME iteration_dir "
                "using the traceback and diagnostics above, then re-call "
                "rsi_submit_pipeline with the same iteration_dir."
            ),
        }
    return json.dumps(response, ensure_ascii=False)


class RsiReviewIterationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str = Field(..., description="Workspace directory.")
    iteration_dir: str = Field(..., description="An iteration whose pipeline already executed successfully.")


@mcp.tool(name="rsi_review_iteration",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_review_iteration(params: RsiReviewIterationInput) -> str:
    """Review an already generated dataset again, without re-running its pipeline.

    Use it when rsi_submit_pipeline returned failure_stage "review" (the review
    LLM produced no verdict): a verdict here completes that iteration exactly as
    a successful submit would. It can also re-score a completed iteration (for
    example after its earlier review failed, or when the user asks); the new
    verdict replaces the old one and the incumbent is recomputed. A failed
    re-review changes nothing. It refuses iterations that never produced a
    dataset and iterations whose pipeline files changed after the dataset was
    generated. Do not call it repeatedly to fish for a higher score.
    """
    return await asyncio.to_thread(_run_iteration_operation, _review_iteration_sync, params)


def _review_iteration_sync(params: RsiReviewIterationInput) -> str:
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    result = _review_iteration(
        ws,
        iteration_dir=params.iteration_dir,
        review_api_url=state.review_api_url,
        review_api_key=_api_key(state, state.review_api_key_env),
        review_model=state.review_model,
    )
    write_history_snapshot(ws)
    return json.dumps(result, ensure_ascii=False)


class RsiDiagnoseInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str = Field(..., description="Workspace directory.")
    iteration_dir: str = Field(..., description="Iteration directory to diagnose.")


@mcp.tool(name="rsi_diagnose_pipeline",
          annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
async def rsi_diagnose_pipeline(params: RsiDiagnoseInput) -> str:
    """Prepare a task for a fresh host-managed diagnostic sub-agent.

    The host starts the sub-agent with delegation.task, waits for its JSON
    result, and saves it to output_path before continuing the RSI loop.
    """
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()

    iter_dir = Path(params.iteration_dir).resolve()
    summary = next(
        (it for it in state.iterations if Path(it.directory).resolve() == iter_dir), None
    )
    review_issues = list(summary.issues) if summary else []
    if summary and summary.execution_error and not review_issues:
        review_issues = [summary.execution_error]
    domain_paths = [str(ws.skills_dir() / name / "SKILL.md") for name in state.domain_skills]
    ctx = build_diagnostic_context(
        iter_dir,
        task_description=state.task_description,
        review_issues=review_issues,
        domain_skill_paths=domain_paths,
        execution_observation_path=summary.execution_observation_path if summary else None,
        execution_error=summary.execution_error if summary else None,
        quality_criteria=state.quality_criteria,
        target_schema=state.target_schema,
    )
    return json.dumps({
        "status": "ok",
        **ctx,
        "delegation": {
            "runner": "host_subagent",
            "fresh_context": True,
            "inherit_conversation": False,
            "artifact_access": "read_only",
            "task": ctx["prompt_text"],
            "result_format": "json_object",
        },
        "instruction": (
            "Start a fresh diagnostic sub-agent using the host's native agent tool, "
            "without inheriting the parent conversation. Pass delegation.task and "
            "any selected external domain skill paths. Wait for its JSON result, "
            "save it to output_path, then continue. If fresh-context delegation "
            "is unavailable, report that the independent diagnosis is blocked."
        ),
    }, ensure_ascii=False)


class RsiTriggerEvalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str
    source: str
    stage: str = "periodic"
    env: dict[str, str] = Field(default_factory=dict, description="Non-secret placement settings chosen by the agent, e.g. CUDA_VISIBLE_DEVICES=0,1. API keys go through source.api_key_envs, never here.")


@mcp.tool(name="rsi_trigger_downstream_eval",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_trigger_downstream_eval(params: RsiTriggerEvalInput) -> str:
    """Start a source's async evaluation. No global concurrency limit.

    Inspect resources and active jobs first, then decide which sources can run
    in parallel. Pass GPU placement through env; API-only jobs can overlap GPU
    jobs. Only duplicate active jobs of the same source/stage/incumbent are
    deduplicated. Read every launched result before the next pipeline.
    """
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    source = state.feedback_sources.get(params.source)
    if source is None:
        return json.dumps({"status": "error", "error": "unknown feedback source"})
    forbidden = set(source.api_key_envs) | {state.review_api_key_env, state.pipeline_api_key_env,
                                           state.attribution_api_key_env}
    if forbidden.intersection(params.env):
        return json.dumps({"status": "error", "error": "Do not pass credentials in env; register their names in api_key_envs."})
    secrets = {name: _api_key(state, name) for name in source.api_key_envs}
    result = trigger_downstream_eval(ws, params.source, stage=params.stage,
                                     env=params.env, secrets=secrets)
    return json.dumps(result, ensure_ascii=False)


class RsiGetEvalResultInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str
    job_id: str | None = Field(default=None, description="A job id, or omit to poll all unfinished jobs.")


@mcp.tool(name="rsi_get_downstream_result",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
async def rsi_get_downstream_result(params: RsiGetEvalResultInput) -> str:
    """Read one job's feedback, or poll every unfinished job when job_id is omitted.

    Each source has its own scores, delta_previous, delta_baseline, optional
    script_analysis and attribution. Never merges unlike metrics into a review
    score. Raw bad cases remain audit-only. Subsequent reads reuse saved feedback.
    """
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    key = _api_key(state, state.attribution_api_key_env)
    result = (get_downstream_result(ws, params.job_id, attribution_api_key=key)
              if params.job_id else poll_downstream(ws, attribution_api_key=key))
    return json.dumps(result, ensure_ascii=False)


class RsiSetCredentialEnvInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str = Field(..., description="Workspace directory.")
    review_api_key_env: str = Field(
        default="",
        description="New env var name for the review LLM API key. Leave empty to keep current.",
    )
    pipeline_api_key_env: str = Field(
        default="",
        description="New env var name for the pipeline LLM API key. Leave empty to keep current.",
    )
    attribution_api_key_env: str = Field(
        default="",
        description="New env var name for the attribution LLM API key. Leave empty to keep current.",
    )
    credentials_file: str = Field(
        default="",
        description="New KEY=value credentials file path. Leave empty to keep current.",
    )


@mcp.tool(name="rsi_set_credential_env")
async def rsi_set_credential_env(params: RsiSetCredentialEnvInput) -> str:
    """Update the env var names used to look up API keys in an existing workspace.

    The key VALUES are never stored. This tool only records which env var name
    to read at runtime. Use it when you want to switch API keys between
    experiments without reinitialising the whole workspace.

    Example: the workspace was created with review_api_key_env="RSI_REVIEW_API_KEY".
    You now want to use "EXP2_REVIEW_KEY" instead. Call this tool with
    review_api_key_env="EXP2_REVIEW_KEY" and make sure that env var is exported
    in the shell where the MCP server runs.
    """
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    changed: list[str] = []
    if params.review_api_key_env:
        state.review_api_key_env = params.review_api_key_env
        changed.append(f"review_api_key_env={params.review_api_key_env}")
    if params.pipeline_api_key_env:
        state.pipeline_api_key_env = params.pipeline_api_key_env
        changed.append(f"pipeline_api_key_env={params.pipeline_api_key_env}")
    if params.attribution_api_key_env:
        state.attribution_api_key_env = params.attribution_api_key_env
        changed.append(f"attribution_api_key_env={params.attribution_api_key_env}")
    if params.credentials_file:
        if not Path(params.credentials_file).expanduser().is_file():
            return json.dumps({
                "status": "error",
                "error": f"credentials_file not found: {params.credentials_file}",
            }, ensure_ascii=False)
        state.credentials_file = str(Path(params.credentials_file).expanduser().resolve())
        changed.append(f"credentials_file={state.credentials_file}")
    if not changed:
        return json.dumps({"status": "no_change", "message": "No fields updated."})
    ws.save(state)
    return json.dumps({
        "status": "ok",
        "updated": changed,
        "note": (
            "Ensure the named env vars are exported in the shell where the "
            "MCP server process runs. No key values are stored on disk."
        ),
    }, ensure_ascii=False)


class RsiStatusInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str = Field(..., description="Workspace directory.")


@mcp.tool(name="rsi_get_status",
          annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
async def rsi_get_status(params: RsiStatusInput) -> str:
    """Return a summary of the workspace: incumbent, iteration history, downstream.

    Call it first in a new session or after the conversation was compacted: it
    rebuilds the whole state from disk, evolution_history included.
    """
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    recorded = [it.pipeline_llm_usage for it in state.iterations if it.pipeline_llm_usage]
    usage_total = {
        "request_count": sum(u["request_count"] for u in recorded),
        "failure_count": sum(u["failure_count"] for u in recorded),
        "total_tokens": sum(u["tokens"]["total_tokens"] for u in recorded),
        "iterations_without_record": len(state.iterations) - len(recorded),
    }
    return json.dumps({
        "task_description": state.task_description,
        "incumbent_iteration": state.incumbent_iteration,
        "incumbent_score": state.incumbent_score,
        "incumbent_dataset_path": state.incumbent_dataset_path,
        "next_iteration": state.next_iteration,
        "total_iterations_run": len(state.iterations),
        "iterations": [
            {
                "iteration": it.iteration,
                "review_status": it.review_status,
                "review_score": it.review_score,
                "passed": it.passed,
                "accepted_as_incumbent": it.accepted_as_incumbent,
                "issues": it.issues[:3],
                "failure_stage": it.execution_error and "exec_error",
                "pipeline_llm_usage": (
                    {
                        "request_count": it.pipeline_llm_usage["request_count"],
                        "failure_count": it.pipeline_llm_usage["failure_count"],
                        "total_tokens": it.pipeline_llm_usage["tokens"]["total_tokens"],
                    }
                    if it.pipeline_llm_usage else None
                ),
            }
            for it in state.iterations
        ],
        "pipeline_llm_usage_total": usage_total,
        "evolution_history": evolution_history(state),
        "evolution_history_file": str(ws.root / "evolution_history.json"),
        "feedback_sources": {name: {**asdict(source), "protocol": protocol_fingerprint(source)}
                             for name, source in state.feedback_sources.items()},
        "missing_baselines": missing_baselines(state),
        "downstream_jobs": [
            {
                "job_id": j.job_id,
                "source": j.source,
                "env": j.env,
                "stage": j.stage,
                "incumbent_iteration": j.iteration,
                "checkpoint_iteration": j.checkpoint_iteration,
                "status": j.status,
                "scores": (j.result or {}).get("scores"),
                "delta_baseline": (j.result or {}).get("delta_baseline"),
                "attribution": ((j.result or {}).get("attribution") or {}).get("status"),
                "error": j.error,
            }
            for j in state.downstream_jobs
        ],
        "domain_skills": state.domain_skills,
        "runtime": runtime_contract(),
        "pipeline_llm": _pipeline_llm_contract(state),
        "input_contract": state.input_contract,
        "output_artifact_fields": state.output_artifact_fields,
        "media_policy": state.media_policy,
    }, ensure_ascii=False)


class RsiExportInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workspace_dir: str = Field(..., description="Workspace directory.")
    output_path: str = Field(default="", description="Destination path. Defaults to workspace_dir/export/incumbent.jsonl.")


@mcp.tool(name="rsi_export_dataset",
          annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True))
async def rsi_export_dataset(params: RsiExportInput) -> str:
    """Export the current incumbent dataset to a specified path."""
    ws = _load_workspace(params.workspace_dir)
    state = ws.state()
    if not state.incumbent_dataset_path:
        return json.dumps({"status": "error", "error": "No incumbent dataset yet."})
    src = Path(state.incumbent_dataset_path)
    if not src.exists():
        return json.dumps({"status": "error", "error": f"Incumbent dataset not found: {src}"})
    if params.output_path:
        dst = Path(params.output_path)
    else:
        dst = ws.root / "export" / "incumbent.jsonl"
    try:
        bundle = await asyncio.to_thread(export_media_dataset, src, dst)
    except (OSError, ValueError) as exc:
        return json.dumps({"status": "error", "error": f"export failed: {exc}"})
    return json.dumps({
        "status": "ok",
        **bundle,
        "incumbent_iteration": state.incumbent_iteration,
        "incumbent_score": state.incumbent_score,
    }, ensure_ascii=False)


def serve():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    serve()
