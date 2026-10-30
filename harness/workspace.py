"""Workspace state management for the DataFlow-Evolve plugin.

Each RSI workspace is a directory holding:
  workspace.json          — persistent state (task config, incumbent, iteration counter,
                            feedback sources, downstream jobs)
  evolution_history.json  — snapshot of the evolution history, rewritten by submit / review
  skills/                 — domain skills the agent registered with rsi_add_domain_skill
  eval_scripts/           — downstream eval scripts written with the user
  iterations/             — iteration_NNN directories (agent writes pipeline.py here)
  downstream_jobs/        — one directory per downstream eval job
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


STATE_FILE = "workspace.json"
SCHEMA_VERSION = "1"


@dataclass
class IterationSummary:
    iteration: int
    directory: str
    review_score: float | None = None
    passed: bool | None = None
    accepted_as_incumbent: bool = False
    issues: list[str] = field(default_factory=list)
    execution_error: str | None = None
    repairs_used: int = 0           # how many repair attempts were made
    execution_observation_path: str | None = None
    review_status: str | None = None    # ok | error; None while no review ran
    dataset_path: str | None = None
    pipeline_llm_usage: dict[str, Any] | None = None
    source_fingerprint: str | None = None   # pipeline files when the dataset was generated


@dataclass
class FeedbackSource:
    """One downstream feedback signal: an eval script and what it reports."""

    name: str
    command: list[str]
    trains_model: bool              # train on the dataset first, or score the dataset itself
    benchmarks: list[str]           # named scores the script reports (benchmarks or metrics)
    sampling_mode: str = "full"     # full | subset
    subset_size: int | None = None
    sampling_seed: int | None = None
    bad_cases_per_benchmark: int = 9
    timeout_sec: int | None = None
    baseline: bool = False          # evaluate the starting point once
    attribution: bool = False       # attribute its bad cases with the attribution LLM
    api_key_envs: list[str] = field(default_factory=list)   # key names injected at run time
    resources: str = ""             # what one run needs, for the agent to plan parallel runs
    script_origin: str = "user"     # user | workspace (written into eval_scripts/)


@dataclass
class DownstreamJob:
    job_id: str
    source: str
    stage: str                      # baseline | periodic
    iteration: int | None           # incumbent evaluated; None for a baseline
    dataset_path: str | None
    checkpoint_iteration: int       # iterations finished when the job started
    protocol: str                   # fingerprint of the source's comparable settings
    env: dict[str, str] = field(default_factory=dict)   # placement the agent chose
    status: str = "pending"   # pending | running | ready | failed
    result: dict[str, Any] | None = None   # the feedback returned to the agent
    error: str | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    pid: int | None = None


@dataclass
class WorkspaceState:
    task_description: str = ""
    input_path: str = ""
    workspace_dir: str = ""
    provider: str = "claude"          # claude | codex
    # LLM API config. Credential values are never persisted; only env-var names.
    review_api_url: str = ""
    review_api_key_env: str = "RSI_REVIEW_API_KEY"
    review_model: str = ""
    pipeline_api_url: str = ""
    pipeline_api_key_env: str = "RSI_PIPELINE_API_KEY"
    pipeline_model: str = ""
    # Optional KEY=value file, re-read on every call so a key change applies
    # without restarting the MCP server. Looked up before the process env.
    credentials_file: str = ""
    pipeline_max_workers: int = 25
    # quality criteria (user-defined)
    quality_criteria: list[str] = field(default_factory=list)
    target_schema: dict[str, str] = field(default_factory=lambda: {"messages": "list[dict]"})
    # downstream feedback: any number of sources, each with its own eval script
    feedback_sources: dict[str, FeedbackSource] = field(default_factory=dict)
    # Optional bad-case attribution LLM, shared by the sources that use it
    attribution_api_url: str = ""
    attribution_api_key_env: str = "RSI_ATTRIBUTION_API_KEY"
    attribution_model: str = ""
    attribution_max_findings: int = 6
    attribution_max_actions: int = 8
    # evolution state
    incumbent_iteration: int | None = None
    incumbent_score: float | None = None
    incumbent_passed: bool | None = None
    incumbent_dataset_path: str | None = None
    next_iteration: int = 0
    iterations: list[IterationSummary] = field(default_factory=list)
    downstream_jobs: list[DownstreamJob] = field(default_factory=list)
    domain_skills: list[str] = field(default_factory=list)   # registered skill names

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": SCHEMA_VERSION, **asdict(self)}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "WorkspaceState":
        d = dict(d)
        version = d.pop("schema_version", None)
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"workspace.json has schema version {version!r}, this harness reads "
                f"{SCHEMA_VERSION!r}; start a new workspace with rsi_init"
            )
        iterations = [IterationSummary(**it) for it in d.pop("iterations", [])]
        jobs = [DownstreamJob(**j) for j in d.pop("downstream_jobs", [])]
        sources = {name: FeedbackSource(**s) for name, s in d.pop("feedback_sources", {}).items()}
        state = cls(**d)
        state.iterations = iterations
        state.downstream_jobs = jobs
        state.feedback_sources = sources
        return state


class Workspace:
    """Load, mutate, and persist workspace state."""

    def __init__(self, workspace_dir: str | Path) -> None:
        self.root = Path(workspace_dir).resolve()
        self._state: WorkspaceState | None = None

    def _state_path(self) -> Path:
        return self.root / STATE_FILE

    def load(self) -> WorkspaceState:
        p = self._state_path()
        if not p.exists():
            raise FileNotFoundError(f"Workspace not initialised at {self.root}")
        self._state = WorkspaceState.from_dict(json.loads(p.read_text(encoding="utf-8")))
        return self._state

    def save(self, state: WorkspaceState) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        p = self._state_path()
        p.write_text(json.dumps(state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        self._state = state

    def state(self) -> WorkspaceState:
        if self._state is None:
            return self.load()
        return self._state

    def iteration_dir(self, n: int) -> Path:
        return self.root / "iterations" / f"iteration_{n:03d}"

    def next_iteration_dir(self) -> tuple[int, Path]:
        s = self.state()
        n = s.next_iteration
        return n, self.iteration_dir(n)

    def advance_iteration(self) -> None:
        s = self.state()
        s.next_iteration += 1
        self.save(s)

    def skills_dir(self) -> Path:
        return self.root / "skills"

    def eval_scripts_dir(self) -> Path:
        return self.root / "eval_scripts"

    def register_downstream_job(self, **fields: Any) -> DownstreamJob:
        job = DownstreamJob(job_id=str(uuid.uuid4())[:8], **fields)
        s = self.state()
        s.downstream_jobs.append(job)
        self.save(s)
        return job

    def update_downstream_job(self, job_id: str, **kwargs: Any) -> DownstreamJob | None:
        s = self.state()
        for job in s.downstream_jobs:
            if job.job_id == job_id:
                for k, v in kwargs.items():
                    setattr(job, k, v)
                self.save(s)
                return job
        return None

    def get_downstream_job(self, job_id: str) -> DownstreamJob | None:
        for job in self.state().downstream_jobs:
            if job.job_id == job_id:
                return job
        return None

    def record_iteration(self, summary: IterationSummary) -> None:
        s = self.state()
        for i, it in enumerate(s.iterations):
            if it.iteration == summary.iteration:
                s.iterations[i] = summary
                self.save(s)
                return
        s.iterations.append(summary)
        self.save(s)

    def update_incumbent(self, iteration: int, score: float, passed: bool,
                         dataset_path: str) -> None:
        s = self.state()
        s.incumbent_iteration = iteration
        s.incumbent_score = score
        s.incumbent_passed = passed
        s.incumbent_dataset_path = dataset_path
        for it in s.iterations:
            if it.iteration == iteration:
                it.accepted_as_incumbent = True
        self.save(s)


def init_workspace(workspace_dir: str | Path, **kwargs: Any) -> Workspace:
    ws = Workspace(workspace_dir)
    ws.root.mkdir(parents=True, exist_ok=True)
    state = WorkspaceState(workspace_dir=str(ws.root), **kwargs)
    ws.save(state)
    (ws.root / "iterations").mkdir(exist_ok=True)
    (ws.root / "skills").mkdir(exist_ok=True)
    (ws.root / "eval_scripts").mkdir(exist_ok=True)
    return ws
