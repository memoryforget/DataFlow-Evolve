"""Scaffolding for files the agent writes: domain skills and eval scripts.

The agent writes their content itself, with the user and the whole conversation
in view; no LLM is called here. The harness supplies what is fixed (the eval
script protocol, a protocol-correct skeleton, the SKILL.md format), puts the
files where the workspace expects them, and registers them.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .workspace import Workspace

_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def check_name(name: str) -> str | None:
    """None when the name is a usable file / directory name, else the problem."""
    if not _NAME_RE.match(name or ""):
        return "name must be 1-64 chars of lowercase letters, digits, '-' or '_'"
    return None


# ── domain skills ─────────────────────────────────────────────────────────────

SKILL_GUIDE = """Write the body of a SKILL.md that teaches you (and later sessions) how to
write pipeline operators for this domain. The harness adds the frontmatter from
name and description. Cover, in imperative form and under 200 lines:
  - domain data patterns and pitfalls seen in this corpus
  - recommended operators and their single responsibilities
  - fields that must never be dropped or altered
  - domain quality gates (what a record must satisfy to be kept)
Operators use the dataflow_evolve contracts (OperatorABC, PipelineABC, FileStorage,
PipelineLLMServing); do not restate the generic pipeline contract."""


def register_domain_skill(workspace: Workspace, name: str, description: str,
                          body: str) -> dict[str, Any]:
    """Write workspace/skills/<name>/SKILL.md from the agent's body and register it."""
    skill_dir = workspace.skills_dir() / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    path = skill_dir / "SKILL.md"
    description = " ".join(description.split())
    path.write_text(f"---\nname: {name}\ndescription: {description}\n---\n\n{body.strip()}\n",
                    encoding="utf-8")
    state = workspace.state()
    replaced = name in state.domain_skills
    if not replaced:
        state.domain_skills.append(name)
        workspace.save(state)
    return {"status": "ok", "skill_name": name, "skill_path": str(path), "replaced": replaced}


# ── eval scripts ──────────────────────────────────────────────────────────────

PROTOCOL = """Downstream eval script protocol (DataFlow-Evolve)

argv:  <command> --request <request.json> --result <result.json>
       ("{request}" / "{result}" inside the command are replaced instead)
cwd:   the workspace directory. env: the MCP server's environment, the API keys
       named in the source's api_key_envs, and the env given at trigger time
       (e.g. CUDA_VISIBLE_DEVICES). Several jobs may run at the same time.

request.json:
  source                   feedback source name
  stage                    "periodic": evaluate the incumbent dataset.
                           "baseline": evaluate the starting point once: the untrained
                           base model for a source that trains (dataset_path null), the
                           raw input corpus for a source that scores data
  training_performed       true when this run trains on dataset_path first
  checkpoint_iteration     iterations finished when the job started
  incumbent_iteration      iteration whose dataset is evaluated (null for a baseline)
  dataset_path             dataset JSONL to train on or score
  pipeline_path            pipeline that produced it (null for a baseline)
  expected_benchmarks      the names to report; report every one of them
  bad_cases_per_benchmark  most failed examples to return per name
  sampling_mode            "full" or "subset"
  subset_size, sampling_seed
                           with "subset": the same subset_size samples, chosen with
                           sampling_seed, every run, so results stay comparable
  task                     task_description, quality_criteria, target_schema

result.json:
  {
    "status": "ok" | "failed",
    "model": "<what was evaluated>",
    "benchmarks": {
      "<name>": {
        "score": <float>,                    required; higher is better
        "metric": "<e.g. accuracy, avg@4, judge_mean>",
        "correct_count", "total_count", "incorrect_count",
        "wrong_answer_count", "parse_failure_count", "runaway_count": <int>  optional
        "bad_cases": [                       optional; only used for attribution
          {"question": str, "model_response": str, "parsed_answer": str,
           "correct_answer": str (optional when there is no reference answer),
           "response_token_count": int, "finish_reason": str,
           "length_truncated": bool, "dimension": str}
        ]
      }
    },
    "summary": "<optional: the script's own analysis>",
    "recommended_pipeline_changes": ["<optional: the script's own suggestions>"],
    "artifacts": {"<name>": "<path>"},
    "error": "<only when status is failed>"
  }

Do not compute score deltas: the harness computes them per source against the
previous result and the baseline. Exit non-zero, or write status "failed" with
an error, when the evaluation could not run; that is reported as an
infrastructure failure, never as a data signal."""

_TRAINING_SKELETON = '''"""Downstream eval script: {name}

{description}

Trains on the dataset, then evaluates. Protocol: see the harness PROTOCOL.
"""
import argparse
import json
from pathlib import Path

BASE_MODEL = "TODO: base model path or id"


def train(dataset_path, request):
    """Fine-tune BASE_MODEL on dataset_path; return the trained model's path or id."""
    raise NotImplementedError("TODO: SFT on the dataset")


def evaluate(model, name, request):
    """Score `model` on `name`, on fixed samples (sampling_mode / subset_size / sampling_seed).

    Return {{"score": float, "metric": str}}, optionally with counts and up to
    request["bad_cases_per_benchmark"] bad_cases.
    """
    raise NotImplementedError(f"TODO: evaluate {{name}}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()
    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    try:
        if request["training_performed"]:
            model = train(request["dataset_path"], request)
        else:
            model = BASE_MODEL   # baseline: the untrained base model
        result = {{
            "status": "ok",
            "model": str(model),
            "benchmarks": {{name: evaluate(model, name, request)
                           for name in request["expected_benchmarks"]}},
        }}
    except Exception as exc:  # reported as an infrastructure failure, not a data signal
        result = {{"status": "failed", "error": f"{{type(exc).__name__}}: {{exc}}"}}
    Path(args.result).write_text(json.dumps(result, ensure_ascii=False, indent=2),
                                 encoding="utf-8")


if __name__ == "__main__":
    main()
'''

_SCORING_SKELETON = '''"""Downstream eval script: {name}

{description}

Scores the dataset itself, without training (e.g. an LLM-API judge or a
script metric). Protocol: see the harness PROTOCOL.
"""
import argparse
import json
from pathlib import Path


def load_rows(dataset_path, request):
    """The rows to score: all of them, or the fixed subset (subset_size, sampling_seed)."""
    raise NotImplementedError("TODO: read dataset_path, apply the sampling settings")


def score(rows, name, request):
    """Return {{"score": float, "metric": str}} for metric `name`, optionally with counts and
    up to request["bad_cases_per_benchmark"] bad_cases."""
    raise NotImplementedError(f"TODO: score {{name}}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()
    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    try:
        # periodic: the incumbent dataset; baseline: the raw input corpus
        rows = load_rows(request["dataset_path"], request)
        result = {{
            "status": "ok",
            "model": "dataset",
            "benchmarks": {{name: score(rows, name, request)
                           for name in request["expected_benchmarks"]}},
        }}
    except Exception as exc:  # reported as an infrastructure failure, not a data signal
        result = {{"status": "failed", "error": f"{{type(exc).__name__}}: {{exc}}"}}
    Path(args.result).write_text(json.dumps(result, ensure_ascii=False, indent=2),
                                 encoding="utf-8")


if __name__ == "__main__":
    main()
'''


def scaffold_eval_script(workspace: Workspace, name: str, description: str,
                         trains_model: bool, overwrite: bool = False) -> dict[str, Any]:
    """Write a protocol-correct skeleton to eval_scripts/<name>.py for the agent to finish."""
    path = workspace.eval_scripts_dir() / f"{name}.py"
    if path.exists() and not overwrite:
        return {"status": "exists", "script_path": str(path), "protocol": PROTOCOL,
                "message": "a script with this name exists; edit it, or pass overwrite=true "
                           "to replace it with a fresh skeleton"}
    path.parent.mkdir(parents=True, exist_ok=True)
    template = _TRAINING_SKELETON if trains_model else _SCORING_SKELETON
    path.write_text(template.format(name=name, description=" ".join(description.split())),
                    encoding="utf-8")
    return {"status": "ok", "script_path": str(path), "protocol": PROTOCOL}
