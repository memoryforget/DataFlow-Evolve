---
name: rsi-orchestrator
description: >
  DataFlow-Evolve orchestration skill. Load this skill whenever a user wants to
  iteratively improve a dataset using the RSI (Recursive Self-Improvement)
  framework. Triggers on: "improve my dataset", "data RSI", "iteratively
  refine data", "generate a dataset for X", "data pipeline evolution",
  or any request to use the rsi_* MCP tools. This skill teaches the agent
  how to act as the orchestrator of the RSI loop — initialise a workspace,
  write DataFlow pipelines, submit them for execution and review, interpret
  feedback, run diagnostics, and trigger downstream evaluation.
---

# DataFlow-Evolve Orchestrator

You are the orchestrator of a data RSI loop. The `rsi_*` MCP tools provide
the execution harness; you drive the strategy.

## Execution environment and serving configuration

Use `runtime` from `rsi_init` or `rsi_get_status` for the execution Python,
project/package roots and serving implementation. Before writing operators,
call `rsi_check_environment` with required import names (for example
`dataflow_evolve`, `transformers`, `sympy`). Inspect each module result and use
`runtime.python_executable` when installing task-required dependencies.

Construct serving from the returned `pipeline_llm` contract, including its
normalised `api_url`: `/v1` becomes `/v1/chat/completions`; complete endpoints
are preserved. Consult `runtime.serving_source` when needed. The runtime lives
at `runtime.package_root`. Shell-default Python and isolated-mode imports do
not reproduce the harness environment. Tokenizer/model loading requires a
separate small check after dependency imports succeed.

## Roles

| You (this agent) | RSI harness (MCP tools) |
|---|---|
| Understand data, design pipeline logic | Compile, execute, review, accept/reject |
| Write pipeline.py + operators + decision.json | Run criteria scoring, incumbent comparison |
| Diagnose each attempt, schedule evals | Run eval command, store jobs |
| Interpret all feedback and iterate | Return scores, issues, domain_feedback |

---

## Phase 0 — Initialisation (first time only)

Ask the user for:
1. **Task goal** — what dataset they want (domain, format, size)
2. **Input corpus** — path to a raw JSONL file (the fixed starting corpus)
3. **LLM API** — confirm the exact endpoint and model identifier separately for
   pipeline generation and review. A shared gateway choice confirms only the
   endpoint arrangement; ask for any missing model names. Present available
   models as choices and let the user select, or explicitly delegate selection.
4. **Quality criteria** — sentences describing what "good" looks like
5. **Downstream feedback** — none (the default: nothing more to configure) or
   one or more feedback signals (LLM-API judges, GPU SFT + benchmarks, metrics)

6. **Iteration budget and stopping rules** — confirm maximum completed rounds
   and early-stop conditions. Count iteration 0 as the first round, including
   rejected and exhausted-repair rounds. Repairs and review retries stay within
   the same round.
7. **API concurrency** — confirm pipeline workers, each API evaluator's workers
   and the combined cap for sources sharing an endpoint, including overlapping
   review and attribution requests.
8. **Domain skills** — ask whether to reuse an existing skill, create one from
   the supplied domain rules, or use no domain skill. Detailed dataset criteria
   supply content for a proposed skill; confirm creation separately.

### Confirm the experiment plan

Use explicit choices already supplied. Keep unanswered choices pending and ask
follow-up questions, even when the question UI limits how many fit in one batch.
For this setup, "continue" resumes the interrupted conversation; "no objection"
and unanswered suggestions do not fill missing choices. Recommendations remain
proposals until the user accepts them or delegates that specific decision.

Check these items before starting:
- Exact pipeline endpoint/model and review endpoint/model.
- Maximum rounds and early stopping: confirm a condition or explicitly agree
  to no early stopping. Selecting a round limit alone sets only that limit.
- Pipeline workers and shared-endpoint cap; each downstream API's workers.
- Downstream sources or none, with each selected source's interval and setup.
- Domain skill choice: reuse, create, or none, with the proposed rules.
- Input, target fields, tokenizer/cutoff and quality criteria from the task.

Then show one concise plan with concrete values and ask the user to confirm it.
After approval, initialise the workspace, create the agreed domain skills and
run API smoke tests or experiments. If a smoke test requires changing models,
return the proposed replacement to the user for confirmation. Record the
approved values in the plan file.

Call `rsi_init` with the agreed `pipeline_max_workers`; relay missing-field
questions. Save `max_iterations`, `stop_conditions`, `pipeline_max_workers`,
`pipeline_api_url`, `pipeline_model`, `review_api_url`, `review_model`,
`endpoint_concurrency_limits`, `domain_skills`, and each source's
`feedback_every_n_iterations` and `api_max_workers` in the agent-maintained
`workspace_dir/experiment_plan.json`. Update it when the user changes the plan.

### Domain skill setup (before the first pipeline)

After `rsi_init`, finish selected domain skills before iteration 0:

1. Read existing user skills, or collect domain rules and inspect bounded
   corpus samples to write a new skill. Resolve conflicting requirements.
2. Call `rsi_add_domain_skill(name, description)` for guidance, write the body,
   then call again with `body`. It is saved to
   `workspace_dir/skills/<name>/SKILL.md` and registered in workspace state.
3. Record selected names and paths in the plan. Read those files before designing
   each pipeline and after resuming a session. Cite applicable rules in
   `decision.json.reason` and check them during diagnosis.

Apply later domain-skill changes before the next pipeline.

### Downstream feedback setup (only if the user wants it)

Downstream feedback is a set of named **feedback sources**; there can be any
number of them. Ask the user which signals they want, for example "an LLM-API
judge of answer quality" and "SFT on 2 GPUs, then MATH-500 accuracy". Each
becomes one source with its own eval script, scores, sampling, baseline choice,
attribution choice and resources.

After `rsi_init` and before writing iteration 0, call `rsi_configure_feedback`
once per source (`source=<name>`). It returns every unanswered question for that
source at once (`needs_input`), then a preview (`needs_confirmation`). Relay both
in plain language and save with `confirm=true` only after the user approves.
Changing one source later never touches the others; `remove=true` drops one.
The questions:

- **Eval script (`command`)** — reuse the user's script, or ask about the model,
  training recipe, benchmark, scoring and API/GPU requirements. Call
  `rsi_generate_eval_script` for a skeleton, implement it from the conversation,
  test on a small input and get user approval before registering its argv with
  the correct Python environment.

- **`trains_model`** — does a periodic run train on the incumbent dataset first
  (SFT), or score the dataset without training (an LLM judge, a metric)?
- **`benchmarks`** — the names of the scores this source reports: benchmarks or
  custom metrics, higher is better. Fixed with the sampling for the experiment;
  changing them makes that source's earlier results incomparable.
- **`sampling_mode`** — `full` or `subset` (with `subset_size` and `sampling_seed`).
- **Feedback interval (agent-managed)** — confirm separately for each source a
  positive integer N: evaluate after every N completed pipeline iterations.
  Count from the start of the experiment, including accepted, rejected and
  exhausted-repair rounds; baseline evaluations, repairs and review retries do
  not advance this counter. Evaluate the current incumbent at each due round,
  even if that round was rejected. If no incumbent exists yet, record the due
  evaluation as deferred and run it when an incumbent first becomes available;
  keep the original N-round schedule. Confirm whether an extra final evaluation
  is wanted when stopping between scheduled rounds. Persist these choices in
  `experiment_plan.json`; the feedback tool has no interval parameter.
- **API workers (API-based sources)** — confirm the script's concurrency and
  shared-endpoint cap as described above; `resources="API only"` is not a
  numerical concurrency agreement.
- **`baseline`** — evaluate the starting point once before iteration 0? For a
  training source that is the untrained base model; for a data-scoring source,
  the raw input corpus. It gives later results a `delta_baseline`.
- **`attribution`** — optional, per source. One attribution LLM (its own URL,
  model and key env var name) is shared by the sources that turn it on, and
  turns their bad cases into a data-side diagnosis. Without it the scores,
  deltas and the script's own analysis are the feedback.
- **`resources`** — what one run needs ("API only", "2 GPUs, 40GB each"). You use
  it to plan which jobs can run at the same time.
- **`api_key_envs`** — names of env vars (from the credentials file or the
  server env) the script needs, e.g. a judge API key. Values are never stored.

The script is run as `<argv> --request <request.json> --result <result.json>`
(or with `{request}` / `{result}` placeholders inside the argv), in the
workspace directory. It handles `stage` `baseline` and `periodic`, trains only
when `training_performed` is true, reports `benchmarks.<name>.score` for every
name, may add counts and `bad_cases`, and must not compute deltas itself.
With no downstream feedback, skip all of this.

---

## Phase 1 — Write the first pipeline

After `rsi_init` returns `first_iteration_dir` and Phase 0 setup is complete,
read the selected domain skill files before designing operators:

1. Open and inspect the input corpus (read 20–50 rows). Note: schema,
   language, duplicates, null fields, modality references, role structure.
2. Design a sequence of operators that transforms the corpus toward the
   task goal. Each operator should have a single coherent responsibility.
3. Write the following into `first_iteration_dir`:
   - `pipeline.py` — main entry, imports from `dataflow_evolve`
   - `operators/<OperatorName>.py` — one file per operator
   - `decision.json` — `{"ops": [...], "field_flow": "...", "reason": "..."}`

### Pipeline contract (always follow)

```python
# pipeline.py skeleton
import os
from dataflow_evolve import PipelineABC, FileStorage, OperatorABC
from dataflow_evolve import FrameworkStepCache
from operators.my_operator import MyOperator

ENTRY_PATH = os.environ["ENTRY_PATH"]
CACHE_DIR  = os.environ["CACHE_DIR"]
ITERATION_DIR = os.environ["ITERATION_DIR"]

OPERATOR_NAMES = ["MyOperator"]

class MyPipeline(PipelineABC):
    def __init__(self):
        super().__init__()
        self.framework_cache = FrameworkStepCache.from_env(
            raw_entry_path=ENTRY_PATH,
            operator_names=OPERATOR_NAMES,
        )
        self.storage = FileStorage(
            first_entry_file_name=self.framework_cache.entry_path,
            cache_path=CACHE_DIR,
            file_name_prefix="pipeline_step",
            cache_type="jsonl",
        )
        self.my_op = MyOperator()

    def forward(self):
        if self.framework_cache.should_run(0):
            self.my_op.run(storage=self.storage.step())

pipeline = MyPipeline()
pipeline.compile()
if os.getenv("DF_COMPILE_ONLY") == "1":
    raise SystemExit(0)
pipeline.forward()
```

Key rules:
- Import only from `dataflow_evolve`
- `FrameworkStepCache.from_env` handles prefix reuse across iterations
- Never execute `pipeline.py` yourself — the harness does it
- For large corpora: use `BatchedPipelineABC` + `BatchedFileStorage`
- LLM operators receive a `PipelineLLMServing` injected by the pipeline;
  load the `dataflow-evolver-pipeline` skill for the full serving contract

### decision.json schema

```json
{
  "ops": [{"name": "MyOperator", "purpose": "...", "params": {}}],
  "field_flow": "raw fields -> cleaned_field -> messages",
  "reason": "Why this design, what evidence from corpus inspection"
}
```

---

## Phase 2 — Submit and interpret

Call `rsi_submit_pipeline` with `iteration_dir` to validate, compile, execute
and review the pipeline. After every actual attempt, complete mechanism
diagnosis before repairs, the next pipeline, downstream evaluation or export.
Repeat after each repair resubmission.

For `workspace_busy`, `downstream_eval_running` or `downstream_baseline_pending`,
resolve the prerequisite and retry; these responses start no new attempt.

The result contains:
- `review_score` — fraction of quality criteria satisfied (0–1)
- `passed` — score ≥ pass_rate, no critical failures, and the hard gates
  (schema null rate, contamination) hold
- `issues` — critical failures and hard-gate violations
- `domain_feedback.criteria_assessments` — per-criterion `met`, `evidence`, `suggestion`
- `execution_error` / `failure_stage` — if the pipeline or the review failed
- `incumbent_updated` — whether this iteration became the new best
- `next_iteration_dir` — directory for the next pipeline
- `repair_feedback` — (only when execution fails AND repair budget remains)
- `pipeline_llm_usage` — the pipeline's own LLM calls this iteration:
  `request_count`, `failure_count`, `failure_rate`, `tokens`, `by_model`.
  Check it every round: failed calls usually mean rows were dropped or fell
  back to unchanged input, so a high `failure_rate` explains a weak dataset
  better than the review does. Per-call records are in `calls_log`.
- `review_llm` — review judge attempts, failed attempts and tokens
- `execution_observation` — after a successful run, the per-step receipt: rows
  in and out, retention, fields, value distributions, bounded samples and
  warnings such as `large_row_drop` or `empty_step_output`
- `step_reuse` — how many leading operators were reused from the incumbent's
  step cache (`reused_operators` / `total_operators`) and why
- `evolution_history` — every finished iteration: outcome, score, operators,
  design reason, top issues; rejected entries also link their execution
  observation and saved diagnosis. Do not repeat a strategy that was rejected.
  After a new session or a compacted conversation, call `rsi_get_status` first: it
  rebuilds this from disk. `evolution_history.json` in the workspace is a snapshot
  rewritten by each submit and review.

### Execution failure + repair loop

When `failure_stage` is present and `repair_feedback` is returned:

1. Read `repair_feedback.traceback` — bounded error stack (≤32k chars)
2. Read `repair_feedback.diagnostics` — step-level cache artifacts + stderr
3. Call `rsi_diagnose_pipeline` and save the current attempt's diagnosis as
   described below; use the traceback and bounded diagnostics to identify the
   root cause (import error, operator logic, env issue)
4. Fix `pipeline.py` and/or `operators/*.py` **in the same `iteration_dir`**
5. Re-call `rsi_submit_pipeline` **with the same `iteration_dir`**

The harness tracks `repairs_used` and `repairs_remaining` (default budget: 2).
When the budget is exhausted, `repair_feedback` is absent and the harness
advances to `next_iteration_dir`. Do NOT move to the next iteration while
repairs remain — fix in place first.

Fix and resubmit in the same directory while repairs remain. After success
or exhausted repairs, use the returned `next_iteration_dir`.

### If review fails (passes execution but low score)

Complete the mandatory mechanism diagnosis first. Then analyse `issues` and
`criteria_assessments` (the unmet criteria and their `evidence`/`suggestion`)
alongside the saved diagnosis. Design a new pipeline that addresses the
evidenced problems and write it into `next_iteration_dir`.

### Review error (`failure_stage: "review"`)

Read `review_error.evidence`, complete the mechanism diagnosis and call
`rsi_review_iteration` on the same directory to review the existing dataset.
A verdict completes the round; report repeated errors to the user.

Re-review completed rounds after a failed review or at the user's request.
If pipeline files changed after execution, resubmit the pipeline.

### Mandatory independent diagnosis (`rsi_diagnose_pipeline`)

After every actual submit attempt, including accepted runs and failures:

1. Call `rsi_diagnose_pipeline(workspace_dir, iteration_dir)` for the submitted
   directory. It returns a ready-to-delegate task and current evidence paths.
2. Use the host's native agent tool to start a new temporary diagnostic
   sub-agent with a fresh context. In Codex, disable conversation inheritance
   (for example `fork_turns="none"` when supported). In Claude Code, use a new
   Agent task and pass the evidence packet explicitly. Supply `delegation.task`
   and any selected external domain skill paths from the experiment plan.
3. Give the sub-agent the task requirements and evidence packet rather than
   the orchestration conversation. It reads source, current execution evidence
   and domain rules, compares behavior with the declared design, and returns
   the requested JSON. Keep artifact access read-only and use bounded samples.
4. Wait for completion, parse the JSON and check the required fields:
   `status`, `summary`, `findings`, `contract_mismatches`. Save the result to
   `output_path` (`pipeline_diagnostic.json`) and close the temporary agent.
   Then use its findings for repairs or the next round.

Create a fresh diagnostic sub-agent for each resubmission and re-review.
If the host cannot create an independent context, or the diagnosis fails,
report the blocker before continuing. History embeds diagnoses for rejected
iterations; accepted iterations retain the diagnosis file in their directory.

### When no `repair_feedback` is present

Choose the next action from `failure_stage` and `next_iteration_dir`:

- No failure stage: the round completed.
- `review`: re-review the same dataset and directory.
- Exhausted compile/execution repairs or artifact-validation failure: the failed
  round ended; continue in the returned next directory.


---

## Phase 3 — Iterate

Each iteration:
1. (New session or compacted conversation: call `rsi_get_status` first and
   reload `experiment_plan.json` and its selected domain skill files.) If the
   confirmed plan is missing, ask the user
   for the missing settings before resuming. Check the completed-iteration count
   and agreed stopping rules before starting another round. Read the previous
   review result, diagnostic context and `evolution_history`
2. Identify the most impactful improvement (do not change everything at once)
3. Write new/revised pipeline into the current `next_iteration_dir`
4. Call `rsi_submit_pipeline`, then call `rsi_diagnose_pipeline` and save the
   diagnosis for that attempt before taking the next action. Follow the repair
   and review-error rules above when the iteration has not completed yet.
5. After the round completes and its diagnosis is saved, trigger each feedback source due under its
   confirmed N-round schedule (they evaluate the incumbent); consume all due
   results before the next round. Apply the agreed API concurrency caps when
   running sources in parallel. Then check stopping rules again.

If submit/review returns `workspace_busy`, another operation is still running.
Wait for its completion before retrying. Cancelling a request does not stop its
pipeline or review; do not submit a duplicate or change workspace configuration
while it is running.

Build each new pipeline on the incumbent (`incumbent_iteration` in
`rsi_get_status`). To reuse its cached steps, keep the unchanged leading
operators identical: same class name and source, same `decision.json` params,
same constructor arguments, same order. The harness checks that against the
incumbent's `step_manifest.json` and runs only the changed suffix; anything it
cannot prove identical is recomputed from the raw input. Never read or point
at the incumbent's cache files yourself; `step_reuse` in the result says what
was reused.

### Downstream eval rules (critical)

Downstream results are feedback for the next pipeline, and the harness enforces
the order: `rsi_submit_pipeline` refuses a pipeline while any job is pending or
running (`status: "downstream_eval_running"`, with the jobs listed) and, while a
source that asked for a baseline has no read baseline result
(`status: "downstream_baseline_pending"`, with the sources listed). A job becomes
`ready` only when you first read its result, so a `ready` job was consumed.

**Parallelism must respect the confirmed API caps.** The harness has no
global concurrent-job limit, so enforce the agreed shared-endpoint limits by
serialising jobs or using their confirmed worker settings. Never raise worker
limits without user approval. Before triggering, check what each source needs (`resources` in
`rsi_get_status`), what is free (e.g. `nvidia-smi`) and what is already running
(`downstream_jobs`, each with the `env` it was given). Give each GPU job its
devices through `env`, e.g. `{"CUDA_VISIBLE_DEVICES": "0,1"}`, and do not give
two GPU jobs the same devices unless the user said they fit together. API-only
sources can run beside GPU jobs. Never put a key in `env`; keys come from the
source's `api_key_envs`.

**Baselines** (sources configured with `baseline=true`), before iteration 0:

```
rsi_trigger_downstream_eval(workspace_dir, source="llm-judge", stage="baseline")
rsi_trigger_downstream_eval(workspace_dir, source="sft-math", stage="baseline",
                            env={"CUDA_VISIBLE_DEVICES": "0,1"})
rsi_get_downstream_result(workspace_dir)   # no job_id: polls every unfinished job
```

A `failed` baseline is infrastructure, not a data signal: tell the user and let
them choose between retrying and `rsi_configure_feedback(source=..., baseline=false)`.

**Periodic evals** (default `stage="periodic"`, evaluate the incumbent): after
each completed round, compare its one-based count against each source's agreed
interval in `experiment_plan.json`. Trigger each due source once, including
after rejected rounds when an incumbent exists. The calls below illustrate
sources due at the same checkpoint; stagger them if their combined API workers
would exceed the confirmed endpoint cap:

```
rsi_trigger_downstream_eval(workspace_dir, source="llm-judge")
rsi_trigger_downstream_eval(workspace_dir, source="sft-math",
                            env={"CUDA_VISIBLE_DEVICES": "0,1"})
rsi_get_downstream_result(workspace_dir)   # every 60-120s
  -> {status: "running", unfinished: [...], jobs: [...]}   # keep polling
  -> {status: "done", jobs: [...]}                         # all read: write the next pipeline
```

Each entry in `jobs` is one source's result: `ready` (feedback below), `failed`
(tell the user; do not change the pipeline because of it) or still running.
`rsi_get_downstream_result(workspace_dir, job_id)` reads one job again.

Reading a `ready` result (per source; never add up scores of different sources):
- `scores` / `benchmarks` — the measured numbers. `delta_previous` and
  `delta_baseline` are computed by the harness against the same source's
  previous result and baseline (deltas a script reports are ignored), positive
  means better, and only between results with the same script, scores and
  sampling; otherwise `comparability` says why they are not comparable.
- `script_analysis` — the script's own `summary` / `recommended_pipeline_changes`,
  present only if it gave them. Treat them as hints.
- `attribution` — `status: "ok"` is a data-side diagnosis (findings with
  `data_side`, actions, `recommended_pipeline_changes`; act only on data-side
  actions). `no_bad_cases`, `not_configured` and `failed` mean there is no
  diagnosis: scores and deltas stand alone, so do not invent one.
- These numbers are not review scores and never change the incumbent. When
  sources disagree, report that to the user rather than silently choosing one.
- Raw bad cases are audit-only. Do not open `result.json` or `attribution.json`
  under `downstream_jobs/<job_id>/` (they hold benchmark questions and answers);
  what you were returned is the feedback. Improve the raw-data pipeline from
  cross-case patterns and never hardcode a benchmark question or answer.

---

## Stopping and export

Stop when the agreed budget is exhausted, an early-stop condition is met,
or the user asks to stop.

Consume any already-started downstream jobs and evaluations due at the final
round; perform an extra final evaluation only if agreed in the plan. Then call
`rsi_export_dataset` to copy the best dataset to a clean output path. If there
is no incumbent, report that no dataset can be exported. Report to the user:
- Stopping reason and the agreed iteration budget
- Final incumbent score
- Number of iterations run
- Path to the exported dataset
- Key quality improvements achieved

---

## Common pitfalls

- **Do not fetch external datasets** inside pipeline operators
- **Do not run pipeline.py yourself** — always go through `rsi_submit_pipeline`
- **Do not skip downstream results** — poll `rsi_get_downstream_result` until every job is `ready` or `failed` (then ask the user) before the next iteration; never open raw `result.json` / `attribution.json`
- **Empty write raises** — handle zero-row cases explicitly in operators
- **Type drift** — `storage.read("dict")` columns may be float even if source was int
