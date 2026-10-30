# DataFlow-Evolve 快速开始

## 概述

DataFlow-Evolve 是一个面向 Claude Code 和 Codex 的数据集迭代优化框架。用户描述数据目标和质量要求，Agent 编写数据处理 pipeline，框架执行并评审结果，Agent 再根据诊断和可选的下游评测反馈改进下一轮，最终导出当前最佳数据集。

- **Agent**：与用户确认实验计划，编写 pipeline、领域 skill 和评测脚本，分析反馈并决定下一轮改进。
- **框架**：通过 MCP 工具执行 pipeline、评审数据、记录迭代历史、管理下游评测任务和导出结果。
- **Skills**：为 Agent 提供实验编排和 pipeline 编写规范。

## 图片与视频

支持文本、图片及视频引用的 JSONL 数据集。图片直接送入视觉模型，视频按配置抽帧，pipeline 和质量评审均可使用；视频音频和原生视频 API 需专用适配器。视觉任务需选择支持图文消息的 pipeline / review 模型。

安装时增加 `multimodal` 依赖：`python -m pip install -e '.[review,multimodal]'`；视频还需安装 FFmpeg 和 ffprobe。Agent 会与用户确认媒体字段、抽帧及评审预算，再初始化实验。导出包含 JSONL 和媒体目录，二者可一起迁移。

字段格式、初始化参数与调用示例见[多模态说明](../skills/claude/dataflow-evolver-pipeline/references/multimodal.md)。

## 安装

使用 Python 3.10+，在运行 Claude Code / Codex 的机器上执行：

```bash
cd /path/to/DataFlow-Evolve
python -m pip install -e '.[review]'
python install.py --target claude   # 可选：codex / both；默认 both
```

安装器复制两个 skill（`rsi-orchestrator`、`dataflow-evolver-pipeline`），并注册 MCP 服务 `dataflow-evolve`。默认位置：

| 目标 | skills | MCP 配置 |
|---|---|---|
| Claude Code | `~/.claude/skills/` | `~/.claude.json`（未设置 `CLAUDE_CONFIG_DIR` 时） |
| Codex | `~/.codex/skills/` | `~/.codex/config.toml` |

MCP 和 pipeline 默认使用运行安装器的 Python；可通过 `--python /path/to/python` 指定已安装依赖的解释器。安装后请保留仓库路径，MCP 直接加载其中的代码。

### 指定 Claude Code

```bash
python install.py --target claude \
  --claude-bin /path/to/bin/claude \
  --claude-config-dir /path/to/.claude
```

`--claude-bin` 选择可执行文件，`--claude-config-dir` 选择配置目录；skills 写入该目录的 `skills/`，MCP 写入 `.claude.json`。启动目标实例时使用同一配置目录：

```bash
CLAUDE_CONFIG_DIR=/path/to/.claude /path/to/bin/claude
```

多个实例通过配置目录隔离。使用自定义 `CLAUDE_CONFIG_DIR` 时，请显式传入 `--claude-config-dir`，确保 skill 和 MCP 安装到同一处。

### 指定 Codex

```bash
python install.py --target codex --codex-home /path/to/.codex
```

启动 Codex 时使用相同的 `CODEX_HOME`；安装器需显式传入 `--codex-home`。已有同名 MCP 配置时不会覆盖，切换仓库或 Python 路径后需更新配置。

### 验证

重新启动目标会话，在 `/mcp` 中确认 `dataflow-evolve` 已连接。Claude Code 也可通过 `claude mcp list` 检查；自定义实例需使用对应的可执行文件和 `CLAUDE_CONFIG_DIR`。

添加 `--print-config` 可只复制 skills 并打印注册命令或配置片段，供手动注册。

## 使用流程

用户可以直接对 Agent 说：


> 帮我优化 `/path/to/input.jsonl` 中的 Math CoT 训练数据，提高解答正确性和完整性，用于对 Qwen2.5-7B Base 模型进行 SFT 训练，提升其 CoT 能力。
>
> 输入行包含来源元数据、题目、标准解答（`golden_solution`）以及标准答案（`golden_answer`）。使用题目作为数学提示词，并且必须保证答案与标准答案一致。最终训练数据严格只保留 `instruction`（指令）和 `output`（输出）两个字段。
>
> 每条最终样本必须满足以下要求：
>
> 1. 包含非空的 `instruction` 字段，填写数学题目。
> 2. 包含正确、自完备的 `output` 字段，需要：
>
>    - 明确梳理题目的核心条件；
>    - 用到对应的定理或公式，给出分步推导；
>    - 展示计算过程，对关键中间结果做核验；
>    - 最终答案用 LaTeX `\boxed{}` 显式包裹。
>
> 3. 样本互不重复、数学逻辑正确，适用于监督微调。

1. **确认计划**：Agent 与用户确认数据路径、输出格式、质量标准、模型和 API、并发数、迭代轮次及停止条件，并询问是否添加领域 skill、是否接入下游反馈；每个反馈源单独确认反馈间隔。
2. **准备工作区**：用户确认后初始化 workspace，检查执行环境，登记领域 skill 和反馈源；如需 baseline，先完成基线评测。
3. **编写并执行**：Agent 分析输入数据，编写 `pipeline.py`、算子和设计说明，通过 `rsi_submit_pipeline` 提交执行和质量评审。
4. **诊断与改进**：每次实际执行后，由独立上下文的临时子 Agent 诊断结果；主体 Agent 根据诊断、评审和按约定间隔获得的下游反馈修复或设计下一轮。
5. **结束并导出**：达到约定的停止条件或迭代上限后，导出当前最佳数据集，并汇报质量、产量和仍存在的问题。

### Phase 0 — 确认计划并初始化

Agent 先整理上述配置供用户确认，再调用 `rsi_init` 创建 workspace，并用 `rsi_check_environment` 检查需要的依赖。用户提供或同意创建的领域 skill 在实验开始前登记，后续编写 pipeline 时读取。

### Phase 0.5 — 下游反馈（可选，在第一轮 pipeline 之前）

不要下游反馈就跳过这一节，只靠 criteria review 迭代。

下游反馈由任意多个**反馈源**（`source`）组成，每个源是一个评测脚本加它报告的分数。用户可能同时想要几种信号，例如：

- `llm-quality`：不训练，用 LLM API 给 incumbent 数据打质量分（只需要 API）；
- `sft-math`：在 incumbent 上做 SFT，再评测 MATH-500（需要 GPU）。

Agent 对每个源调一次 `rsi_configure_feedback(source=<名字>)`。它一次返回该源所有没回答的问题（`needs_input`），再给一份预览（`needs_confirmation`），Agent 转述给用户，用户同意后 `confirm=true` 保存。改一个源不影响其他源；`remove=true` 移除一个源。有未读取 job 的源不能改、不能删。要问清的事：

1. **评测脚本 `command`**，两种来源：
   - 用户已有的脚本：把运行它的命令作为 argv 列表交给 `command`（带上正确的 Python 环境，建议写绝对路径）；
   - 实验前与 Agent 对话得到：Agent 问清基座模型、训练配方、benchmark 数据、打分方式、用 API 还是 GPU，调 `rsi_generate_eval_script` 得到协议正确的骨架（写进 workspace 的 `eval_scripts/`，不调用 LLM），自己实现其中的 TODO，小规模试跑，给用户确认后再注册。
2. **`trains_model`**：periodic 评测是否先在 incumbent 上训练再评测；`false` 表示只评测数据本身，不训练（是否仍要用 GPU 由 `resources` 说明）。
3. **`benchmarks` 与采样**：这个源报告的分数名（benchmark 或自定义指标）；`full` 或 `subset`（`subset_size`、`sampling_seed`）。整个实验期间保持不变。
4. **`baseline`**：要不要在第 0 轮前评测一次起点。训练源评测未训练的基座，直接评分的源评测原始输入语料。多一次评测的代价，换来该源每个结果的 `delta_baseline` 和第一轮的起点诊断。
5. **`attribution`**：要不要把这个源脚本返回的 bad case 交给独立的归因 LLM（自己的 URL、model、key 环境变量名），变成数据侧诊断。不用也可以，分数、分差和脚本自己的分析就是反馈。
6. **`resources` 与 `api_key_envs`**：一次运行需要什么（“只要 API”“2 张 40GB GPU”），供 Agent 规划并行；脚本需要的 key 的环境变量名，值从凭据文件或服务进程环境注入，不落盘。

**脚本协议**：harness 在 workspace 目录下以 `<command> --request <request.json> --result <result.json>` 调用；argv 里含 `{request}` / `{result}` 时按占位符替换。

```json
// request.json（节选）
{"source": "sft-math", "stage": "periodic", "training_performed": true, "dataset_path": "...",
 "expected_benchmarks": ["math500"], "sampling_mode": "subset",
 "subset_size": 500, "sampling_seed": 42, "bad_cases_per_benchmark": 9}

// result.json（必需的只有 status 和每个 benchmark 的 score）
{"status": "ok", "model": "...",
 "benchmarks": {"math500": {"score": 0.68, "metric": "accuracy",
                           "bad_cases": [{"question": "...", "model_response": "..."}]}},
 "summary": "可选：脚本自己的分析", "recommended_pipeline_changes": ["可选"]}
```

`stage="baseline"` 时脚本评测起点：训练源的 `dataset_path` 为 null，跳过训练直接评测基座；直接评分的源拿到的是原始输入语料。脚本自己算的分差会被忽略。`training_performed` 仅在 `stage=periodic` 且 `trains_model=true` 时为 true。

有源要求了 baseline 时，写第 0 轮之前先跑，各源可以按资源并行：

```python
rsi_trigger_downstream_eval(workspace_dir, source="llm-quality", stage="baseline")
rsi_trigger_downstream_eval(workspace_dir, source="sft-math", stage="baseline",
                            env={"CUDA_VISIBLE_DEVICES": "0,1"})
rsi_get_downstream_result(workspace_dir)   # 不传 job_id：轮询所有未完成 job，直到 status 为 done
```

还有源没有 baseline 结果时，`rsi_submit_pipeline` 返回 `downstream_baseline_pending`（列出这些源）。baseline 失败是基础设施问题，不是数据信号：告诉用户，由用户决定重试，还是 `rsi_configure_feedback(source="sft-math", baseline=false)` 放弃。

### Phase 1 — 写第一版 pipeline

Agent 会：
1. 读取输入数据前 50 行，分析 schema、语言、重复、缺失
2. 设计 operators（每个单一职责）
3. 写入 `first_iteration_dir/`：
   - `pipeline.py`
   - `operators/MyOp.py`
   - `decision.json`

**关键约束**：
- 只 import `dataflow_evolve`（不是 `dataflow`）
- 使用 `FrameworkStepCache.from_env` 处理缓存
- 主入口必须是 `pipeline.compile() → DF_COMPILE_ONLY 守卫 → pipeline.forward()`

### Phase 2 — 提交并解读

Agent 调用 `rsi_submit_pipeline(workspace_dir, iteration_dir)`。

返回：
```json
{
  "review_score": 0.67,
  "passed": true,
  "issues": [],
  "domain_feedback": {"review_mode": "criteria", "criteria_met_rate": 0.67,
                      "criteria_assessments": [{"criterion_index": 2, "met": false,
                                                "evidence": "...", "suggestion": "..."}],
                      "critical_failures": []},
  "incumbent_updated": true,
  "step_reuse": {"reused_operators": 1, "total_operators": 3, "reason": "..."},
  "evolution_history": {"entries": [...]},
  "next_iteration_dir": ".../iteration_002"
}
```

- 若 `failure_stage` 非空且有 `repair_feedback` → 读 traceback 和诊断，在同一个目录修复后重新提交
- 每次实际执行后调用 `rsi_diagnose_pipeline`，将返回的任务包交给独立上下文的临时子 Agent；等待诊断并保存到 `output_path`，再修复、继续迭代或导出
- 若 `passed=false` → 分析 `issues` 和 `criteria_assessments`，设计改进，写入 `next_iteration_dir`

### Phase 3 — 迭代

每轮：
1. （新会话或对话被压缩后，先调 `rsi_get_status`，它从磁盘重建全部状态）
2. 分析上一轮 review、`domain_feedback` 和 `evolution_history`，不要重复已被拒绝的策略
3. 设计一个目标明确的改进（不要一次改所有），基于 incumbent；保持前面没变的算子完全一致，harness 才能复用它们的 step 缓存
4. 写新 pipeline 到 `next_iteration_dir`
5. `rsi_submit_pipeline`

**进化历史**：`rsi_submit_pipeline` 和 `rsi_get_status` 都返回 `evolution_history`（最近 12 轮：结果、分数、算子、设计理由、前两条问题；被拒轮次附执行观测和已存档的诊断）。每次 submit / review 另写 workspace 根目录的 `evolution_history.json` 快照供审计；`rsi_get_status` 的结果现算，是权威。诊断存进 `pipeline_diagnostic.json` 后，`rsi_get_status` 立即能看到，快照在下一次 submit 或 review 时刷新。

**下游评估规则（关键）**：

如果配置了反馈源，Agent 按实验前确认的各源反馈间隔触发评测，并在约定的资源和 API 并发范围内安排并行：

```python
# 触发异步评估（默认 stage="periodic"，评测当前 incumbent）；每个源一次调用
rsi_trigger_downstream_eval(workspace_dir, source="llm-quality")
  -> {"job_id": "a1", "status": "running", "source": "llm-quality", "stage": "periodic"}
rsi_trigger_downstream_eval(workspace_dir, source="sft-math",
                            env={"CUDA_VISIBLE_DEVICES": "0,1"})
  -> {"job_id": "b2", "status": "running", "source": "sft-math", "env": {...}}

# 轮询（每 60-120 秒）：不传 job_id，一次查所有未完成的 job
rsi_get_downstream_result(workspace_dir)
  -> {"status": "running", "unfinished": ["b2"], "jobs": [...], "missing_baselines": []}
  -> {"status": "done", "jobs": [...]}   # 每个 job 是 ready 或 failed

# 只有所有 job 都读到 ready（或用户处理完 failed）后才能写下一轮 pipeline
```

- **并行由 Agent 决定**：harness 不设并发上限。触发前先看各源的 `resources`（`rsi_get_status`）、空闲设备（如 `nvidia-smi`）和在跑的 job（`downstream_jobs`，每个带它拿到的 `env`）。GPU job 通过 `env` 指定设备，不要把同一批设备给两个 GPU job，除非用户说它们放得下；只用 API 的源可以和 GPU job 同时跑。`env` 里不要放 key，key 走源的 `api_key_envs`。
- 同一个源、同一 stage、同一 incumbent 的相同评测已在跑时，返回 `already_running` 和原 `job_id`，不会重复启动。
- 单独读某个 job：`rsi_get_downstream_result(workspace_dir, job_id)`。

单个 job 的 `ready` 结果（每个源一份）：

```json
{
  "job_id": "b2", "status": "ready", "source": "sft-math", "stage": "periodic",
  "scores": {"math500": 0.68},
  "delta_previous": {"math500": 0.02},
  "delta_baseline": {"math500": 0.12},
  "comparability": {"delta_previous": "vs job ...", "delta_baseline": "vs job ..."},
  "script_analysis": {"summary": "...", "recommended_pipeline_changes": ["..."]},
  "attribution": {"status": "ok", "summary": "...", "findings": [...]},
  "warnings": []
}
```

- `delta_previous` / `delta_baseline` 由 harness 算，正数表示变好；只在同一个源、同一协议指纹（脚本命令、`trains_model`、benchmarks、采样）的结果之间计算，否则 `comparability` 说明不可比。
- `script_analysis` 是脚本自己的分析，只在脚本给了才有，当作提示。
- `attribution.status` 为 `ok` 时是数据侧诊断；`no_bad_cases`、`not_configured`、`failed` 时没有诊断，分数和分差本身就是反馈。
- 这些分数不是 review 分数，不改变 incumbent；不同源的分数不会合并成一个数，几个源的信号不一致时，把分歧告诉用户，不要悄悄选一个。
- 原始 bad case 只供人工审计：不要打开 `downstream_jobs/<job_id>/` 下的 `result.json` 和 `attribution.json`，也不要把 benchmark 的题目和答案写进数据。

**不可跳过下游结果**：有 job 处于 pending / running 时，`rsi_submit_pipeline` 返回 `downstream_eval_running`（列出这些 job）。job 在首次读取结果时才标记为 `ready`，所以 `ready` 一定已被读取。`failed` 的 job 是基础设施问题：告诉用户，不要据此改 pipeline。

### Phase 4 — 导出

达到约定的停止条件或迭代上限后，调用 `rsi_export_dataset(workspace_dir)` 导出当前最佳数据集，并汇报评审结果和已知问题。

---

## 13 个 MCP 工具速查

| 工具 | 作用 | 关键参数 |
|---|---|---|
| `rsi_check_environment` | 检查 pipeline 执行环境的依赖 | modules, timeout_sec |
| `rsi_init` | 初始化 workspace | task_description, input_path, API 配置 |
| `rsi_add_domain_skill` | 登记 Agent 写的垂域 skill（不调用 LLM） | name, description, body |
| `rsi_configure_feedback` | 配置一个反馈源（脚本、分数名、采样、baseline、归因、资源） | source, command, trains_model, benchmarks, sampling_mode, baseline, attribution, resources, api_key_envs, confirm, remove |
| `rsi_generate_eval_script` | 可选：创建评测脚本骨架并返回协议（Agent 自己实现 TODO，不调用 LLM） | name, description, trains_model, overwrite |
| `rsi_submit_pipeline` | 提交 pipeline 执行 + review | workspace_dir, iteration_dir |
| `rsi_review_iteration` | 对已生成的数据重新 review，不重跑 pipeline | workspace_dir, iteration_dir |
| `rsi_diagnose_pipeline` | 返回只读诊断 prompt，结论存档 | workspace_dir, iteration_dir |
| `rsi_trigger_downstream_eval` | 触发某个源的异步评测（baseline 或 incumbent） | workspace_dir, source, stage, env |
| `rsi_get_downstream_result` | 读一个 job，或不传 job_id 轮询所有未完成 job；返回分数、分差、归因 | workspace_dir, job_id（可选） |
| `rsi_get_status` | 现算 workspace 状态（含进化历史、反馈源、下游 job） | workspace_dir |
| `rsi_export_dataset` | 导出 incumbent 数据集 | workspace_dir, output_path |
| `rsi_set_credential_env` | 修改读取 key 的环境变量名 / 凭据文件 | workspace_dir, *_api_key_env, credentials_file |

---

## 关键设计约束

1. **宿主管理诊断子 Agent**：harness 准备任务包，主体 Agent 启动独立上下文的临时子 Agent，等待并保存结果
2. **按质量标准评审**：逐项检查用户约定的 criteria
3. **异步评估 + 强制消费**：有 job 未读完、或有源要求了 baseline 却没有结果时，新 pipeline 被拒绝
4. **不设评测并发上限**：并行与设备放置由 Agent 按资源决定
5. **Timeout 用户控制**：不配置则无 timeout；评测的 timeout 按源设置，`0` 表示不限
6. **下游配置对话式确认**：`rsi_configure_feedback` 一次问全，用户确认后才保存
7. **反馈各层可选**：分数 → 分差 → 脚本分析 → 归因，缺哪一层都不影响其余各层
8. **生成不调用 LLM**：skill 和评测脚本由 Agent 结合对话自己写

---

## 常见问题

**Q: pipeline.py 编译失败怎么办？**

A: 先看 `rsi_submit_pipeline` 返回的 `repair_feedback`（traceback 和逐步诊断），在同一个 iteration 目录修复后重新提交。

**Q: 下游评估一直 running 怎么办？**

A: 持续轮询 `rsi_get_downstream_result`。超过预期时间（如 2 小时）仍未结束，检查 `workspace_dir/downstream_jobs/<job_id>/stderr.log`。评测进程退出却没写 `result.json`、或超过该源设的 `timeout_sec`，job 会被标为 `failed`。

**Q: 能跳过下游评估写下一轮吗？**

A: 不能。有 job 处于 pending / running 时，`rsi_submit_pipeline` 返回 `downstream_eval_running`；有源要求了 baseline 却没有结果时返回 `downstream_baseline_pending`。`failed` 是基础设施问题：告诉用户，由用户决定重试还是放弃。

**Q: 能同时跑多个评测吗？**

A: 能，也是预期用法：不同的源（比如 API 打分和 GPU 训练评测）各自触发各自的 job，harness 不限制数量。是否同时跑、各用哪几张 GPU 由 Agent 根据 `resources` 和空闲设备判断；只有完全相同的在跑 job 才会被去重。

**Q: 我已有评测脚本，怎么接入？**

A: 把运行脚本的 argv 作为 `command` 传给 `rsi_configure_feedback`（一个脚本对应一个 `source`）。脚本只要接受 `--request` / `--result`（或在 argv 里用 `{request}` / `{result}`），并写出带每个分数名 `score` 的 `result.json`。只支持本仓库的统一协议。

**Q: 评测要调用 LLM API，key 怎么给？**

A: 在源的 `api_key_envs` 里写环境变量名，harness 从凭据文件或服务进程环境取值，只注入这个源的评测进程，不写进 workspace，也不要放进触发时的 `env`。

**Q: 没有 GPU，或评测里没有 bad case，还能用吗？**

A: 能。`trains_model=false` 的源只评测数据本身，不训练。`bad_cases` 是可选的，没有就没有归因（`no_bad_cases`），分数和分差照常返回。

**Q: 对话被压缩或换了新会话，怎么恢复？**

A: 调 `rsi_get_status`，它从磁盘重建 incumbent、迭代历史、`evolution_history`、反馈源配置和 job。

**Q: 中途能改某个源的 benchmark 或评测脚本吗？**

A: 能，但只影响这个源：改完后它的结果和之前的不可比，`comparability` 会说明，该源的分差不再计算，其他源不受影响。

**Q: 旧版本创建的 workspace 还能用吗？**

A: 不能。workspace schema 版本为 `1`，读到其他版本会报错并提示用 `rsi_init` 新建，不做迁移。

---

## 下一步

- 阅读 `skills/claude/rsi-orchestrator/SKILL.md` 完整 orchestration 流程
- 阅读 `skills/claude/dataflow-evolver-pipeline/SKILL.md` pipeline 编写规范
- 运行一个真实任务，观察 Agent 如何 orchestrate RSI 循环

### 执行环境检查

`rsi_init` 和 `rsi_get_status` 返回 `runtime`：实际 Python、项目根目录、运行时目录及 serving 源码路径。`rsi_check_environment(modules=["dataflow_evolve", "transformers"])` 在 pipeline 的 Python/PYTHONPATH 下逐项检查导入，不安装依赖。生成 pipeline 使用返回的 `pipeline_llm` 配置。
