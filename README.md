# DataFlow-Evolve

DataFlow-Evolve 是一个面向 Claude Code 和 Codex 的数据集迭代优化框架。用户描述数据目标和质量要求，Agent 编写数据处理 pipeline，框架执行并评审结果，Agent 再根据诊断和可选的下游评测反馈改进下一轮，最终导出当前最佳数据集。

- **Agent**：与用户确认实验计划，编写 pipeline、领域 skill 和评测脚本，分析反馈并决定下一轮改进。
- **框架**：通过 MCP 工具执行 pipeline、评审数据、记录迭代历史、管理下游评测任务和导出结果。
- **Skills**：为 Agent 提供实验编排和 pipeline 编写规范。

## 运行流程

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

Agent 按以下流程执行：

1. **确认计划**：Agent 与用户确认数据路径、输出格式、质量标准、模型和 API、并发数、迭代轮次及停止条件，并询问是否添加领域 skill、是否接入下游反馈；每个反馈源单独确认反馈间隔。
2. **准备工作区**：用户确认后初始化 workspace，检查执行环境，登记领域 skill 和反馈源；如需 baseline，先完成基线评测。
3. **编写并执行**：Agent 分析输入数据，编写 `pipeline.py`、算子和设计说明，通过 `rsi_submit_pipeline` 提交执行和质量评审。
4. **诊断与改进**：每次实际执行后，由独立上下文的临时子 Agent 诊断结果；主体 Agent 根据诊断、评审和按约定间隔获得的下游反馈修复或设计下一轮。
5. **结束并导出**：达到约定的停止条件或迭代上限后，导出当前最佳数据集，并汇报质量、产量和仍存在的问题。

## 仓库结构

```
DataFlow-Evolve/
├── rsi_mcp/
│   └── server.py                # MCPServer（MCP SDK 2.x）stdio server，13 个 rsi_* 工具
├── harness/
│   ├── workspace.py             # 工作区状态：迭代、反馈源、下游 job
│   ├── executor.py              # compile → execute（step 复用）→ review → incumbent
│   ├── execution_observation.py # 逐步执行观测（行数、保留率、字段分布、样本）
│   ├── reviewer.py              # criteria mode review + 硬指标
│   ├── diagnostic.py            # 独立诊断任务包
│   ├── history.py               # 进化历史与 evolution_history.json 快照
│   ├── downstream.py            # 多反馈源的异步评测 job、结果校验、分差
│   ├── attribution.py           # 可选的 bad case 归因
│   ├── scaffold.py              # Agent 编写 skill / 评测脚本的脚手架（不调用 LLM）
│   └── telemetry/               # pipeline LLM 调用记录
├── skills/
│   ├── claude/rsi-orchestrator/SKILL.md
│   ├── codex/rsi-orchestrator/SKILL.md
│   ├── claude/dataflow-evolver-pipeline/   # pipeline 编写规范（随仓库自带）
│   └── codex/dataflow-evolver-pipeline/
├── dataflow_evolve/                # 生成的 pipeline 所 import 的运行时（OperatorABC、PipelineABC、FileStorage 等）
├── install.py                   # 一键安装到 Claude Code / Codex
├── LICENSE                      # Apache-2.0
├── pyproject.toml
└── docs/quickstart.md
```

## MCP 工具

| 工具 | 作用 |
|---|---|
| `rsi_check_environment` | 检查 pipeline 执行环境中的 Python 模块及其导入结果 |
| `rsi_init` | 从自然语言初始化 RSI workspace；缺字段返回 `needs_input` 让 Agent 转问用户 |
| `rsi_add_domain_skill` | 登记 Agent 自己写的领域 skill（`name`、`description`、`body`）；不带 `body` 时返回写作指引；不调用 LLM |
| `rsi_configure_feedback` | 配置一个命名反馈源（`source`）：脚本 `command`、`trains_model`、`benchmarks`、采样、`baseline`、`attribution`、`resources`、`api_key_envs`。先一次问全（`needs_input`），再给预览（`needs_confirmation`），用户确认后 `confirm=true` 保存；每次只改一个源，`remove=true` 移除 |
| `rsi_generate_eval_script` | 在 workspace 的 `eval_scripts/` 写一份协议正确的脚本骨架（`name`、`description`、`trains_model`）并返回协议；不调用 LLM，TODO 由 Agent 自己实现；用户已有脚本时不需要它 |
| `rsi_submit_pipeline` | 接收 iteration 目录，执行 compile + execute + criteria review + incumbent 比较；有 job 未读完、或有源要求了 baseline 却没有结果时拒绝 |
| `rsi_review_iteration` | 对已生成的数据重新 review，不重跑 pipeline；用于 review 失败后补评分，或在用户要求时重新评分 |
| `rsi_diagnose_pipeline` | 返回独立诊断任务包（逐步执行观测 + review 证据），主体 Agent 委派独立上下文的临时子 Agent 诊断，等待结果后存档 |
| `rsi_trigger_downstream_eval` | 启动某个源的异步评测 job：`source`、`stage`（`periodic` 默认，评测 incumbent；`baseline` 评测起点）、`env`（设备等非敏感放置参数，如 `CUDA_VISIBLE_DEVICES`）。不设并发上限，只对完全相同的在跑 job 返回 `already_running` |
| `rsi_get_downstream_result` | 读单个 job（`job_id`），或不传 `job_id` 批量轮询所有未完成 job；首次读到结果时校验、计算分差、（可选）归因，返回反馈；不返回原始 bad case |
| `rsi_get_status` | 现算整个状态：incumbent、迭代、`evolution_history`、反馈源配置、下游 job。新会话或上下文压缩后先调它 |
| `rsi_export_dataset` | 导出当前 incumbent 数据集到指定路径 |
| `rsi_set_credential_env` | 修改 workspace 读取 API key 的环境变量名（review / pipeline / attribution）或凭据文件路径（不存 key 值） |

## 图片与视频

支持文本、图片及视频引用的 JSONL 数据集。图片直接送入视觉模型，视频按配置抽帧，pipeline 和质量评审均可使用；视频音频和原生视频 API 需专用适配器。视觉任务需选择支持图文消息的 pipeline / review 模型。

安装时增加 `multimodal` 依赖：`python -m pip install -e '.[review,multimodal]'`；视频还需安装 FFmpeg 和 ffprobe。Agent 会与用户确认媒体字段、抽帧及评审预算，再初始化实验。导出包含 JSONL 和媒体目录，二者可一起迁移。

字段格式、初始化参数与调用示例见[多模态说明](skills/claude/dataflow-evolver-pipeline/references/multimodal.md)。

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

## 下游反馈（可选）

不需要下游反馈就不用配置，只靠 criteria review 迭代。需要时，下游反馈由任意多个命名的**反馈源**组成：每个源是一个评测脚本加它报告的分数。例如 `llm-quality`（调用 LLM API 给数据打质量分，只需要 API）和 `sft-math`（在 incumbent 上 SFT 后评测 MATH-500，需要 GPU）。

每个源独立配置（`rsi_configure_feedback`，每次一个 `source`，改一个不影响其他）：
- `command`：评测脚本的 argv。
- `trains_model`：periodic 评测是否先在 incumbent 数据上训练；`false` 表示只评测数据本身，不训练（是否仍要用 GPU 由 `resources` 说明）。
- `benchmarks`：这个源报告的分数名，可以是 benchmark，也可以是自定义指标；`sampling_mode` / `subset_size` / `sampling_seed` 固定评测样本。
- `baseline`：是否在第 0 轮前评测一次起点。
- `attribution`：是否对它的 bad case 做归因。
- `resources`：一次运行需要什么（“只要 API”“2 张 40GB GPU”），供 Agent 规划并行。
- `api_key_envs`：脚本需要的 key 的环境变量名，值从凭据文件或服务进程环境注入，不落盘。

**评测脚本有两种来源，用同一套协议**
- 用户指定：已有脚本，把运行它的 argv 交给 `rsi_configure_feedback`。
- 实验开始前与 Agent 对话得到：Agent 问清基座模型、训练配方、benchmark 数据、打分方式、用 API 还是 GPU，调 `rsi_generate_eval_script` 拿到骨架和协议，自己实现，小规模试跑，给用户确认后再注册。harness 不调用 LLM 来写脚本。

**脚本协议**：harness 在 workspace 目录下以 `<command> --request <job_dir>/request.json --result <job_dir>/result.json` 调用；argv 里含 `{request}` / `{result}` 时按占位符替换。
- request：`source`、`stage`（`baseline`：评测起点，训练源评测未训练的基座，`dataset_path` 为 null，直接评分的源评测原始输入语料；`periodic`：评测 incumbent 数据）、`training_performed`、`expected_benchmarks`、`sampling_mode` / `subset_size` / `sampling_seed`、`bad_cases_per_benchmark`、`task` 等。
- result：`status`、`model`、`benchmarks.<name>.score`（必需，越高越好）、可选的计数和 `bad_cases`、可选的 `summary` 与 `recommended_pipeline_changes`（脚本自己的分析）、`artifacts`，失败时写 `error`。
- 脚本里自己算的 `score_deltas` / `delta_baseline` 会被忽略，分差由 harness 算。
- `training_performed` 仅在 `stage=periodic` 且 `trains_model=true` 时为 true。

**并行由 Agent 决定**：harness 不设并发上限。Agent 看各源的 `resources`、空闲设备（如 `nvidia-smi`）和已在跑的 job，自行决定哪些源同时跑；GPU 的放置通过触发时的 `env` 给出，API key 不放进 `env`。

```python
rsi_trigger_downstream_eval(workspace_dir, source="llm-quality")
rsi_trigger_downstream_eval(workspace_dir, source="sft-math",
                            env={"CUDA_VISIBLE_DEVICES": "0,1"})
rsi_get_downstream_result(workspace_dir)   # 不传 job_id：轮询所有未完成 job
```

**反馈分层，每一层都可选**

| 脚本提供 | harness 做什么 | Agent 看到什么 |
|---|---|---|
| 只有分数 | 计算分差 | `scores` + `delta_previous` / `delta_baseline` |
| 分数 + 自己的分析 | 原样透传 | 再加 `script_analysis` |
| 分数 + bad case，且该源开了归因 | 调用归因 LLM 并校验结果 | 再加 `attribution` 报告 |
| 分数 + bad case，没开归因 | 不调用 | `attribution.status = "not_configured"` |

没有 bad case 时是 `no_bad_cases`；归因 LLM 失败时是 `failed`，说明是基础设施问题。这三种都不影响分数和分差，也不会编造诊断。归因 LLM 单独配置（URL、model、key 环境变量名），开了归因的源共用它。分数和分差不是 review 分数，不改变 incumbent，不同源的分数不会合并成一个数。

**Baseline（可选，按源选择）**：实验开始前确认每个源要不要评测一次起点。需要时同一个脚本用 `stage="baseline"` 跑，各源的 baseline 可以按资源并行，在第一轮 pipeline 之前全部读完，之后该源的每个结果都有 `delta_baseline`。某个源的 baseline 失败时由用户决定重试，还是对该源 `baseline=false` 放弃。

**可比性**：每个源有自己的协议指纹（脚本命令、`trains_model`、benchmarks、采样设置）。分差只在同一个源、同一指纹的结果之间计算，中途改过则 `comparability` 说明“不可比”，不硬算。

**隔离**：原始 bad case 只留在 `downstream_jobs/<job_id>/` 里供人工审计，返回值里没有。Agent 不应打开其中的 `result.json` / `attribution.json`，只读返回的反馈（同目录的 `feedback.json` 是同一份），避免把 benchmark 的题目和答案写进数据。这靠约定，不是文件系统隔离。

## 关键设计约束

- **宿主管理诊断子 Agent**：harness 返回诊断任务包，主体 Agent 通过宿主原生工具启动独立上下文的临时子 Agent，等待并保存结果
- **按质量标准评审**：逐项检查用户约定的 criteria 并返回证据和改进建议
- **异步评估 + 强制消费**：有 job 处于 pending / running 时，`rsi_submit_pipeline` 返回 `downstream_eval_running`（列出这些 job）；有源要求了 baseline 却没有读到结果时返回 `downstream_baseline_pending`（列出这些源）。job 在首次读取结果时才标记为 `ready`，所以 `ready` 一定已被读取
- **不设评测并发上限**：并行与放置由 Agent 按资源决定；harness 只去重完全相同的在跑 job
- **Timeout 用户决定**：不设则 subprocess 无 timeout；评测的 timeout 按源设置，`0` 表示不限
- **下游配置对话式确认**：`rsi_configure_feedback` 一次问全，用户确认后才保存
- **生成不调用 LLM**：`rsi_add_domain_skill` 和 `rsi_generate_eval_script` 只提供骨架、协议并登记，内容由 Agent 结合对话自己写
- **进化历史不靠对话记忆**：`rsi_get_status` 现算；每次 submit / review 另写 `evolution_history.json` 快照供审计
- **workspace schema 版本为 `1`**：读到其他版本会报错并提示用 `rsi_init` 新建，不做迁移

### 执行环境检查

`rsi_init` 和 `rsi_get_status` 返回 `runtime`：实际 Python、项目根目录、运行时目录及 serving 源码路径。`rsi_check_environment(modules=["dataflow_evolve", "transformers"])` 在 pipeline 的 Python/PYTHONPATH 下逐项检查导入，不安装依赖。生成 pipeline 使用返回的 `pipeline_llm` 配置。
