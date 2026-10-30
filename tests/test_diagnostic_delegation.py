import asyncio
import json
import os

from harness.diagnostic import build_diagnostic_context
from harness.workspace import Workspace, WorkspaceState, IterationSummary
from rsi_mcp.server import RsiDiagnoseInput, rsi_diagnose_pipeline


def test_mcp_returns_self_contained_delegation_task(tmp_path):
    ws = Workspace(str(tmp_path))
    iteration = tmp_path / 'iterations/iteration_000'
    iteration.mkdir(parents=True)
    (iteration / 'pipeline.py').write_text('# pipeline source')
    (iteration / 'decision.json').write_text('{"ops": [{"name": "KeepLatex"}]}')
    observation = iteration / 'execution_observation.json'
    observation.write_text('{"steps": []}')
    skill = tmp_path / 'skills/math/SKILL.md'
    skill.parent.mkdir(parents=True)
    skill.write_text('Preserve LaTeX.')
    ws.save(WorkspaceState(
        task_description='Prepare math data', quality_criteria=['Preserve formulas'],
        target_schema={'messages': 'list[dict]'}, domain_skills=['math'],
        iterations=[IterationSummary(iteration=0, directory=str(iteration),
                                     execution_observation_path=str(observation))],
    ))
    params = RsiDiagnoseInput(workspace_dir=str(tmp_path), iteration_dir=str(iteration))
    result = json.loads(asyncio.run(rsi_diagnose_pipeline(params)))
    delegation = result['delegation']
    assert delegation['runner'] == 'host_subagent'
    assert delegation['fresh_context'] is True
    assert delegation['inherit_conversation'] is False
    assert delegation['task'] == result['prompt_text']
    assert str(skill) in delegation['task']
    assert str(observation) in result['evidence_paths']
    assert 'Preserve formulas' in delegation['task']
    assert 'messages' in delegation['task']
    assert 'KeepLatex' in delegation['task']
    assert not (iteration / 'pipeline_diagnostic.json').exists()


def test_failure_uses_current_tail_instead_of_old_receipt_and_review(tmp_path):
    observation = tmp_path / 'execution_observation.json'
    observation.write_text('{"status": "ok"}')
    (tmp_path / 'review.jsonl').write_text(json.dumps({
        'result': {'issues': ['old review issue']}
    }) + '\n')
    tail = tmp_path / 'execution_tail_attempt01.json'
    tail.write_text('{"error": "current compile failure"}')
    os.utime(observation, ns=(1_000_000_000, 1_000_000_000))
    os.utime(tail, ns=(2_000_000_000, 2_000_000_000))
    result = build_diagnostic_context(
        tmp_path, 'task', ['current compile failure'],
        execution_observation_path=str(tail), execution_error='current compile failure',
    )
    assert result['observation_path'] is None
    assert result['evidence_paths'] == [str(tail)]
    assert 'current compile failure' in result['prompt_text']
    assert 'old review issue' not in result['prompt_text']


def test_missing_failure_evidence_is_not_replaced_by_old_receipt(tmp_path):
    (tmp_path / 'execution_observation.json').write_text('{"status": "ok"}')
    result = build_diagnostic_context(tmp_path, 'task', execution_error='missing pipeline.py')
    assert result['observation_path'] is None
    assert result['evidence_paths'] == []
    assert 'missing pipeline.py' in result['prompt_text']
