import asyncio
import json
import sys

import pytest

from harness.runtime_environment import check_environment, runtime_contract, pipeline_pythonpath
from rsi_mcp.server import RsiInitInput, RsiStatusInput, rsi_init, rsi_get_status, _chat_completions_url


def test_probe_uses_pipeline_python_and_reports_missing_import():
    result = check_environment(['dataflow_evolve', 'rsi_nonexistent_test_module'])
    assert result['runtime']['python_executable'] == sys.executable
    assert result['modules']['dataflow_evolve']['status'] == 'ok'
    assert result['modules']['dataflow_evolve']['module_file'].endswith('dataflow_evolve/__init__.py')
    assert result['modules']['rsi_nonexistent_test_module']['error_type'] == 'ModuleNotFoundError'
    assert result['status'] == 'dependency_issues'


def test_probe_rejects_code_as_module_name():
    with pytest.raises(ValueError):
        check_environment(['os; print(1)'])


def test_pythonpath_retains_existing_environment(monkeypatch):
    monkeypatch.setenv('PYTHONPATH', '/custom/library')
    assert pipeline_pythonpath().endswith('/custom/library')
    assert runtime_contract()['package_root'].endswith('/DataFlow-Evolve/dataflow_evolve')


def test_init_and_status_expose_same_contract(tmp_path):
    corpus = tmp_path / 'input.jsonl'
    corpus.write_text('{}\n')
    workspace = str(tmp_path / 'workspace')
    params = RsiInitInput(task_description='test', input_path=str(corpus),
        review_api_url='http://unused/v1', review_model='review',
        pipeline_api_url='http://unused/v1', pipeline_model='pipeline',
        workspace_dir=workspace, quality_criteria=['correct'])
    async def exercise():
        initial = json.loads(await rsi_init(params))
        status = json.loads(await rsi_get_status(RsiStatusInput(workspace_dir=workspace)))
        assert initial['runtime'] == status['runtime']
        assert initial['pipeline_llm'] == status['pipeline_llm']
        assert initial['pipeline_llm']['api_url'] == 'http://unused/v1/chat/completions'
    asyncio.run(exercise())


def test_endpoint_contract_preserves_custom_paths():
    assert _chat_completions_url('http://host/v1/') == 'http://host/v1/chat/completions'
    assert _chat_completions_url('http://host/v1/chat/completions') == 'http://host/v1/chat/completions'
    assert _chat_completions_url('http://host/custom/chat') == 'http://host/custom/chat'
