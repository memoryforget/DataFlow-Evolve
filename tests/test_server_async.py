"""Exercise the MCP handlers without running pipelines or making API calls."""
import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

from rsi_mcp import server


@pytest.fixture
def harness(monkeypatch):
    state = SimpleNamespace(
        review_api_url='http://unused', review_api_key_env='TEST_REVIEW_KEY',
        review_model='test', pipeline_api_key_env='TEST_PIPELINE_KEY',
    )
    monkeypatch.setattr(server, '_load_workspace', lambda path: SimpleNamespace(state=lambda: state))
    monkeypatch.setattr(server, '_api_key', lambda *args: '')
    monkeypatch.setattr(server, 'unfinished_jobs', lambda ws: [])
    monkeypatch.setattr(server, 'missing_baselines', lambda state: [])
    monkeypatch.setattr(server, 'write_history_snapshot', lambda ws: [])
    return state


def submit_result():
    return SimpleNamespace(
        iteration=0, review_score=0.8, passed=True, issues=[], domain_feedback={},
        execution_error=None, execution_observation=None, failure_stage=None,
        incumbent_updated=True, incumbent_score=0.8, next_iteration_dir='next',
        pipeline_llm_usage={}, review_llm={}, step_reuse={}, repair_feedback=None,
    )


@pytest.mark.parametrize('kind', ['submit', 'review'])
def test_blocking_executor_keeps_event_loop_responsive(kind, harness, monkeypatch, tmp_path):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def blocking(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(3), 'event loop did not release the worker'
            return submit_result() if kind == 'submit' else {'status': 'ok'}
        finally:
            finished.set()

    monkeypatch.setattr(server, '_submit_pipeline' if kind == 'submit' else '_review_iteration', blocking)
    model = server.RsiSubmitPipelineInput if kind == 'submit' else server.RsiReviewIterationInput
    handler = server.rsi_submit_pipeline if kind == 'submit' else server.rsi_review_iteration
    params = model(workspace_dir=str(tmp_path), iteration_dir=str(tmp_path / 'iteration_000'))

    async def exercise():
        task = asyncio.create_task(handler(params))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            # This coroutine must run before the synchronous executor finishes.
            await asyncio.sleep(0)
            assert not finished.is_set()
            assert not task.done()
        finally:
            release.set()
            result = json.loads(await task)
        assert result.get('review_score', result.get('status')) == (0.8 if kind == 'submit' else 'ok')

    asyncio.run(exercise())


def test_workspace_exclusion_survives_request_cancellation(harness, monkeypatch, tmp_path):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def blocking(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(3)
            return submit_result()
        finally:
            finished.set()

    monkeypatch.setattr(server, '_submit_pipeline', blocking)
    monkeypatch.setattr(server, '_review_iteration', lambda *args, **kwargs: {'status': 'ok'})
    submit = server.RsiSubmitPipelineInput(workspace_dir=str(tmp_path), iteration_dir='iteration_000')
    # A different spelling of the same workspace must share its lock.
    review = server.RsiReviewIterationInput(workspace_dir=str(tmp_path / 'child' / '..'), iteration_dir='iteration_000')
    other = server.RsiReviewIterationInput(workspace_dir=str(tmp_path / 'other'), iteration_dir='iteration_000')

    async def exercise():
        task = asyncio.create_task(server.rsi_submit_pipeline(submit))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            assert json.loads(await server.rsi_review_iteration(review))['status'] == 'workspace_busy'
            assert json.loads(await server.rsi_review_iteration(other))['status'] == 'ok'
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert json.loads(await server.rsi_review_iteration(review))['status'] == 'workspace_busy'
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 2)
        # Worker completion includes response/history writing and lock release.
        for _ in range(100):
            result = json.loads(await server.rsi_review_iteration(review))
            if result['status'] != 'workspace_busy':
                break
            await asyncio.sleep(0.01)
        assert result['status'] == 'ok'

    asyncio.run(exercise())


def test_exception_releases_workspace_lock(harness, monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise RuntimeError('review failed')

    monkeypatch.setattr(server, '_review_iteration', fail)
    params = server.RsiReviewIterationInput(workspace_dir=str(tmp_path), iteration_dir='iteration_000')

    async def exercise():
        with pytest.raises(RuntimeError, match='review failed'):
            await server.rsi_review_iteration(params)
        monkeypatch.setattr(server, '_review_iteration', lambda *args, **kwargs: {'status': 'ok'})
        assert json.loads(await server.rsi_review_iteration(params))['status'] == 'ok'

    asyncio.run(exercise())
