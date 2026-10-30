"""Execution environment shared by pipeline launches and dependency probes."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = REPO_ROOT / 'harness/telemetry/_bootstrap'


def pipeline_pythonpath() -> str:
    return os.pathsep.join(str(p) for p in (REPO_ROOT, BOOTSTRAP_DIR, os.environ.get('PYTHONPATH', '')) if p)


def runtime_contract() -> dict[str, Any]:
    return {
        'python_executable': sys.executable,
        'python_version': sys.version.split()[0],
        'repo_root': str(REPO_ROOT),
        'package_root': str(REPO_ROOT / 'dataflow_evolve'),
        'serving_source': str(REPO_ROOT / 'dataflow_evolve/io/serving.py'),
        'pythonpath': pipeline_pythonpath(),
        'dependency_check_tool': 'rsi_check_environment',
        'note': 'Compile and execute use this Python with the listed PYTHONPATH; task-specific dependencies belong in this environment.',
    }


_PROBE = '''import importlib, json, sys
try:
    module = importlib.import_module(sys.argv[1])
    result = {"status": "ok", "module_file": getattr(module, "__file__", None)}
except Exception as exc:
    result = {"status": "import_error", "error_type": type(exc).__name__, "error": str(exc)[:2000]}
print("RSI_IMPORT_RESULT=" + json.dumps(result))
'''


def check_environment(modules: list[str], timeout_sec: int = 30) -> dict[str, Any]:
    if len(modules) > 20 or not all(re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*', name) for name in modules):
        raise ValueError('Supply at most 20 Python module names, e.g. transformers or sympy.')
    env = dict(os.environ)
    env['PYTHONPATH'] = pipeline_pythonpath()
    results = {}
    for name in dict.fromkeys(modules):
        try:
            proc = subprocess.run([sys.executable, '-c', _PROBE, name],
                                  cwd=str(REPO_ROOT), env=env, capture_output=True,
                                  text=True, timeout=timeout_sec)
            records = [line[len('RSI_IMPORT_RESULT='):] for line in proc.stdout.splitlines()
                       if line.startswith('RSI_IMPORT_RESULT=')]
            results[name] = json.loads(records[-1]) if records else {
                'status': 'probe_error', 'returncode': proc.returncode,
                'error': 'Import probe exited without a result.'}
        except subprocess.TimeoutExpired:
            results[name] = {'status': 'timeout', 'timeout_sec': timeout_sec}
    return {'status': 'ok' if all(r['status'] == 'ok' for r in results.values()) else 'dependency_issues',
            'runtime': runtime_contract(), 'modules': results,
            'scope': 'Imports only; model loading, tokenizer assets and API connectivity require task-specific checks.'}
