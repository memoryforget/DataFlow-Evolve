"""Automatically imported only in generated pipeline subprocesses.

The executor puts this directory on PYTHONPATH, so the interpreter imports this
file as ``sitecustomize`` at startup and HTTP calls are recorded from the start.
"""

import importlib.util
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parents[2]  # _bootstrap -> telemetry -> harness -> repo root


def _run_shadowed_sitecustomize() -> None:
    """Python imports only the first sitecustomize on sys.path, so this file hides
    the interpreter's own one (conda uses it to register package directories).
    Run that one too so the pipeline sees the same environment as a normal run."""
    for entry in sys.path:
        if not entry:
            continue
        try:
            if Path(entry).resolve() == _HERE:
                continue
        except OSError:
            continue
        candidate = Path(entry) / "sitecustomize.py"
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location("_shadowed_sitecustomize", candidate)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return


try:
    _run_shadowed_sitecustomize()
except Exception as exc:  # match site.py: report and keep starting
    print(f"Error in shadowed sitecustomize: {exc!r}", file=sys.stderr)

try:
    # The bootstrap directory itself is on PYTHONPATH; keep the repository
    # importable without an editable install.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    # Load by path: importing harness.telemetry would run harness/__init__.py
    # and pull the whole harness into every pipeline process.
    _spec = importlib.util.spec_from_file_location(
        "rsi_pipeline_usage", _HERE.parent / "pipeline_usage.py"
    )
    _usage = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_usage)
    _usage.install_requests_usage_tracking()
except Exception:
    # Telemetry is best-effort and must never prevent a pipeline from starting.
    pass
