"""Runtime contracts that generated DataFlow-Evolve pipelines import.

Generated operators subclass ``OperatorABC`` and implement ``run(self, storage)``;
a pipeline subclasses ``PipelineABC``, builds one ``FileStorage`` and calls its
operators in ``forward()``. The behavior matches open-dataflow 1.0.10 without
requiring that package.
"""

from .core.contracts import ArtifactRef
from .core.dataflow_operator import OperatorABC
from .core.dataflow_pipeline import (
    BatchedPipelineABC,
    PipelineABC,
    StreamBatchedPipelineABC,
)
from .core.llm_serving import LLMServingABC
from .io.dataflow_storage import (
    BatchedFileStorage,
    DataFlowStorage,
    FileStorage,
    StreamBatchedFileStorage,
)
from .io.serving import PipelineLLMServing
from .media import (
    MediaPolicy, artifact_store_for, resolve_local_input_artifact,
    artifact_to_content_parts, absolute_artifact_refs,
)
from .step_cache import FrameworkStepCache

__all__ = [
    "ArtifactRef",
    "MediaPolicy",
    "artifact_to_content_parts",
    "absolute_artifact_refs",
    "OperatorABC",
    "LLMServingABC",
    "PipelineABC",
    "BatchedPipelineABC",
    "StreamBatchedPipelineABC",
    "DataFlowStorage",
    "FileStorage",
    "BatchedFileStorage",
    "StreamBatchedFileStorage",
    "PipelineLLMServing",
    "FrameworkStepCache",
    "artifact_store_for",
    "resolve_local_input_artifact",
]
