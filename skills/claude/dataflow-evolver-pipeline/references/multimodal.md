# Image and video pipelines

## Dataset and initialisation

Each JSONL row carries text plus canonical local media references. Paths are
relative to the JSONL file, or absolute. Download remote dataset media before
initialisation. A field may contain one reference or a list of references.

```json
{"question":"Describe the scene.","image":{"kind":"image","uri":"images/001.png","media_type":"image/png"}}
{"question":"What happens in the clip?","video":{"kind":"video_reference","uri":"videos/001.mp4","media_type":"video/mp4","metadata":{"start_sec":0,"end_sec":2}}}
```

For an image QA corpus, pass these agreed settings to `rsi_init`:

```json
{
  "input_contract": {
    "modalities": ["text", "image"],
    "required_fields": ["question", "image"],
    "artifact_fields": {"image": "image"}
  },
  "output_artifact_fields": {"image": "image"},
  "media_policy": {"review_sample_size": 8, "video_frames": 8, "max_edge": 768,
    "max_images": 64, "max_payload_bytes": 20971520,
    "max_source_bytes": 1073741824, "decode_timeout_sec": 30}
}
```

For video use modality `video` and artifact kind `video_reference`. Declare
all required output media fields in `output_artifact_fields`. Confirm the
actual policy with the user before starting; the values above are defaults.
Budget review rows × media per row × frames per video within `max_images` and
the encoded media payload limit. Oversized review samples return an error.

## Visual model calls

The Pipeline constructs the agreed `PipelineLLMServing` and injects it into
operators. Within an operator, convert references into actual image parts:

```python
import os
from dataflow_evolve import artifact_to_content_parts

rows = storage.read("dict")
dialogues = []
for row in rows:
    parts, evidence = artifact_to_content_parts(row["image"], os.environ["ENTRY_PATH"])
    dialogues.append([{"role": "user", "content": [
        {"type": "text", "text": row["question"]}, *parts]}])
responses = self.llm_serving.generate_from_conversations(dialogues)
# Validate each aligned str/None response, preserve media references, then storage.write.
```

The same helper accepts `video_reference`: it samples uniform frames inside
the optional `start_sec`/`end_sec` interval, attaches their timestamps and uses
`image_url` parts. `DF_MEDIA_POLICY` supplies the workspace settings. Returned
evidence records content hashes and sampled timestamps; preserve these when
an operator makes time-dependent decisions. Audio is outside this transport.
Models and endpoints must support OpenAI-compatible image content parts.

`FileStorage`, `BatchedFileStorage` and `StreamBatchedFileStorage` normalize
canonical references to absolute paths across steps. Keep source references in
output rows so review can check answers against the actual image/video frames.
The source corpus and media should remain fixed throughout an experiment.

## Validation, review and export

Initialisation and submission check every reference, declared required field,
file size, content hash when supplied, and visual decodability. Images require
Pillow; video also requires FFmpeg/ffprobe on PATH (or `DF_FFMPEG_BIN` and
`DF_FFPROBE_BIN`). Use the execution Python reported by `runtime`.

Quality review sends actual images and timestamped frames to the review model.
Criteria should check visual grounding, answer correctness and, for video,
temporal claims against the sampled interval. `review.jsonl` records media
hashes and timestamps for the independent diagnostic sub-agent. Diagnoses can
inspect original media when sampled evidence cannot resolve a question.

Cache reuse checks referenced media bytes, workspace media policy, JSONL and
operator identity. Declare any operator-specific sampling settings in
`decision.json.params` so changing them also invalidates the affected steps.

`rsi_export_dataset` writes a JSONL plus `<name>_media/` containing referenced
files, deduplicated by content and format, with relative paths and SHA-256
hashes. Move both together. A downstream script receives the input/output
contracts, media policy and `task.media_base_dir` in its request; implement the
training-format conversion required by its trainer in that adapter.
