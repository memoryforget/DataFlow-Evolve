"""Exercise real media decoding, transport shapes, cache reuse and portable export."""
import asyncio
import base64
import io
import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

Image = pytest.importorskip("PIL.Image", reason="install the multimodal extra for media tests")

from dataflow_evolve import (FileStorage, BatchedFileStorage, StreamBatchedFileStorage,
    PipelineLLMServing, MediaPolicy, artifact_store_for, artifact_to_content_parts)
from dataflow_evolve.core.contracts import InputContract
from dataflow_evolve.media import (export_media_dataset, resolve_local_input_artifact,
    validate_media_manifest, visual_sample_parts)
from dataflow_evolve.models import PipelineConfig
from dataflow_evolve.step_cache import StepReusePlan, prepare_step_reuse, write_step_manifest
from harness import reviewer
from harness.executor import submit_pipeline
from harness.workspace import init_workspace
from harness.diagnostic import build_diagnostic_context
from rsi_mcp.server import RsiInitInput, RsiStatusInput, rsi_init, rsi_get_status


def image_ref(path, color="red"):
    Image.new("RGB", (64, 32), color=color).save(path, format="PNG")
    return {"kind": "image", "uri": str(path), "media_type": "image/png"}


def manifest(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return path


def reply(*args):
    return {"text": json.dumps({"criteria_assessments": [{"criterion_index": 1, "met": True,
            "evidence": "red image agrees with answer", "suggestion": ""}], "critical_failures": []}),
            "http_status": 200, "finish_reason": "stop", "usage": {}}


def test_serving_transmits_image_parts_and_preserves_text(monkeypatch, tmp_path):
    monkeypatch.setenv("TEST_API_KEY", "fake")
    serving = PipelineLLMServing(api_url="http://unused/v1/chat/completions",
        key_name_of_api_key="TEST_API_KEY", model_name="vision", max_workers=1, max_retries=1)
    ref = image_ref(tmp_path / "input.png")
    parts, _ = artifact_to_content_parts(ref, tmp_path / "input.jsonl")
    sent = []
    def post(url, **kwargs):
        sent.append(json.loads(kwargs["data"]))
        return SimpleNamespace(status_code=200, headers={}, content=b"{}", json=lambda: {
            "choices": [{"message": {"content": "red"}}]})
    monkeypatch.setattr(serving.session, "post", post)
    dialogues = [[{"role": "user", "content": [{"type": "text", "text": "What color?"}, *parts]}]]
    assert serving.generate_from_conversations(dialogues) == ["red"]
    assert sent[0]["messages"] == dialogues[0]
    assert serving.generate_from_input(["hello"]) == ["red"]
    assert sent[1]["messages"][-1]["content"] == "hello"
    for bad in ([{"role": "user", "content": [{"role": "user", "content": "nested"}]}],
                [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "/tmp/a.png"}}]}]):
        with pytest.raises(TypeError, match="invalid content part"):
            serving.generate_from_conversations([bad])
    with pytest.raises(TypeError):
        serving.generate_from_input(dialogues)
    assert len(sent) == 2
    serving.cleanup()


@pytest.mark.parametrize("storage_type", [FileStorage, BatchedFileStorage, StreamBatchedFileStorage])
def test_relative_and_generated_media_survive_steps_and_export(storage_type, monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    ref = image_ref(source / "source.png")
    ref["uri"] = "source.png"
    entry = manifest(source / "data.jsonl", [{"images": [ref], "answer": "red"}])
    iteration = tmp_path / "iteration"
    monkeypatch.setenv("ITERATION_DIR", str(iteration))
    storage = storage_type(str(entry), cache_path=str(iteration / "cache"))
    first = storage.step()
    if storage_type is StreamBatchedFileStorage:
        first.batch_size = 1
        first._current_streaming_chunk = next(first.iter_chunks())
        assert first._current_streaming_chunk.iloc[0]["images"][0]["uri"] == str(source / "source.png")
    rows = first.read("dict")
    assert rows[0]["images"][0]["uri"] == str(source / "source.png")
    generated = artifact_store_for(iteration).put_image("generated", source / "source.png")
    rows[0]["generated"] = generated.to_dict()
    cached = Path(first.write(rows))
    cached_row = json.loads(cached.read_text())
    assert Path(cached_row["generated"]["uri"]).is_file()
    validate_media_manifest(cached)
    exported = tmp_path / "export" / "train.jsonl"
    result = export_media_dataset(cached, exported)
    assert result["media_files"] == 1  # identical bytes are content-addressed
    moved = tmp_path / "moved"
    shutil.move(str(exported.parent), moved)
    shutil.rmtree(source)
    shutil.rmtree(iteration)
    output = moved / "train.jsonl"
    row = json.loads(output.read_text())
    assert not Path(row["images"][0]["uri"]).is_absolute()
    assert resolve_local_input_artifact(output, row["images"][0]).is_file()
    assert validate_media_manifest(output)["references"] == 2


@pytest.mark.parametrize("failure", ["missing", "corrupt", "sha256", "remote", "oversize"])
def test_invalid_media_is_reported_before_review_api(failure, monkeypatch, tmp_path):
    ref = image_ref(tmp_path / "bad.png")
    policy = {}
    if failure == "missing":
        Path(ref["uri"]).unlink()
    elif failure == "corrupt":
        Path(ref["uri"]).write_bytes(b"not an image")
    elif failure == "sha256":
        ref["sha256"] = "f" * 64
    elif failure == "remote":
        ref["uri"] = "https://unused/image.png"
    else:
        Path(ref["uri"]).write_bytes(b"x" * 2048)
        policy = {"max_source_bytes": 1024}
    entry = manifest(tmp_path / "input.jsonl", [{"image": ref, "answer": "red"}])
    def unexpected(*args):
        pytest.fail("bad media must not fall back to text-only judging")
    monkeypatch.setattr(reviewer, "_call_llm", unexpected)
    result = reviewer.CriteriaReviewer("http://unused", "fake", "vision").review(
        str(entry), ["visually correct"], {"answer": "str"}, "test", media_policy=policy)
    assert result.status == "error"
    assert "media review preparation failed" in result.issues[0]


def test_visual_review_attaches_images_and_records_only_metadata(monkeypatch, tmp_path):
    ref = image_ref(tmp_path / "red.png")
    entry = manifest(tmp_path / "input.jsonl", [{"image": ref, "answer": "red"}])
    evidence = tmp_path / "review.jsonl"
    received = []
    def judge(url, key, model, content, timeout):
        received.append(content)
        data = next(p["image_url"]["url"] for p in content if p["type"] == "image_url")
        with Image.open(io.BytesIO(base64.b64decode(data.split(",", 1)[1]))) as image:
            assert image.getpixel((0, 0))[0] > 200
        return reply()
    monkeypatch.setattr(reviewer, "_call_llm", judge)
    result = reviewer.CriteriaReviewer("http://unused", "fake", "vision").review(
        str(entry), ["visually correct"], {"answer": "str"}, "test", str(evidence),
        input_contract={"modalities": ["text", "image"]}, output_artifact_fields={"image": "image"})
    assert result.status == "ok" and result.passed
    assert len(received) == 1
    record = json.loads(evidence.read_text())
    assert record["visual_evidence"][0]["sha256"]
    assert "base64," not in evidence.read_text()
    assert result.hard_metrics["visual_assets_reviewed"] == 1


def test_multimodal_review_rejects_lost_evidence(monkeypatch, tmp_path):
    entry = manifest(tmp_path / "input.jsonl", [{"answer": "red"}])
    monkeypatch.setattr(reviewer, "_call_llm", lambda *a: pytest.fail("missing image"))
    result = reviewer.CriteriaReviewer("http://unused", "fake", "vision").review(
        str(entry), ["visually correct"], {"answer": "str"}, "test",
        input_contract={"modalities": ["image"]})
    assert result.status == "error"
    assert "retain source evidence" in result.issues[0]


def test_review_budget_is_explicit(tmp_path):
    ref = image_ref(tmp_path / "red.png")
    with pytest.raises(ValueError, match="max_images"):
        visual_sample_parts([{"images": [ref, ref]}], tmp_path / "input.jsonl", MediaPolicy(max_images=1))
    with pytest.raises(ValueError, match="video_frames"):
        MediaPolicy.from_mapping({"video_frames": 0})


def test_video_frames_include_decoded_images_and_timestamps(tmp_path):
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        pytest.skip("ffmpeg/ffprobe are optional external dependencies")
    video = tmp_path / "clip.mp4"
    subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=64x32:r=2:d=2",
                    "-c:v", "mpeg4", "-threads", "1", str(video)], check=True)
    ref = {"kind": "video_reference", "uri": str(video), "media_type": "video/mp4"}
    parts, evidence = artifact_to_content_parts(ref, tmp_path / "input.jsonl", policy=MediaPolicy(video_frames=4))
    assert sum(p["type"] == "image_url" for p in parts) == 4
    assert len(evidence["timestamps_sec"]) == 4
    assert all(0 <= t < 2 for t in evidence["timestamps_sec"])
    assert evidence["audio_reviewed"] is False
    ref["metadata"] = {"start_sec": 2, "end_sec": 1}
    with pytest.raises(ValueError, match="invalid video interval"):
        artifact_to_content_parts(ref, tmp_path / "input.jsonl")


def test_video_review_diagnosis_and_portable_export(monkeypatch, tmp_path):
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        pytest.skip("ffmpeg/ffprobe are optional external dependencies")
    video = tmp_path / "clip.mp4"
    subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=red:s=64x32:r=4:d=2",
                    "-c:v", "mpeg4", "-threads", "1", str(video)], check=True)
    ref = {"kind": "video_reference", "uri": str(video), "media_type": "video/mp4",
           "metadata": {"start_sec": 0.25, "end_sec": 1.75}}
    entry = manifest(tmp_path / "input.jsonl", [{"video": ref, "answer": "red"}])
    def judge(url, key, model, content, timeout):
        images = [p for p in content if p["type"] == "image_url"]
        assert len(images) == 2
        assert sum("Video frame at" in p.get("text", "") for p in content) == 2
        return reply()
    monkeypatch.setattr(reviewer, "_call_llm", judge)
    result = reviewer.CriteriaReviewer("http://unused", "fake", "vision").review(
        str(entry), ["visually grounded"], {"video": "ArtifactRef", "answer": "str"}, "video QA",
        str(tmp_path / "review.jsonl"), input_contract={"modalities": ["video"]},
        output_artifact_fields={"video": "video_reference"}, media_policy={"video_frames": 2})
    assert result.status == "ok" and result.passed
    context = build_diagnostic_context(tmp_path, "video QA")
    assert context["media_paths"] == [str(video)]
    assert "timestamps_sec" in context["prompt_text"]
    exported = tmp_path / "export" / "data.jsonl"
    assert export_media_dataset(entry, exported)["media_files"] == 1
    video.unlink()
    assert validate_media_manifest(exported)["references"] == 1


def test_text_input_can_declare_generated_visual_output(tmp_path):
    entry = manifest(tmp_path / "input.jsonl", [{"prompt": "draw a red square"}])
    params = RsiInitInput(task_description="image generation", input_path=str(entry),
        quality_criteria=["red square"], workspace_dir=str(tmp_path / "workspace"),
        review_api_url="http://unused", review_model="vision",
        pipeline_api_url="http://unused", pipeline_model="vision",
        output_artifact_fields={"image": "image"})
    result = json.loads(asyncio.run(rsi_init(params)))
    assert result["status"] == "ok"
    assert result["input_contract"]["modalities"] == ["text"]
    assert result["output_artifact_fields"] == {"image": "image"}


def test_media_changes_invalidate_actual_cache_reuse(tmp_path):
    source_ref = image_ref(tmp_path / "source.png")
    output_ref = image_ref(tmp_path / "output.png")
    entry = manifest(tmp_path / "input.jsonl", [{"image": source_ref}])
    parent, child = tmp_path / "parent", tmp_path / "child"
    parent.mkdir()
    child.mkdir()
    source = "class Keep:\n    def run(self, storage): pass\n# FrameworkStepCache .should_run(\n"
    for directory in (parent, child):
        (directory / "pipeline.py").write_text(source)
    output = manifest(parent / "step.jsonl", [{"image": output_ref}])
    config = PipelineConfig([{"name": "Keep"}], "", source, "")
    plan = StepReusePlan(["Keep"], 0, str(entry), str(entry))
    write_step_manifest(config, parent, raw_entry_path=str(entry), pipeline_filename="pipeline.py",
        plan=plan, executed_artifacts=[output], final_dataset_path=str(output))
    def reuse(policy=None):
        return prepare_step_reuse(config, child, parent_iteration_dir=parent,
            raw_entry_path=str(entry), pipeline_filename="pipeline.py", media_policy=policy).prefix_count
    assert reuse() == 1
    assert reuse({"video_frames": 2}) == 0
    image_ref(tmp_path / "output.png", "blue")
    assert reuse() == 0
    image_ref(tmp_path / "output.png", "red")
    assert reuse() == 1
    image_ref(tmp_path / "source.png", "blue")
    assert reuse() == 0


def test_mcp_persists_media_configuration_and_validates_input(tmp_path):
    ref = image_ref(tmp_path / "input.png")
    entry = manifest(tmp_path / "input.jsonl", [{"images": [ref], "question": "color?"}])
    contract = {"modalities": ["text", "image"], "required_fields": ["images", "question"],
                "artifact_fields": {"images": "image"}}
    params = RsiInitInput(task_description="visual QA", input_path=str(entry), quality_criteria=["grounded"],
        workspace_dir=str(tmp_path / "workspace"), review_api_url="http://unused", review_model="vision",
        pipeline_api_url="http://unused", pipeline_model="vision", input_contract=contract,
        output_artifact_fields={"images": "image"}, media_policy={"review_sample_size": 2})
    async def exercise():
        initial = json.loads(await rsi_init(params))
        assert initial["status"] == "ok"
        status = json.loads(await rsi_get_status(RsiStatusInput(workspace_dir=params.workspace_dir)))
        for name in ("input_contract", "output_artifact_fields", "media_policy"):
            assert initial[name] == status[name]
        assert initial["media_policy"]["review_sample_size"] == 2
        Path(ref["uri"]).unlink()
        invalid = json.loads(await rsi_init(params.model_copy(update={"workspace_dir": str(tmp_path / "invalid")})))
        assert invalid["status"] == "error"
        assert not (tmp_path / "invalid" / "workspace.json").exists()
    asyncio.run(exercise())


def test_executor_runs_multimodal_pipeline_and_reviews_visuals(monkeypatch, tmp_path):
    ref = image_ref(tmp_path / "input.png")
    ref["uri"] = "input.png"
    entry = manifest(tmp_path / "input.jsonl", [{"image": ref, "answer": "red"}])
    ws = init_workspace(tmp_path / "workspace", input_path=str(entry), task_description="visual QA",
        quality_criteria=["visually grounded"], target_schema={"answer": "str", "image": "ArtifactRef"},
        input_contract={"modalities": ["text", "image"], "artifact_fields": {"image": "image"}},
        output_artifact_fields={"image": "image"})
    _, iteration = ws.next_iteration_dir()
    iteration.mkdir(parents=True)
    (iteration / "decision.json").write_text('{"ops": [{"name": "Keep"}]}')
    (iteration / "pipeline.py").write_text('''import os
from dataflow_evolve import FileStorage
class Keep:
    def run(self, storage):
        storage.write(storage.read("dict"))
if os.environ.get("DF_COMPILE_ONLY") != "1":
    storage = FileStorage(os.environ["ENTRY_PATH"], cache_path=os.environ["CACHE_DIR"], file_name_prefix="pipeline_step")
    Keep().run(storage.step())
''')
    def judge(url, key, model, content, timeout):
        assert any(p.get("type") == "image_url" for p in content)
        return reply()
    monkeypatch.setattr(reviewer, "_call_llm", judge)
    result = submit_pipeline(ws, iteration, review_api_url="http://unused", review_api_key="fake", review_model="vision")
    assert result.failure_stage is None, result
    assert result.passed and result.incumbent_updated
    dataset = Path(ws.state().incumbent_dataset_path)
    assert Path(json.loads(dataset.read_text())["image"]["uri"]).is_file()
    assert json.loads((iteration / "review.jsonl").read_text())["visual_evidence"]
