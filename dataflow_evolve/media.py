"""Local media references, bounded visual messages, and portable dataset export."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import math
import mimetypes
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from .core.contracts import ArtifactRef, InputContract


@dataclass(frozen=True)
class MediaPolicy:
    review_sample_size: int = 8
    video_frames: int = 8
    max_edge: int = 768
    max_images: int = 64
    max_payload_bytes: int = 20 * 1024 * 1024
    max_source_bytes: int = 1024 * 1024 * 1024
    decode_timeout_sec: int = 30

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None = None) -> "MediaPolicy":
        value = dict(value or {})
        limits = {"review_sample_size": (1, 80), "video_frames": (1, 64),
                  "max_edge": (64, 4096), "max_images": (1, 256),
                  "max_payload_bytes": (1024, 100 * 1024 * 1024),
                  "max_source_bytes": (1024, 100 * 1024 ** 3), "decode_timeout_sec": (1, 300)}
        if set(value) - set(limits):
            raise ValueError(f"unknown media policy fields: {sorted(set(value) - set(limits))}")
        for key, number in value.items():
            low, high = limits[key]
            if type(number) is not int or not low <= number <= high:
                raise ValueError(f"media_policy.{key} must be an integer in [{low}, {high}]")
        return cls(**value)

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def artifact_store_for(iteration_dir: str | Path):
    """Generated blobs live under iteration_dir/runs/generated."""
    from .io.storage import StorageBundle
    return StorageBundle(iteration_dir, "generated").artifacts


def iter_artifact_refs(value: Any):
    """Walk canonical ArtifactRef objects, including nested lists of references."""
    if isinstance(value, ArtifactRef):
        yield value.to_dict()
    elif isinstance(value, Mapping):
        if value.get("kind") in {"image", "video_reference", "blob", "binary"} and "uri" in value:
            yield dict(value)
        else:
            for child in value.values():
                yield from iter_artifact_refs(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from iter_artifact_refs(child)


def resolve_local_input_artifact(entry_path: str | Path, reference: ArtifactRef | Mapping[str, Any]) -> Path:
    """Resolve a local reference relative to its JSONL manifest, never the cwd."""
    ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_mapping(reference)
    if "://" in ref.uri or ref.uri.startswith("data:"):
        raise ValueError("dataset artifacts must be local files; materialize remote media before submission")
    path = Path(ref.uri).expanduser()
    resolved = (path if path.is_absolute() else Path(entry_path).resolve().parent / path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"artifact is missing: {resolved}")
    return resolved


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def map_artifact_refs(value: Any, transform):
    if isinstance(value, ArtifactRef):
        return transform(value.to_dict())
    if isinstance(value, Mapping):
        if value.get("kind") in {"image", "video_reference", "blob", "binary"} and "uri" in value:
            return transform(dict(value))
        return {k: map_artifact_refs(v, transform) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [map_artifact_refs(v, transform) for v in value]
    return value


def absolute_artifact_refs(value: Any, entry_path: str | Path, *, generated_root: str | Path | None = None):
    def resolve(ref):
        base = entry_path
        if generated_root and str(ref["uri"]).startswith("blobs/"):
            base = Path(generated_root) / "manifest.jsonl"
        return {**ref, "uri": str(resolve_local_input_artifact(base, ref))}
    return map_artifact_refs(value, resolve)


def media_file_identity(manifest: str | Path) -> list[dict[str, Any]]:
    """Content identity includes referenced bytes, not just JSONL metadata."""
    identities = {}
    with Path(manifest).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            for ref in iter_artifact_refs(json.loads(line)):
                path = resolve_local_input_artifact(manifest, ref)
                key = str(path)
                if key not in identities:
                    identities[key] = {"path": key, "bytes": path.stat().st_size, "sha256": sha256_file(path)}
                if ref.get("sha256") and ref["sha256"] != identities[key]["sha256"]:
                    raise ValueError(f"artifact sha256 mismatch: {path}")
    return [identities[k] for k in sorted(identities)]


def _binary(name: str) -> str:
    return os.environ.get(f"DF_{name.upper()}_BIN") or os.environ.get(f"RSI_{name.upper()}_BIN") or name


def probe_video(path: str | Path, *, timeout_sec: int = 30) -> dict[str, Any]:
    try:
        proc = subprocess.run([_binary("ffprobe"), "-v", "error", "-show_format", "-show_streams",
                               "-of", "json", str(path)], capture_output=True, timeout=timeout_sec)
    except FileNotFoundError as exc:
        raise ValueError("video requires ffmpeg and ffprobe; install them or set DF_FFMPEG_BIN/DF_FFPROBE_BIN") from exc
    if proc.returncode:
        raise ValueError(f"cannot probe video {path}: {proc.stderr.decode(errors='replace')[:500]}")
    info = json.loads(proc.stdout)
    streams = [s for s in info.get("streams", []) if s.get("codec_type") == "video"]
    duration = float(info.get("format", {}).get("duration") or (streams[0].get("duration", 0) if streams else 0))
    if not streams or not math.isfinite(duration) or duration <= 0:
        raise ValueError(f"video has no decodable timed video stream: {path}")
    rate = str(streams[0].get("avg_frame_rate", "0/1")).split("/")
    fps = float(rate[0]) / float(rate[1]) if len(rate) == 2 and float(rate[1]) else 0
    if not math.isfinite(fps) or fps <= 0:
        raise ValueError(f"video has no usable frame rate: {path}")
    return {"duration_sec": duration, "fps": fps, "width": streams[0].get("width"), "height": streams[0].get("height")}


def _image_part(source: bytes | Path, policy: MediaPolicy) -> dict[str, Any]:
    try:
        from PIL import Image, ImageOps
    except ImportError as exc:
        raise ValueError("image/video encoding requires Pillow: pip install '.[multimodal]'") from exc
    with Image.open(io.BytesIO(source) if isinstance(source, bytes) else source) as image:
        image = ImageOps.exif_transpose(image).convert("RGB")
        image.thumbnail((policy.max_edge, policy.max_edge))
        output = io.BytesIO()
        image.save(output, format="JPEG", quality=85)
    return {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + base64.b64encode(output.getvalue()).decode()}}


def artifact_to_content_parts(reference: ArtifactRef | Mapping[str, Any], entry_path: str | Path,
                              *, policy: MediaPolicy | None = None) -> tuple[list[dict], dict]:
    """Encode an image or uniform video frames; metadata start_sec/end_sec bounds the interval."""
    policy = policy or MediaPolicy.from_mapping(json.loads(os.environ.get("DF_MEDIA_POLICY", "{}")))
    ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_mapping(reference)
    path = resolve_local_input_artifact(entry_path, ref)
    if path.stat().st_size > policy.max_source_bytes:
        raise ValueError(f"media exceeds max_source_bytes: {path}")
    digest = sha256_file(path)
    if ref.sha256 and digest != ref.sha256:
        raise ValueError(f"artifact sha256 mismatch: {path}")
    evidence = {"uri": str(path), "sha256": digest, "kind": ref.kind}
    if ref.kind == "image":
        content = [_image_part(path, policy)]
        if len(json.dumps(content).encode()) > policy.max_payload_bytes:
            raise ValueError("image content exceeds max_payload_bytes")
        return content, evidence
    if ref.kind != "video_reference":
        raise ValueError(f"visual content requires image or video_reference, got {ref.kind}")
    if policy.video_frames > policy.max_images:
        raise ValueError("video_frames exceeds max_images")
    info = probe_video(path, timeout_sec=policy.decode_timeout_sec)
    start = float(ref.metadata.get("start_sec", 0))
    end = float(ref.metadata.get("end_sec", info["duration_sec"]))
    if not (0 <= start < end <= info["duration_sec"]):
        raise ValueError(f"invalid video interval [{start}, {end}] for {path}")
    last_frame = max(0.0, info["duration_sec"] - 1 / info["fps"])
    timestamps = [min(last_frame, start + (i + 0.5) * (end - start) / policy.video_frames)
                  for i in range(policy.video_frames)]
    actual_timestamps = []
    parts = []
    for timestamp in timestamps:
        command = [_binary("ffmpeg"), "-v", "info", "-nostdin", "-threads", "1", "-ss", str(timestamp),
                   "-i", str(path), "-frames:v", "1", "-vf",
                   f"showinfo,scale={policy.max_edge}:{policy.max_edge}:force_original_aspect_ratio=decrease",
                   "-f", "image2pipe", "-vcodec", "mjpeg", "-threads", "1", "pipe:1"]
        try:
            proc = subprocess.run(command, capture_output=True, timeout=policy.decode_timeout_sec)
        except FileNotFoundError as exc:
            raise ValueError("video frame sampling requires ffmpeg (DF_FFMPEG_BIN)") from exc
        if proc.returncode or not proc.stdout:
            raise ValueError(f"cannot decode video frame at {timestamp:.3f}s: {path}")
        match = re.search(r"pts_time:([-+\d.eE]+)", proc.stderr.decode(errors="replace"))
        if not match:
            raise ValueError(f"decoder did not report a frame timestamp: {path}")
        actual = timestamp + float(match.group(1))
        if not start <= actual < end:
            raise ValueError(f"no sampled frame in requested interval [{start}, {end}]: {path}")
        actual_timestamps.append(actual)
        parts.extend([{"type": "text", "text": f"Video frame at {actual:.3f} seconds"}, _image_part(proc.stdout, policy)])
    if len(json.dumps(parts).encode()) > policy.max_payload_bytes:
        raise ValueError("video content exceeds max_payload_bytes")
    return parts, {**evidence, **info, "timestamps_sec": actual_timestamps,
                   "requested_timestamps_sec": timestamps, "audio_reviewed": False}


def visual_sample_parts(rows: list[dict], manifest: str | Path, policy: MediaPolicy) -> tuple[list[dict], list[dict]]:
    parts, evidence = [], []
    count = payload_bytes = 0
    for row_number, row in enumerate(rows, 1):
        for ref in iter_artifact_refs(row):
            if ref["kind"] not in {"image", "video_reference"}:
                continue
            expected = policy.video_frames if ref["kind"] == "video_reference" else 1
            if count + expected > policy.max_images:
                raise ValueError("visual sample exceeds max_images; reduce review_sample_size/video_frames or raise the agreed limit")
            content, item = artifact_to_content_parts(ref, manifest, policy=policy)
            payload_bytes += len(json.dumps(content).encode())
            if payload_bytes > policy.max_payload_bytes:
                raise ValueError("visual sample exceeds max_payload_bytes; reduce review sampling or image size")
            count += expected
            parts.append({"type": "text", "text": f"Sample row {row_number}, media {len(evidence) + 1}"})
            parts.extend(content)
            evidence.append({"sample_row": row_number, **item})
    return parts, evidence


def validate_media_manifest(manifest: str | Path, *, contract: InputContract | None = None,
                            policy: MediaPolicy | None = None) -> dict[str, int]:
    """Validate all references and decode each unique visual asset once."""
    policy = policy or MediaPolicy()
    checked = set()
    rows = references = 0
    with Path(manifest).open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"manifest row {number} must be an object")
            if contract:
                contract.validate_record(row, row_number=number)
            rows += 1
            for ref in iter_artifact_refs(row):
                references += 1
                identity = json.dumps(ref, sort_keys=True)
                if identity in checked:
                    continue
                path = resolve_local_input_artifact(manifest, ref)
                if path.stat().st_size > policy.max_source_bytes:
                    raise ValueError(f"media exceeds max_source_bytes: {path}")
                digest = sha256_file(path)
                if ref.get("sha256") and ref["sha256"] != digest:
                    raise ValueError(f"artifact sha256 mismatch: {path}")
                if ref["kind"] in {"image", "video_reference"}:
                    artifact_to_content_parts(ref, manifest, policy=MediaPolicy.from_mapping({**policy.to_dict(), "video_frames": 1}))
                checked.add(identity)
    if not rows:
        raise ValueError("manifest contains no records")
    return {"rows": rows, "references": references, "unique_assets": len(checked)}


def export_media_dataset(source: str | Path, destination: str | Path) -> dict[str, Any]:
    """Copy media into a content-addressed bundle and atomically publish rewritten JSONL."""
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("export destination must differ from the source dataset")
    destination.parent.mkdir(parents=True, exist_ok=True)
    assets = destination.parent / (destination.stem + "_media")
    copied = {}
    def copy_ref(ref):
        path = resolve_local_input_artifact(source, ref)
        digest = sha256_file(path)
        if ref.get("sha256") and digest != ref["sha256"]:
            raise ValueError(f"artifact sha256 mismatch: {path}")
        extension = mimetypes.guess_extension(ref["media_type"]) or path.suffix.lower()
        relative = Path(assets.name) / (digest + extension)
        target = destination.parent / relative
        if str(target) not in copied:
            assets.mkdir(exist_ok=True)
            if not target.exists() or sha256_file(target) != digest:
                shutil.copy2(path, target)
            copied[str(target)] = {"uri": relative.as_posix(), "sha256": digest, "bytes": target.stat().st_size}
        return {**ref, "uri": relative.as_posix(), "sha256": digest}
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=destination.parent, delete=False) as out:
            temp_path = Path(out.name)
            with source.open(encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        out.write(json.dumps(map_artifact_refs(json.loads(line), copy_ref), ensure_ascii=False) + "\n")
        os.replace(temp_path, destination)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()
    return {"output_path": str(destination), "media_files": len(copied), "media_directory": str(assets) if copied else None}
