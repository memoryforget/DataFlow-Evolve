"""Normalized n-gram overlap between a candidate dataset and user reference sets.

Ported from DataFlow-Evolve's decontamination module. Contamination is a
deterministic hard check, not a prompt instruction:

- Text is lowercased, stripped of punctuation and whitespace-collapsed, so
  surface edits do not evade detection.
- Latin text is split into words and CJK text into characters. Splitting CJK on
  whitespace alone would turn a whole sentence into one token, leaving no
  n-grams and silently disabling detection for Chinese data.
- Text shorter than n tokens falls back to an exact match on the normalized
  string, otherwise short questions could never be detected.
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_NGRAM = 13
MAX_EXAMPLES = 3

_NON_WORD = re.compile(r"[^\w\s]", flags=re.UNICODE)
_SPACES = re.compile(r"\s+")
_CJK_CHAR = re.compile(r"([぀-ヿ㐀-䶿一-鿿가-힯])")


def normalize(text: str) -> str:
    return _SPACES.sub(" ", _NON_WORD.sub(" ", text.lower())).strip()


def _tokens(text: str) -> list[str]:
    norm = normalize(text)
    if not norm:
        return []
    return _CJK_CHAR.sub(r" \1 ", norm).split()


def _ngrams(tokens: list[str], n: int) -> set[str]:
    if len(tokens) < n:
        return set()
    return {" ".join(tokens[i:i + n]) for i in range(len(tokens) - n + 1)}


@dataclass
class ContaminationReport:
    rate: float = 0.0
    hits: int = 0
    total: int = 0
    examples: list[str] = field(default_factory=list)


class ContaminationIndex:
    """N-gram index of the reference sets; an empty index reports every scan clean."""

    def __init__(self, n: int = DEFAULT_NGRAM) -> None:
        self.n = max(2, int(n))
        self._ngrams: set[str] = set()
        self._exact: set[str] = set()
        self.sources: list[str] = []
        self.missing: list[str] = []

    @property
    def enabled(self) -> bool:
        return bool(self._ngrams or self._exact)

    def add_text(self, text: str) -> None:
        toks = _tokens(text)
        if not toks:
            return
        grams = _ngrams(toks, self.n)
        if grams:
            self._ngrams |= grams
        else:
            self._exact.add(" ".join(toks))

    def add_file(self, path: str | Path, fields: list[str] | None = None) -> int:
        """Index one reference file; an unreadable file is recorded, not raised."""
        p = Path(path).expanduser()
        if not p.is_file():
            self.missing.append(str(p))
            return 0
        count = 0
        try:
            for text in _iter_texts(p, fields):
                self.add_text(text)
                count += 1
        except (OSError, UnicodeDecodeError) as exc:
            print(f"[decontamination] could not read {p}: {exc}", file=sys.stderr)
            self.missing.append(str(p))
            return count
        self.sources.append(str(p))
        return count

    def scan(self, dataset_path: str | Path, fields: list[str] | None = None) -> ContaminationReport:
        """Scan every row of the candidate file and return its contamination rate."""
        report = ContaminationReport()
        if not self.enabled:
            return report
        p = Path(dataset_path)
        if not p.is_file():
            return report
        with p.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                report.total += 1
                row = _loads(line)
                if row is None:
                    continue
                hit = self._hit(_row_text(row, fields) if isinstance(row, dict) else str(row))
                if hit:
                    report.hits += 1
                    if len(report.examples) < MAX_EXAMPLES:
                        report.examples.append(hit)
        report.rate = report.hits / report.total if report.total else 0.0
        return report

    def _hit(self, text: str) -> str | None:
        toks = _tokens(text)
        if not toks:
            return None
        grams = _ngrams(toks, self.n)
        if grams:
            for g in grams:
                if g in self._ngrams:
                    return g
            return None
        joined = " ".join(toks)
        return joined if joined in self._exact else None


def build(
    reference_files: list[str] | None,
    n: int = DEFAULT_NGRAM,
    fields: list[str] | None = None,
) -> ContaminationIndex:
    index = ContaminationIndex(n=n)
    for path in reference_files or []:
        index.add_file(path, fields)
    return index


def _iter_texts(path: Path, fields: list[str] | None):
    # utf-8-sig: benchmark files often carry a BOM, which would make the first
    # row unparseable and silently drop it from the index.
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        with path.open("r", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                row = _loads(line)
                if isinstance(row, dict):
                    yield _row_text(row, fields)
                elif isinstance(row, str):
                    yield row
    elif suffix == ".json":
        data = _loads(path.read_text(encoding="utf-8-sig", errors="replace"))
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if isinstance(row, dict):
                yield _row_text(row, fields)
            elif isinstance(row, str):
                yield row
    else:
        with path.open("r", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                if line.strip():
                    yield line


def _row_text(row: dict, fields: list[str] | None) -> str:
    """Join a row's strings. Default is every string field, because field names
    rarely match between a candidate and a benchmark (question/problem/prompt...)."""
    def strings(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for nested in value.values():
                yield from strings(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from strings(nested)

    values = [row.get(k) for k in fields] if fields else row.values()
    picked = [text for value in values for text in strings(value)]
    return " ".join(p for p in picked if p)


def _loads(text: str):
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
