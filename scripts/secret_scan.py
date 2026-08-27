"""Find high-risk credentials in the repository's text source and config files.

This scanner intentionally uses a small set of conservative textual rules. It
does not guess account numbers or attempt to decode arbitrary data. Every Git
workspace file is considered, including ignored backup and runtime files. The
exact root `.env` runtime credential store plus known generated, cache, and lock
artifacts are excluded, and binary files are skipped before their contents are
read.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import re
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]

_EXCLUDED_PARTS = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".venv",
        "__pycache__",
        "build",
        "cache",
        "coverage",
        "dist",
        "generated",
        "htmlcov",
        "node_modules",
        "venv",
    }
)
_EXCLUDED_FILENAMES = frozenset(
    {
        "cargo.lock",
        "package-lock.json",
        "pnpm-lock.yaml",
        "poetry.lock",
        "uv.lock",
        "yarn.lock",
    }
)
_EXCLUDED_FILES = frozenset(
    {
        # These files intentionally contain credential-shaped test vectors or
        # the scanner's own detection patterns. Keep the allowlist exact: all
        # other tests and fixture files must remain in the scan surface.
        "scripts/secret_scan.py",
        "tests/test_secret_scan.py",
    }
)
_EXCLUDED_FILENAME_PART = re.compile(
    r"(?:^|[_.-])(?:cache|generated)(?:[_.-]|$)", re.IGNORECASE
)

_PRIVATE_KEY = re.compile(
    r"-----BEGIN\s+(?:ENCRYPTED\s+)?(?:RSA\s+|EC\s+|DSA\s+|OPENSSH\s+)?PRIVATE\s+KEY-----",
    re.IGNORECASE,
)
_BEARER = re.compile(r"\bbearer\s+([^\s,;\"']+)", re.IGNORECASE)
_TOKEN_LITERALS = (
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    re.compile(r"\bsk-(?:proj-|live-)?[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b"),
    re.compile(r"\bGOCSPX-[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\bSG\.[0-9A-Za-z_-]{16,}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
)
_URL_CREDENTIAL = re.compile(r"\b(?:https?|postgres(?:ql)?|mysql)://[^\s/@:]+:([^\s/@]+)@", re.IGNORECASE)

# Keep this list specific enough that ordinary identifiers such as `key` and
# account numbers are not treated as credentials.
_SENSITIVE_KEY = (
    r"(?:"
    r"app[_-]?(?:key|secret(?:[_-]?key)?)|"
    r"(?:api|client|oauth)[_-]?secret|"
    r"secret[_-]?access[_-]?key|"
    r"(?:api|access|auth|refresh|service|webhook)[_-]?token|"
    r"(?:api|client|encryption|private|secret|signing)[_-]?key|"
    r"(?:authorization|basic[_-]?auth|bearer)|"
    r"(?:password|passwd|pwd|secret|token)"
    r")"
)
_ASSIGNMENT = re.compile(
    rf"(?P<key>[\"']?{_SENSITIVE_KEY}[\"']?)"
    r"(?:\s*:\s*[^=\n]+?(?=\s*=))?"
    r"\s*(?:=|:)\s*(?P<value>.*)$",
    re.IGNORECASE,
)
_STRING_PART = r"(?:\"[^\"\n]*\"|'[^'\n]*')"
_STRING_PART_RE = re.compile(_STRING_PART)
_STRING_EXPRESSION = re.compile(
    rf"^\s*(?P<part>{_STRING_PART})(?:(?:\s*\+\s*|\s+)(?P<next>{_STRING_PART}))*\s*$"
)
_QUOTED_PART = re.compile(r"^\s*([\"'])(.*)\1\s*$")

_PLACEHOLDER_EXACT = frozenset(
    {
        "changeme",
        "change-me",
        "change_me",
        "dummy",
        "example",
        "fake",
        "n/a",
        "na",
        "none",
        "null",
        "password",
        "placeholder",
        "redacted",
        "replace-me",
        "replace_me",
        "sample",
        "secret",
        "test",
        "testing",
        "token",
    }
)
_PLACEHOLDER_WORD = re.compile(r"(?:change|dummy|example|fake|placeholder|redacted|replace|sample|test|your)", re.IGNORECASE)
_PLACEHOLDER_WRAPPER = re.compile(
    r"^(?:<[^>]+>|\$\{[^}]+\}|\$\{\{[^}]+\}\}|\$[A-Za-z_][A-Za-z0-9_]*|\{[^{}]+\}|\{\{[^{}]+\}\})$"
)


@dataclass(frozen=True)
class Finding:
    """A location and category without including the secret value itself."""

    path: Path
    line: int
    kind: str
    detail: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.kind}: {self.detail}"


def _is_placeholder(value: str) -> bool:
    value = value.strip().strip(",;\"'")
    if not value:
        return False
    normalized = value.casefold()
    if normalized in _PLACEHOLDER_EXACT or _PLACEHOLDER_WRAPPER.fullmatch(value):
        return True
    if re.fullmatch(r"[x*_-]{3,}", normalized):
        return True
    # Compound placeholders such as `your-test-token` are safe examples, but
    # a bare `secret-token` remains suspicious and is intentionally detected.
    return bool(_PLACEHOLDER_WORD.search(normalized)) and any(
        marker in normalized for marker in ("your", "change", "dummy", "example", "fake", "placeholder", "redacted", "replace", "sample", "test")
    )


def _literal_value(raw_value: str, suffix: str) -> str | None:
    """Return a literal assignment value, including simple concatenation."""
    quote: str | None = None
    escaped = False
    end = len(raw_value)
    for index, character in enumerate(raw_value):
        if escaped:
            escaped = False
        elif quote and character == "\\":
            escaped = True
        elif quote and character == quote:
            quote = None
        elif not quote and character in "\"'":
            quote = character
        elif not quote and character == "#":
            end = index
            break
    value = raw_value[:end].strip()
    if suffix != ".py":
        if value.endswith("}") and not value.startswith(("${", "{{")):
            value = value[:-1].rstrip()
        value = value.rstrip(",]").strip()
    if not value:
        return None
    expression = _STRING_EXPRESSION.fullmatch(value)
    if expression:
        parts = [match.group(0) for match in _STRING_PART_RE.finditer(value)]
        decoded: list[str] = []
        for part in parts:
            quoted = _QUOTED_PART.fullmatch(part)
            if quoted:
                decoded.append(quoted.group(2))
        return "".join(decoded)
    if suffix == ".py":
        return None
    if value.startswith(("\"", "'")):
        quoted = _QUOTED_PART.match(value)
        return quoted.group(2) if quoted else None
    if value.startswith(("{", "[")):
        return None
    # JSON/TOML/YAML/.env permit bare scalars. Remove only syntax delimiters;
    # no numeric or account-number heuristic is used here.
    return value


def _finding(path: Path, line: int, kind: str, detail: str) -> Finding:
    return Finding(path=path, line=line, kind=kind, detail=detail)


def scan_text(text: str, path: Path = Path("<text>")) -> tuple[Finding, ...]:
    """Scan text and return deterministic, non-secret findings."""
    findings: list[Finding] = []
    suffix = path.suffix.casefold()
    for line_number, line in enumerate(text.splitlines(), 1):
        if _PRIVATE_KEY.search(line):
            findings.append(_finding(path, line_number, "private-key", "PEM private-key marker"))

        for match in _BEARER.finditer(line):
            candidate = match.group(1).rstrip(".\"")
            if candidate and not _is_placeholder(candidate):
                findings.append(_finding(path, line_number, "bearer-credential", "Bearer credential literal"))

        for pattern in _TOKEN_LITERALS:
            if pattern.search(line):
                findings.append(_finding(path, line_number, "high-risk-credential", "known credential prefix"))

        if _URL_CREDENTIAL.search(line):
            candidate = _URL_CREDENTIAL.search(line)
            assert candidate is not None
            if not _is_placeholder(candidate.group(1)):
                findings.append(_finding(path, line_number, "high-risk-credential", "URL contains password material"))

        assignment = _ASSIGNMENT.search(line)
        if assignment:
            literal = _literal_value(assignment.group("value"), suffix)
            if literal and not _is_placeholder(literal):
                key = assignment.group("key").strip("\"'")
                findings.append(_finding(path, line_number, "credential-assignment", f"non-empty {key} assignment"))

    return tuple(dict.fromkeys(findings))


def _relative(path: Path, root: Path) -> Path:
    try:
        return path.relative_to(root)
    except ValueError:
        return path


def _in_scope(relative: Path) -> bool:
    parts = tuple(part.casefold() for part in relative.parts)
    if not parts or any(part in _EXCLUDED_PARTS for part in parts):
        return False
    normalized = relative.as_posix().casefold()
    if normalized in _EXCLUDED_FILES:
        return False
    name = relative.name.casefold()
    if len(relative.parts) == 1 and name == ".env":
        return False
    if (
        name in _EXCLUDED_FILENAMES
        or name.endswith((".lock", ".generated", ".map"))
        or _EXCLUDED_FILENAME_PART.search(name)
    ):
        return False
    return True


def _workspace_files(root: Path) -> tuple[Path, ...]:
    """Use Git's workspace view, including ignored backup and runtime files."""
    result = subprocess.run(
        ["git", "-C", str(root), "ls-files", "--cached", "--others", "-z"],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        candidates = (root / Path(name) for name in result.stdout.decode("utf-8").split("\0") if name)
    else:
        candidates = root.rglob("*")
    files = []
    for path in candidates:
        relative = _relative(path, root)
        if _in_scope(relative) and path.is_file() and not path.is_symlink():
            files.append(path)
    return tuple(sorted(set(files), key=lambda path: path.as_posix().casefold()))


def scan_repository(root: Path = ROOT) -> tuple[Finding, ...]:
    """Scan eligible workspace files while excluding binary and generated data."""
    findings: list[Finding] = []
    for path in _workspace_files(root):
        try:
            content = path.read_bytes()
            if b"\0" in content:
                continue
            text = content.decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        findings.extend(scan_text(text, path))
    return tuple(sorted(set(findings), key=lambda item: (item.path.as_posix().casefold(), item.line, item.kind)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    findings = scan_repository(args.root.resolve())
    if findings:
        for finding in findings:
            print(finding)
        return 1
    print("Secret scan passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
