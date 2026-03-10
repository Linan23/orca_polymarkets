"""Lightweight repository secret scan for CI.

This scanner is intentionally strict for high-risk patterns:
- tracked private key files (`*.pem`, `*.key`)
- private key block text inside tracked files
- non-placeholder hardcoded Dune API keys
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
import sys
from pathlib import Path


PRIVATE_KEY_BLOCK = re.compile(r"-----BEGIN (?:RSA )?PRIVATE KEY-----")
DUNE_KEY_ASSIGN = re.compile(r"^\s*DUNE_API_KEY\s*=\s*([^\s#]+)", re.IGNORECASE)
PLACEHOLDER_FRAGMENTS = (
    "your_",
    "example",
    "<",
    ">",
    "changeme",
    "${",
)
FORBIDDEN_FILE_PATTERNS = ("*.pem", "*.key")
EXCLUDED_PATH_PATTERNS = (
    ".venv/*",
    "venv/*",
    "env/*",
    "__pycache__/*",
)


def tracked_files() -> list[str]:
    """Return the tracked file list from git."""
    proc = subprocess.run(
        ["git", "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    raw = proc.stdout.decode("utf-8", errors="ignore")
    return [item for item in raw.split("\0") if item]


def is_excluded(path: str) -> bool:
    """Return whether the path should be excluded from scanning."""
    return any(fnmatch.fnmatch(path, pattern) for pattern in EXCLUDED_PATH_PATTERNS)


def scan_file(path: Path) -> list[str]:
    """Scan one tracked file and return violation messages."""
    violations: list[str] = []
    text: str
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Binary-ish tracked files are ignored unless extension itself is forbidden.
        return violations
    except OSError as exc:
        violations.append(f"{path}: unable to read file ({exc})")
        return violations

    if PRIVATE_KEY_BLOCK.search(text):
        violations.append(f"{path}: contains private key block text")

    for line_no, line in enumerate(text.splitlines(), start=1):
        match = DUNE_KEY_ASSIGN.search(line)
        if not match:
            continue
        value = match.group(1).strip().strip("\"'").lower()
        if not value:
            continue
        if any(fragment in value for fragment in PLACEHOLDER_FRAGMENTS):
            continue
        violations.append(f"{path}:{line_no}: hardcoded DUNE_API_KEY value")

    return violations


def main() -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Scan tracked repo files for high-risk secret patterns.")
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root path (default: current directory).",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    files = tracked_files()
    violations: list[str] = []

    for relative in files:
        if is_excluded(relative):
            continue
        if any(fnmatch.fnmatch(relative, pattern) for pattern in FORBIDDEN_FILE_PATTERNS):
            violations.append(f"{relative}: forbidden tracked file type")
            continue
        violations.extend(scan_file(repo_root / relative))

    if violations:
        print("Secret scan failed:")
        for item in violations:
            print(f"- {item}")
        return 1

    print("Secret scan passed: no high-risk patterns found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
