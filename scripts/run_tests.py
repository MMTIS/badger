#!/usr/bin/env python
"""Run the unittest suite and emit a GitHub-flavored Markdown results table.

Run from the repository root: ``uv run python scripts/run_tests.py``.
"""

import os
import time
import unittest
from collections import OrderedDict
from typing import Any


class _Tally:
    __slots__ = ("tests", "passed", "failed", "skipped", "seconds")

    def __init__(self) -> None:
        self.tests = 0
        self.passed = 0
        self.failed = 0  # failures + errors
        self.skipped = 0
        self.seconds = 0.0


class MarkdownResult(unittest.TextTestResult):
    """A TextTestResult that additionally tallies outcomes per TestCase class."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._starts: dict[unittest.TestCase, float] = {}
        self.suites: "OrderedDict[str, _Tally]" = OrderedDict()

    def _bucket(self, test: unittest.TestCase) -> _Tally:
        key = f"{type(test).__module__}.{type(test).__qualname__}"
        return self.suites.setdefault(key, _Tally())

    def startTest(self, test: unittest.TestCase) -> None:
        super().startTest(test)
        self._starts[test] = time.perf_counter()

    def stopTest(self, test: unittest.TestCase) -> None:
        super().stopTest(test)
        bucket = self._bucket(test)
        bucket.tests += 1
        bucket.seconds += time.perf_counter() - self._starts.pop(test, time.perf_counter())

    def addSuccess(self, test: unittest.TestCase) -> None:
        super().addSuccess(test)
        self._bucket(test).passed += 1

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self._bucket(test).failed += 1

    def addError(self, test: unittest.TestCase, err: Any) -> None:
        super().addError(test, err)
        self._bucket(test).failed += 1

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self._bucket(test).skipped += 1


def _render(result: MarkdownResult) -> str:
    lines = [
        "## Test results",
        "",
        "| Suite | Tests | ✅ | ❌ | ⎭ | Time |",
        "|-------|------:|--:|--:|--:|-----:|",
    ]
    total = _Tally()
    for name, bucket in result.suites.items():
        total.tests += bucket.tests
        total.passed += bucket.passed
        total.failed += bucket.failed
        total.skipped += bucket.skipped
        total.seconds += bucket.seconds
        lines.append(f"| {name} | {bucket.tests} | {bucket.passed} | {bucket.failed} | {bucket.skipped} | {bucket.seconds:.2f}s |")
    lines.append(f"| **TOTAL** | **{total.tests}** | **{total.passed}** | **{total.failed}** | **{total.skipped}** | **{total.seconds:.2f}s** |")
    return "\n".join(lines) + "\n"


def main() -> int:
    suite = unittest.TestLoader().discover(start_dir="tests", pattern="test*.py", top_level_dir=".")
    result = unittest.TextTestRunner(resultclass=MarkdownResult, verbosity=2).run(suite)
    assert isinstance(result, MarkdownResult)

    table = _render(result)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write(table)
    else:
        print("\n" + table)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
