#!/usr/bin/env python3
"""Bound an original test process, including synchronous destruction.

Output is inherited so --nocapture phase markers reach CI immediately. A timed
out process is a failed test, never evidence of graceful task or store cleanup.
"""

import argparse
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys


def cargo_test_executable(lines, target):
    executables = set()
    for line in lines:
        artifact = json.loads(line)
        if (
            artifact.get("reason") == "compiler-artifact"
            and artifact.get("target", {}).get("name") == target
            and "test" in artifact.get("target", {}).get("kind", [])
            and artifact.get("profile", {}).get("test") is True
            and artifact.get("executable")
        ):
            executables.add(artifact["executable"])
    if len(executables) != 1:
        raise ValueError(f"expected exactly one compiled test executable for {target}")
    executable = Path(executables.pop())
    if not executable.is_absolute() or not executable.is_file():
        raise ValueError("compiled test executable must be an existing absolute path")
    return str(executable)


def capture_stacks(pid):
    gdb = shutil.which("gdb")
    if gdb is None:
        print("bounded-test: gdb unavailable; no stacks captured", file=sys.stderr, flush=True)
        return
    try:
        result = subprocess.run(
            [gdb, "-batch", "-nx", "-nh", "-ex", "set pagination off", "-ex",
             "thread apply all bt 32", "-p", str(pid)],
            timeout=10,
            check=False,
        )
        if result.returncode:
            print("bounded-test: stack capture failed (see diagnostics above)",
                  file=sys.stderr, flush=True)
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"bounded-test: stack capture unavailable: {error}", file=sys.stderr, flush=True)


def kill_original_group(child):
    # The original remains unreaped until after this signal. Its PID/PGID cannot
    # be reused in between diagnosis and group termination, even if it exits.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait(timeout=10)


def run(command, timeout, stacks=False):
    if os.name != "posix":
        raise ValueError("bounded-test requires POSIX process-group ownership")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    print(f"bounded-test: original process deadline {timeout:g}s", file=sys.stderr, flush=True)
    child = subprocess.Popen(command, start_new_session=True)
    try:
        return child.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print("bounded-test: DEADLINE EXPIRED; inspect per-test markers for incomplete phases. "
              "Termination is not successful drain; do not reopen its store.",
              file=sys.stderr, flush=True)
        try:
            if stacks:
                capture_stacks(child.pid)
        finally:
            kill_original_group(child)
        return 124
    except BaseException:
        kill_original_group(child)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, required=True)
    parser.add_argument("--stacks", action="store_true")
    parser.add_argument("--cargo-artifacts", type=Path)
    parser.add_argument("--test-target")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    try:
        if bool(args.cargo_artifacts) != bool(args.test_target):
            raise ValueError("--cargo-artifacts and --test-target must be used together")
        if args.cargo_artifacts:
            with args.cargo_artifacts.open() as stream:
                command.insert(0, cargo_test_executable(stream, args.test_target))
        if not command:
            raise ValueError("a test command is required")
        return run(command, args.timeout, args.stacks)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"bounded-test: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
