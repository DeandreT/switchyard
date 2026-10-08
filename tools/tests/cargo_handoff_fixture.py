"""Small opt-in reproducer; mutates timestamps only in its new fixture directory."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import time


SPEC = importlib.util.spec_from_file_location("cargo_handoff", Path(__file__).parents[1] / "cargo_handoff.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


def corpus(stdout, root, target, expected):
    artifacts = []
    rows = {}
    declared = None
    summary = None
    for line in stdout.splitlines():
        if line.startswith("{"):
            message = json.loads(line)
            if message.get("reason") == "compiler-artifact" and message.get("executable") and message.get("profile", {}).get("test"):
                artifact = message["executable"]
                guard.require(guard.canonical(artifact).is_relative_to(target) and Path(message["target"]["src_path"]) == root / "api/src/lib.rs", "wrong fixture executable/source owner")
                artifacts.append(artifact)
        elif match := re.fullmatch(r"running (\d+) tests?", line):
            guard.require(declared is None, "duplicate fixture harness")
            declared = int(match[1])
        elif match := re.fullmatch(r"test (\S+) \.\.\. (ok|FAILED|ignored)(?:, .*)?", line):
            guard.require(match[1] not in rows, "duplicate fixture test identity")
            rows[match[1]] = match[2]
        elif line.startswith("test result:"):
            guard.require(summary is None, "duplicate fixture summary")
            summary = re.fullmatch(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s", line)
    guard.require(len(artifacts) == 1 and declared == len(rows) == 1 and summary and tuple(map(int, summary.groups())) == (1, 0, 0, 0, 0), "incomplete fixture executable/corpus")
    guard.require(rows == expected, "missing/substituted/status-mismatched fixture test identity")
    return {"executable": artifacts[0], "executable_sha256": guard.digest(Path(artifacts[0]).read_bytes()), "tests": rows}


def prepare(root, label, value):
    (root / "api/src").mkdir(parents=True)
    (root / "Cargo.toml").write_text('[workspace]\nresolver="3"\nmembers=["api"]\n')
    (root / "rust-toolchain.toml").write_text('[toolchain]\nchannel="1.97.1"\nprofile="minimal"\n')
    (root / "Cargo.lock").write_text('version = 4\n\n[[package]]\nname = "handoff-fixture"\nversion = "0.1.0"\n')
    (root / "api/Cargo.toml").write_text('[package]\nname="handoff-fixture"\nversion="0.1.0"\nedition="2024"\n[lib]\ndoctest=false\n')
    (root / "api/src/lib.rs").write_text(f'pub fn {label}_api()->u32 {{ {value} }}\n#[test]\nfn {label}_only() {{ assert_eq!({label}_api(), {value}); }}\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new external fixture directory; retained, never cleaned automatically")
    args = parser.parse_args()
    directory = guard.canonical(args.directory)
    guard.require(not directory.exists(), "fixture directory must be new")
    directory.mkdir()
    old = directory / "old"
    new = directory / "new"
    prepare(old, "old", 1)
    prepare(new, "new", 2)
    earlier = time.time_ns() - 3_600_000_000_000
    for path in new.rglob("*"):
        if path.is_file():
            os.utime(path, ns=(earlier, earlier))
    target = directory / "shared" / "target"
    target.parent.mkdir()
    guard.initialize(old, target)
    command = ["cargo", "test", "--workspace", "--message-format", "json"]
    guard.run(old, target, command, directory / "old-guarded", 1024**3, 120)
    old_receipt = guard.read_json(directory / "old-guarded/receipt.json")
    old_stdout = Path(old_receipt["commands"][-1]["stdout"]["path"]).read_text()
    old_corpus = corpus(old_stdout, old, target, {"old_only": "ok"})
    environment = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="2", CARGO_INCREMENTAL="0", CARGO_TERM_COLOR="never")
    raw = directory / "unfenced"
    raw.mkdir()
    original = guard.original_process(guard.validate_command(command), new, environment, raw, "cargo", 120)
    guard.require(original["exit_code"] == 0, "unfenced original Cargo failed")
    unfenced_stdout = Path(original["stdout"]["path"]).read_text()
    try:
        unfenced = corpus(unfenced_stdout, new, target, {"new_only": "ok"})
        stale = False
    except guard.Refused:
        unfenced = corpus(unfenced_stdout, new, target, {"old_only": "ok"})
        stale = True
    reference_target = directory / "reference" / "target"
    reference_target.parent.mkdir()
    guard.initialize(new, reference_target)
    guard.run(new, reference_target, command, directory / "reference-guarded", 1024**3, 120)
    reference_receipt = guard.read_json(directory / "reference-guarded/receipt.json")
    reference = corpus(Path(reference_receipt["commands"][-1]["stdout"]["path"]).read_text(), new, reference_target, {"new_only": "ok"})
    guard.run(new, target, command, directory / "new-guarded", 1024**3, 120)
    fenced_receipt = guard.read_json(directory / "new-guarded/receipt.json")
    fenced = corpus(Path(fenced_receipt["commands"][-1]["stdout"]["path"]).read_text(), new, target, {"new_only": "ok"})
    guard.require(fenced_receipt["invalidated"] == ["handoff-fixture"], "handoff did not invalidate exactly its owned package")
    guard.write_json(directory / "comparison.json", {"old": old_corpus, "unfenced_command": original, "unfenced": unfenced, "observed_stale": stale, "fresh_reference": reference, "fenced": fenced})
    print(json.dumps({"observed_stale": stale, "comparison": str(directory / "comparison.json")}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (guard.Refused, OSError, ValueError) as error:
        print(f"refused: {error}", file=sys.stderr)
        sys.exit(1)
