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


def file_proof(path):
    body = guard.read_file(path)
    return {"path": str(path), "sha256": guard.digest(body), "bytes": len(body), "mtime_ns": path.stat().st_mtime_ns}


def source_proof(root):
    return {relative: file_proof(root / relative) for relative in guard.source_files(root)}


def integration_corpus(stdout, root, target):
    libraries = []
    executables = []
    rows = {}
    values = []
    declared = None
    summary = None
    for line in stdout.splitlines():
        if line.startswith("{"):
            message = json.loads(line)
            if message.get("reason") != "compiler-artifact":
                continue
            owner = message.get("target", {})
            if owner.get("name") == "fixture_api" and owner.get("kind") == ["lib"]:
                guard.require(Path(owner["src_path"]) == root / "api/src/lib.rs", "wrong fixture library source owner")
                libraries.extend(Path(path) for path in message["filenames"] if path.endswith(".rlib"))
            if message.get("executable") and message.get("profile", {}).get("test"):
                guard.require(owner.get("name") == "current" and owner.get("kind") == ["test"] and Path(owner["src_path"]) == root / "consumer/tests/current.rs", "wrong fixture integration source owner")
                executables.append(Path(message["executable"]))
        elif match := re.fullmatch(r"running (\d+) tests?", line):
            guard.require(declared is None, "duplicate integration harness")
            declared = int(match[1])
        elif match := re.fullmatch(r"test (\S+) \.\.\. (ok|FAILED|ignored)(?:, .*)?", line):
            guard.require(match[1] not in rows, "duplicate integration test identity")
            rows[match[1]] = match[2]
        elif line.startswith("test result:"):
            guard.require(summary is None, "duplicate integration summary")
            summary = re.fullmatch(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s", line)
        if match := re.search(r"(?:^|\s)fixture_value=([12])(?:\s|$)", line):
            values.append(int(match[1]))
    guard.require(len(libraries) == len(executables) == 1 and declared == len(rows) == len(values) == 1 and summary, "incomplete integration artifacts/value/corpus")
    status = next(iter(rows.values()))
    counts = tuple(map(int, summary.groups()[1:]))
    guard.require((status, summary.group(1), counts) in (("ok", "ok", (1, 0, 0, 0, 0)), ("FAILED", "FAILED", (0, 1, 0, 0, 0))), "integration status/summary mismatch")
    library, executable = libraries[0], executables[0]
    guard.require(guard.canonical(library).parent == target / "debug/deps" and guard.canonical(executable).parent == target / "debug/deps", "integration artifacts escaped the owned target")
    library_hash = re.fullmatch(r"libfixture_api-([0-9a-f]+)\.rlib", library.name)
    executable_hash = re.fullmatch(r"current-([0-9a-f]+)", executable.name)
    guard.require(library_hash and executable_hash, "unexpected pinned fixture artifact layout")
    library_directory = target / "debug/.fingerprint" / ("fixture-api-" + library_hash[1])
    test_directory = target / "debug/.fingerprint" / ("fixture-consumer-" + executable_hash[1])
    fingerprint = guard.read_file(library_directory / "lib-fixture_api").decode("ascii").strip()
    guard.require(re.fullmatch(r"[0-9a-f]{16}", fingerprint), "invalid library fingerprint")
    test_metadata = guard.read_json(test_directory / "test-integration-test-current.json")
    dependencies = test_metadata.get("deps", [])
    guard.require(isinstance(dependencies, list) and all(isinstance(entry, list) and len(entry) == 4 for entry in dependencies), "malformed integration dependencies")
    linked = [entry for entry in dependencies if entry[1] == "fixture_api"]
    guard.require(len(linked) == 1 and linked[0][2] is False and linked[0][3] == int.from_bytes(bytes.fromhex(fingerprint), "little"), "integration did not bind the recorded library fingerprint")
    return {"tests": rows, "value": values[0], "library": file_proof(library), "executable": file_proof(executable), "library_fingerprint": fingerprint, "linked_dependency": linked[0], "library_metadata": file_proof(library_directory / "lib-fixture_api.json"), "test_metadata": file_proof(test_directory / "test-integration-test-current.json")}


def original_stdout(result, directory):
    for name in ("stdout", "stderr"):
        recorded = result[name]
        path = guard.canonical(recorded["path"])
        guard.require(path == directory / ("cargo." + name), "original output escaped its phase")
        body = guard.read_file(path)
        guard.require(len(body) == recorded["bytes"] and guard.digest(body) == recorded["sha256"], "original output changed")
    return Path(result["stdout"]["path"]).read_text()


def guarded_phase(directory, name, root, target, command, identity, value, dirty):
    phase = directory / name
    report = {"root": str(root), "target": str(target), "source": source_proof(root), "expected_test": identity, "expected_value": value}
    try:
        guard.run(root, target, command, phase, 1024**3, 120)
        receipt = guard.read_json(phase / "receipt.json")
        guard.require(receipt["accepted"] and receipt["invalidated"] == dirty and receipt["source_before"] == receipt["source_after"], "wrong guarded phase invalidation/input result")
        result = receipt["commands"][-1]
        report.update(receipt=str(phase / "receipt.json"), original=result, source_after=source_proof(root))
        guard.require(report["source"] == report["source_after"], "guarded phase source/timestamps changed")
        guard.require(result["exit_code"] == 0 and result["command"] == guard.validate_command(command), "wrong original guarded command/exit")
        proof = integration_corpus(original_stdout(result, phase), root, target)
        report["artifacts"] = proof
        guard.require(proof["tests"] == {identity: "ok"} and proof["value"] == value, "guarded phase used wrong source-owned test/library value")
        report["accepted"] = True
        return report
    except (guard.Refused, OSError, ValueError) as error:
        report.update(accepted=False, refusal=str(error))
        raise
    finally:
        if phase.is_dir():
            guard.write_json(phase / "phase-proof.json", report)


def raw_phase(directory, name, root, target, command, identity, value, previous):
    phase = directory / name
    phase.mkdir()
    environment = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="2", CARGO_INCREMENTAL="0", CARGO_TERM_COLOR="never")
    report = {"root": str(root), "target": str(target), "source": source_proof(root), "expected_test": identity, "expected_value": value, "previous_artifacts": previous["artifacts"]}
    try:
        result = guard.original_process(guard.validate_command(command), root, environment, phase, "cargo", 120)
        report["original"] = result
        report["source_after"] = source_proof(root)
        guard.require(report["source"] == report["source_after"], "raw phase source/timestamps changed")
        proof = integration_corpus(original_stdout(result, phase), root, target)
        report["artifacts"] = proof
        if result["exit_code"] == 0 and proof["tests"] == {identity: "ok"} and proof["value"] == value:
            observed_stale = False
        else:
            prior = previous["artifacts"]
            stderr = Path(result["stderr"]["path"]).read_text()
            guard.require(result["exit_code"] == 101 and proof["tests"] == {identity: "FAILED"} and proof["value"] == prior["value"] != value and "fixture api value mismatch" in stderr and proof["library"]["sha256"] == prior["library"]["sha256"] and proof["library_fingerprint"] == prior["library_fingerprint"] and proof["linked_dependency"] == prior["linked_dependency"], "raw failure is not proven stale-library reuse")
            observed_stale = True
        report["observed_stale"] = observed_stale
        return report
    except (guard.Refused, OSError, ValueError) as error:
        report["refusal"] = str(error)
        raise
    finally:
        guard.write_json(phase / "phase-proof.json", report)


def write_phase(root, identity, value, previous=None, age_all=False):
    library = root / "api/src/lib.rs"
    test = root / "consumer/tests/current.rs"
    if previous:
        artifacts = previous["artifacts"]
        earlier = min(artifacts["library"]["mtime_ns"], artifacts["executable"]["mtime_ns"]) - 3_600_000_000_000
        guard.require(earlier > 0, "invalid artifact timestamp boundary")
        if age_all:
            for path in root.rglob("*"):
                if path.is_file():
                    os.utime(path, ns=(earlier, earlier))
    library.write_text(f"pub fn value() -> u32 {{ {value} }}\n")
    test.write_text(f'#[test]\nfn {identity}() {{\n    let actual = fixture_api::value();\n    println!("fixture_value={{actual}}");\n    assert_eq!(actual, {value}, "fixture api value mismatch");\n}}\n')
    if previous:
        os.utime(library, ns=(earlier, earlier))
        now = time.time_ns()
        os.utime(test, ns=(now, now))
        guard.require(library.stat().st_mtime_ns < artifacts["library"]["mtime_ns"] and test.stat().st_mtime_ns > artifacts["executable"]["mtime_ns"], "older-library/new-test mtime frontier not established")


def prepare(root, identity, value):
    (root / "api/src").mkdir(parents=True)
    (root / "consumer/src").mkdir(parents=True)
    (root / "consumer/tests").mkdir()
    (root / "Cargo.toml").write_text('[workspace]\nresolver="3"\nmembers=["api","consumer"]\n')
    (root / "rust-toolchain.toml").write_text('[toolchain]\nchannel="1.97.1"\nprofile="minimal"\n')
    (root / "Cargo.lock").write_text('version = 4\n\n[[package]]\nname = "fixture-api"\nversion = "0.1.0"\n\n[[package]]\nname = "fixture-consumer"\nversion = "0.1.0"\ndependencies = ["fixture-api"]\n')
    (root / "api/Cargo.toml").write_text('[package]\nname="fixture-api"\nversion="0.1.0"\nedition="2024"\n[lib]\ndoctest=false\n')
    (root / "consumer/Cargo.toml").write_text('[package]\nname="fixture-consumer"\nversion="0.1.0"\nedition="2024"\n[lib]\ndoctest=false\n[dependencies]\nfixture-api={path="../api"}\n')
    (root / "consumer/src/lib.rs").write_text("pub fn anchor() {}\n")
    write_phase(root, identity, value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new external fixture directory; retained, never cleaned automatically")
    args = parser.parse_args()
    directory = guard.canonical(args.directory)
    guard.require(not directory.exists(), "fixture directory must be new")
    directory.mkdir()
    a = directory / "a"
    b = directory / "b"
    prepare(a, "a_current_library", 1)
    prepare(b, "b_current_library", 2)
    target = directory / "shared" / "target"
    target.parent.mkdir()
    guard.initialize(a, target)
    command = ["cargo", "test", "-p", "fixture-consumer", "--test", "current", "--message-format", "json", "--", "--nocapture"]
    packages = ["fixture-api", "fixture-consumer"]
    phases = []
    first_a = guarded_phase(directory, "01-a-guarded", a, target, command, "a_current_library", 1, [])
    phases.append(first_a)
    phases.append(guarded_phase(directory, "02-a-reuse", a, target, command, "a_current_library", 1, []))
    write_phase(b, "b_current_library", 2, first_a, age_all=True)
    phases.append(raw_phase(directory, "03-b-raw", b, target, command, "b_current_library", 2, first_a))
    reference_target = directory / "reference" / "target"
    reference_target.parent.mkdir()
    guard.initialize(b, reference_target)
    phases.append(guarded_phase(directory, "04-b-reference", b, reference_target, command, "b_current_library", 2, []))
    guarded_b = guarded_phase(directory, "05-b-guarded", b, target, command, "b_current_library", 2, packages)
    phases.append(guarded_b)
    write_phase(a, "a_return_current_library", 1, guarded_b)
    phases.append(raw_phase(directory, "06-return-a-raw", a, target, command, "a_return_current_library", 1, guarded_b))
    phases.append(guarded_phase(directory, "07-return-a-reference", a, reference_target, command, "a_return_current_library", 1, packages))
    returned_a = guarded_phase(directory, "08-return-a-guarded", a, target, command, "a_return_current_library", 1, packages)
    phases.append(returned_a)
    write_phase(a, "same_root_b_current_library", 2, returned_a)
    phases.append(raw_phase(directory, "09-same-root-b-raw", a, target, command, "same_root_b_current_library", 2, returned_a))
    phases.append(guarded_phase(directory, "10-same-root-b-reference", a, reference_target, command, "same_root_b_current_library", 2, packages))
    same_root_b = guarded_phase(directory, "11-same-root-b-guarded", a, target, command, "same_root_b_current_library", 2, packages)
    phases.append(same_root_b)
    write_phase(a, "same_root_a_current_library", 1, same_root_b)
    phases.append(raw_phase(directory, "12-same-root-a-raw", a, target, command, "same_root_a_current_library", 1, same_root_b))
    phases.append(guarded_phase(directory, "13-same-root-a-reference", a, reference_target, command, "same_root_a_current_library", 1, packages))
    phases.append(guarded_phase(directory, "14-same-root-a-guarded", a, target, command, "same_root_a_current_library", 1, packages))
    phases.append(guarded_phase(directory, "15-same-root-a-reuse", a, target, command, "same_root_a_current_library", 1, []))
    probes = [phase["observed_stale"] for phase in phases if "observed_stale" in phase]
    guard.write_json(directory / "comparison.json", {"phases": phases, "observed_stale": probes, "qualification": "Raw probes may be fresh; only an exact current test/wrong API value/retained linked-library proof counts as stale. Guarded corpus acceptance is separate from helper exit zero. No live inputs or caches are adopted/copied."})
    print(json.dumps({"observed_stale": probes, "comparison": str(directory / "comparison.json")}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (guard.Refused, OSError, ValueError) as error:
        print(f"refused: {error}", file=sys.stderr)
        sys.exit(1)
