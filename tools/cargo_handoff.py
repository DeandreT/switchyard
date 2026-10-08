#!/usr/bin/env python3
"""Opt-in content fence for an explicitly owned, serialized Linux Cargo target."""

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import time
import tomllib
import uuid


MARKER = ".switchyard-cargo-owner.json"
GUARD = ".switchyard-cargo-guard"
LOCKS = (".cargo-lock", ".cargo-build-lock", ".cargo-artifact-lock")
MAX_FILE = 16 * 1024 * 1024
MAX_SOURCE = 64 * 1024 * 1024
MAX_OUTPUT = 16 * 1024 * 1024
LOCAL_FILESYSTEMS = {"ext2/ext3", "btrfs", "xfs", "tmpfs"}


class Refused(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Refused(reason)


def digest(value):
    if not isinstance(value, bytes):
        value = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(value).hexdigest()


def read_file(path):
    before = path.stat(follow_symlinks=False)
    require(path.is_file() and not path.is_symlink(), f"not a regular file: {path}")
    require(before.st_size <= MAX_FILE, f"oversized input: {path}")
    body = path.read_bytes()
    after = path.stat(follow_symlinks=False)
    identity = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    require(identity(before) == identity(after) and len(body) == before.st_size, f"input changed: {path}")
    return body


def read_json(path):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(read_file(path), object_pairs_hook=unique_pairs)


def write_json(path, value):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex)
    with temporary.open("x", encoding="ascii") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def canonical(path):
    path = Path(os.path.abspath(path))
    require(path == path.resolve(), f"symlinked path: {path}")
    return path


def workspace(root):
    manifest = tomllib.loads(read_file(root / "Cargo.toml").decode())
    members = manifest.get("workspace", {}).get("members", [])
    require(members and not manifest.get("package"), "a virtual workspace is required")
    require(not manifest["workspace"].get("exclude"), "workspace exclusions are unsupported")
    packages = {}
    paths = {}
    versions = {}
    for member in members:
        require(isinstance(member, str) and not any(ch in member for ch in "*?[]"), "literal workspace members are required")
        path = canonical(root / member)
        require(path.is_relative_to(root) and path != root, "external workspace member")
        data = tomllib.loads(read_file(path / "Cargo.toml").decode())
        name = data.get("package", {}).get("name")
        require(isinstance(name, str) and name not in packages, "invalid or duplicate package name")
        version = data["package"].get("version")
        if isinstance(version, dict):
            require(set(version) == {"workspace"} and version["workspace"] is True, f"unsupported inherited package version: {name}")
            inherited_package = manifest["workspace"].get("package", {})
            require(isinstance(inherited_package, dict), "invalid workspace package defaults")
            version = inherited_package.get("version")
        require(isinstance(version, str) and version, f"explicit or exact workspace package version required: {name}")
        packages[name] = data
        paths[name] = path.relative_to(root).as_posix()
        versions[name] = version
    dependencies = {name: set() for name in packages}
    inherited = manifest.get("workspace", {}).get("dependencies", {})
    for name, data in packages.items():
        tables = [data, *data.get("target", {}).values()]
        for table in tables:
            for section in ("dependencies", "dev-dependencies", "build-dependencies"):
                for alias, spec in table.get(section, {}).items():
                    if not isinstance(spec, dict):
                        continue
                    if spec.get("workspace"):
                        require(alias in inherited, f"unresolved workspace dependency: {alias}")
                        spec = inherited[alias]
                    if not isinstance(spec, dict) or "path" not in spec:
                        continue
                    base = root if table.get(section, {}).get(alias, {}).get("workspace") else root / paths[name]
                    destination = canonical(base / spec["path"])
                    matches = [owner for owner, relative in paths.items() if root / relative == destination]
                    require(len(matches) == 1, f"unowned path dependency: {destination}")
                    dependencies[name].add(matches[0])
    # Cargo 1.97 clean ignores path qualifiers and removes every version by name.
    lock = tomllib.loads(read_file(root / "Cargo.lock").decode())
    entries = lock.get("package", [])
    require(lock.get("version") == 4 and isinstance(entries, list) and all(isinstance(entry, dict) for entry in entries), "a structured version-4 Cargo.lock is required")
    for name, version in versions.items():
        matches = [entry for entry in entries if entry.get("name") == name]
        require(len(matches) == 1, f"ambiguous locked owned package name: {name}")
        require(matches[0].get("version") == version and "source" not in matches[0] and "checksum" not in matches[0], f"locked owned package version/source mismatch: {name}")
    return paths, dependencies


def source_files(root):
    result = {}
    size = 0
    for directory, children, names in os.walk(root, followlinks=False):
        children[:] = sorted(child for child in children if child not in {".git", "__pycache__"})
        for child in children:
            require(not (Path(directory) / child).is_symlink(), "symlinked source directory")
        for name in sorted(names):
            if name == ".git" or name.endswith(".pyc"):
                continue
            path = Path(directory) / name
            body = read_file(path)
            size += len(body)
            require(size <= MAX_SOURCE and len(result) < 4096, "source tree is too large; generated outputs must be external")
            result[path.relative_to(root).as_posix()] = digest(body)
    return result


def configuration(root, environment):
    require(not any(environment.get(key) for key in ("RUSTC", "RUSTDOC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET", "CARGO_BUILD_BUILD_DIR")), "wrappers or custom Cargo layouts are unsupported")
    files = {}
    directories = [parent / ".cargo" for parent in (root, *root.parents)]
    directories.append(Path(environment.get("CARGO_HOME", str(Path.home() / ".cargo"))))
    for directory in dict.fromkeys(directories):
        for name in ("config", "config.toml"):
            path = directory / name
            if not path.exists():
                continue
            body = read_file(path)
            data = tomllib.loads(body.decode())
            require(not any(key in data for key in ("include", "unstable", "env", "target", "host")), f"unsupported Cargo configuration: {path}")
            build = data.get("build", {})
            require(not any(key in build for key in ("target", "target-dir", "build-dir", "rustc", "rustc-wrapper", "rustc-workspace-wrapper")), f"custom Cargo layout/tool is unsupported: {path}")
            files[str(canonical(path))] = digest(body)
    flags = {key: value for key, value in environment.items() if key.startswith(("CARGO_", "RUST"))}
    flags.pop("CARGO_TARGET_DIR", None)
    return {"files": files, "environment": flags}


def snapshot(root, command, environment, toolchain):
    paths, dependencies = workspace(root)
    files = source_files(root)
    package_hashes = {}
    owned = set()
    for name, relative in paths.items():
        selected = {path: value for path, value in files.items() if path.startswith(relative + "/")}
        owned.update(selected)
        package_hashes[name] = digest(selected)
    # An unmapped file may be a build/include input; prefer invalidation to guessing.
    global_inputs = {path: value for path, value in files.items() if path not in owned}
    return {"layout": paths, "dependencies": {name: sorted(value) for name, value in dependencies.items()}, "packages": package_hashes, "global": digest({"root": str(canonical(root)), "files": global_inputs, "helper": digest(read_file(Path(__file__))), "configuration": configuration(root, environment), "toolchain": toolchain, "graph": command}), "source": digest(files)}


def invalidated(previous, current, pending=False):
    names = set(current["packages"])
    if pending or previous is None or previous["global"] != current["global"] or previous["layout"] != current["layout"]:
        return sorted(names)
    changed = {name for name in names if previous["packages"].get(name) != current["packages"][name]}
    while True:
        expanded = changed | {name for name, dependencies in current["dependencies"].items() if changed.intersection(dependencies)}
        if expanded == changed:
            return sorted(changed)
        changed = expanded


def pristine_bootstrap(target, marker):
    if marker["snapshot"] is not None:
        require(target.is_dir(), "previously populated target is missing")
        return False
    if marker["pending"]:
        tag = target / "CACHEDIR.TAG"
        require(tag.is_file() and not tag.is_symlink(), "interrupted initial target requires Cargo-tagged cleanup")
        return False
    require(not target.exists(), "foreign initial target; Cargo must create it")
    return True


def owner_entries(target):
    parent = target.parent
    entries = {path.name for path in parent.iterdir()}
    require(MARKER in entries and entries <= {MARKER, GUARD, target.name}, "foreign entry in owned parent")
    for name in entries:
        if name == target.name:
            continue
        path = parent / name
        require(path.is_file() and not path.is_symlink() and path.stat().st_uid == os.getuid(), "uncertain bootstrap entry")
    require(GUARD not in entries or (parent / GUARD).stat().st_size == 0, "foreign guard contents")


@contextlib.contextmanager
def exclusive(path):
    require(not path.is_symlink(), f"symlinked lock: {path}")
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "r+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refused(f"active lock: {path}") from error
        yield stream


def idle_target(target):
    # Probes are released before invoking Cargo, which must take its own locks.
    with contextlib.ExitStack() as stack:
        for profile in ("debug", "release"):
            require(not (target / profile).is_symlink(), "symlinked Cargo profile")
            for name in LOCKS:
                path = target / profile / name
                if path.exists():
                    stack.enter_context(exclusive(path))


def target_path(root, target):
    target = canonical(target)
    require(target.name == "target" and not target.is_relative_to(root), "target must be a separate external directory named target")
    require(not target.is_relative_to(Path.home()) and target.parent != Path("/"), "home/system targets are refused")
    require(target.parent.is_dir() and target.parent.stat().st_uid == os.getuid(), "target parent ownership is uncertain")
    require(not target.exists() or target.is_dir() and target.stat().st_uid == os.getuid(), "target ownership is uncertain")
    filesystem = subprocess.check_output(["stat", "-f", "--format=%T", str(target.parent)], text=True, timeout=10).strip()
    require(filesystem in LOCAL_FILESYSTEMS, f"unsupported filesystem: {filesystem}")
    return target


def initialize(root, target):
    paths, _ = workspace(root)
    target = canonical(target)
    require(target.name == "target" and not target.is_relative_to(root) and not target.is_relative_to(Path.home()) and target.parent != Path("/"), "target must be a caller-owned external directory named target")
    require(target.parent.exists(), "create the caller-owned parent first")
    target = target_path(root, target)
    require(not target.exists() and not any(target.parent.iterdir()), "only an empty owned parent with an absent target can be claimed")
    write_json(target.parent / MARKER, {"version": 2, "target": str(target), "uid": os.getuid(), "layout": paths, "pending": False, "snapshot": None})


def validate_command(command):
    require(command and command[0] == "cargo" and len(command) >= 2 and command[1] in {"test", "build", "clippy"}, "only cargo test/build/clippy are supported")
    takes_value = {"-p", "--package", "--features", "--test", "--bin", "--message-format"}
    switches = {"--workspace", "--all-targets", "--all-features", "--no-default-features", "--release", "--lib", "--locked", "--offline", "--no-run"}
    index = 2
    while index < len(command) and command[index] != "--":
        arg = command[index]
        if arg in takes_value:
            require(index + 1 < len(command) and not command[index + 1].startswith("-"), f"missing value: {arg}")
            require(arg != "--message-format" or command[index + 1] == "json", "only JSON Cargo messages are supported")
            index += 2
        else:
            require(arg in switches, f"unsupported Cargo flag: {arg}")
            index += 1
    if index < len(command):
        require(command[1] in {"test", "clippy"}, "unsupported trailing flags")
        tail = command[index + 1:]
        require(tail == ["-D", "warnings"] if command[1] == "clippy" else all(arg in {"--nocapture", "--ignored", "--include-ignored", "--exact"} or not arg.startswith("-") for arg in tail), "unsupported trailing flags")
    fixed = ([] if "--locked" in command[:index] else ["--locked"]) + ["-j2", "--color", "never"]
    result = command[:index] + fixed + command[index:]
    return result


def original_process(command, root, environment, directory, name, timeout):
    stdout = directory / (name + ".stdout")
    stderr = directory / (name + ".stderr")
    started = time.monotonic()
    with stdout.open("xb") as out, stderr.open("xb") as err:
        child = subprocess.Popen(command, cwd=root, env=environment, stdout=out, stderr=err, start_new_session=True)
        try:
            while child.poll() is None:
                require(time.monotonic() - started < timeout, f"{name} deadline")
                require(stdout.stat().st_size <= MAX_OUTPUT and stderr.stat().st_size <= MAX_OUTPUT, f"{name} output limit")
                time.sleep(0.05)
        except BaseException:
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            child.wait()
            raise
    require(stdout.stat().st_size <= MAX_OUTPUT and stderr.stat().st_size <= MAX_OUTPUT, f"{name} output limit")
    return {"command": command, "exit_code": child.returncode, "stdout": {"path": str(stdout), "sha256": digest(stdout.read_bytes()), "bytes": stdout.stat().st_size}, "stderr": {"path": str(stderr), "sha256": digest(stderr.read_bytes()), "bytes": stderr.stat().st_size}}


def run(root, target, command, receipt, min_free, timeout):
    require(sys.platform == "linux" and len(os.sched_getaffinity(0)) <= 2, "Linux and caller-selected affinity of at most two CPUs are required")
    target = target_path(root, target)
    require(receipt == canonical(receipt) and not receipt.is_relative_to(root) and not receipt.is_relative_to(target.parent), "receipt directory must be external to the workspace and owned parent")
    receipt.mkdir()
    command = validate_command(command)
    environment = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="2", CARGO_INCREMENTAL="0", CARGO_TERM_COLOR="never")
    toolchain = {tool: subprocess.check_output([tool, "-Vv"], cwd=root, env=environment, text=True, timeout=10) for tool in ("cargo", "rustc")}
    require(toolchain["cargo"].startswith("cargo 1.97.1 ") and toolchain["rustc"].startswith("rustc 1.97.1 "), "only the pinned 1.97.1 toolchain layout is supported")
    marker = read_json(target.parent / MARKER)
    validate_marker(marker, target, workspace(root)[0])
    owner_entries(target)
    with exclusive(target.parent / GUARD):
        marker = read_json(target.parent / MARKER)
        current = snapshot(root, command, environment, toolchain)
        validate_marker(marker, target, current["layout"])
        owner_entries(target)
        for index, arg in enumerate(command):
            if arg in {"-p", "--package"}:
                require(command[index + 1] in current["layout"], "only owned workspace packages may be selected")
        idle_target(target)
        free = shutil.disk_usage(target.parent).free
        require(free >= min_free, "insufficient disk headroom")
        bootstrap = pristine_bootstrap(target, marker)
        dirty = [] if bootstrap else invalidated(marker["snapshot"], current, marker["pending"])
        report = {"version": 1, "root": str(root), "target": str(target), "source_before": current, "pristine_bootstrap": bootstrap, "invalidated": dirty, "free_bytes_before": free, "target_bytes_before": target_size(target), "commands": [], "accepted": False}
        marker["pending"] = True
        write_json(target.parent / MARKER, marker)
        try:
            if dirty:
                clean = ["cargo", "clean", "--locked", "--target-dir", str(target)]
                for package in dirty:
                    clean.extend(["-p", "path+" + (root / current["layout"][package]).as_uri() + "#" + package])
                preview = original_process(clean + ["--dry-run", "--verbose"], root, environment, receipt, "clean-preview", timeout)
                report["commands"].append(preview)
                require(preview["exit_code"] == 0, "Cargo clean preview failed")
                idle_target(target)
                cleaned = original_process(clean, root, environment, receipt, "clean", timeout)
                report["commands"].append(cleaned)
                require(cleaned["exit_code"] == 0, "package-scoped cleanup failed")
            idle_target(target)
            require(snapshot(root, command, environment, toolchain) == current, "inputs changed before original Cargo command")
            result = original_process(command, root, environment, receipt, "cargo", timeout)
            report["commands"].append(result)
            report["source_after"] = snapshot(root, command, environment, toolchain)
            require(report["source_after"] == current, "source/configuration drift during Cargo command")
            require(result["exit_code"] == 0, "original Cargo command failed")
            tag = target / "CACHEDIR.TAG"
            require(target.is_dir() and tag.is_file() and not tag.is_symlink(), "Cargo did not create a cache-tagged target")
            report["target_bytes_after"] = target_size(target)
            marker.update(snapshot=current, pending=False)
            write_json(target.parent / MARKER, marker)
            report["accepted"] = True
        except BaseException as error:
            report["refusal"] = str(error)
            raise
        finally:
            report["free_bytes_after"] = shutil.disk_usage(target.parent).free
            try:
                report["target_bytes_after"] = target_size(target)
            except (OSError, Refused) as error:
                report["measurement_error"] = str(error)
            write_json(receipt / "receipt.json", report)
    print(json.dumps({"invalidated": dirty, "command": shlex.join(command), "receipt": str(receipt / "receipt.json")}, sort_keys=True))


def target_size(target):
    total = 0
    for directory, children, names in os.walk(target, followlinks=False):
        require(not any((Path(directory) / name).is_symlink() for name in children + names), "symlinked target entry")
        total += sum((Path(directory) / name).stat().st_size for name in names)
    return total


def validate_marker(marker, target, layout):
    require(isinstance(marker, dict) and set(marker) == {"version", "target", "uid", "layout", "pending", "snapshot"}, "invalid target sentinel fields")
    require(marker["version"] == 2 and marker["uid"] == os.getuid() and marker["target"] == str(target) and marker["layout"] == layout and isinstance(marker["pending"], bool), "invalid target sentinel or package ownership")
    previous = marker["snapshot"]
    if previous is not None:
        require(isinstance(previous, dict) and set(previous) == {"layout", "dependencies", "packages", "global", "source"}, "invalid prior fingerprint")
        require(previous["layout"] == layout and isinstance(previous["packages"], dict) and set(previous["packages"]) == set(layout) and isinstance(previous["dependencies"], dict) and set(previous["dependencies"]) == set(layout), "invalid prior package ownership")
        hashes = [previous["global"], previous["source"], *previous["packages"].values()]
        require(all(isinstance(value, str) and len(value) == 64 and all(ch in "0123456789abcdef" for ch in value) for value in hashes), "invalid prior content hashes")
        require(all(isinstance(values, list) and all(value in layout for value in values) for values in previous["dependencies"].values()), "unowned prior dependency")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "run"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--min-free-gib", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=900)
    arguments = sys.argv[1:]
    separator = arguments.index("--") if "--" in arguments else len(arguments)
    args = parser.parse_args(arguments[:separator])
    command = arguments[separator + 1:]
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("termination requested")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        root = canonical(args.root)
        require(args.min_free_gib >= 1 and 1 <= args.timeout <= 3600, "invalid resource limits")
        if args.action == "init":
            require(not command and args.receipt is None, "init takes no command or receipt")
            initialize(root, args.target)
        else:
            require(args.receipt is not None, "run requires a new receipt directory")
            run(root, args.target, command, args.receipt, args.min_free_gib * 1024**3, args.timeout)
    except (Refused, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"refused: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
