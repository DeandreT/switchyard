import copy
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("cargo_handoff", Path(__file__).parents[1] / "cargo_handoff.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)
FIXTURE_SPEC = importlib.util.spec_from_file_location("cargo_handoff_fixture", Path(__file__).with_name("cargo_handoff_fixture.py"))
fixture = importlib.util.module_from_spec(FIXTURE_SPEC)
FIXTURE_SPEC.loader.exec_module(fixture)


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "worktree"
        self.root.mkdir()
        (self.root / "Cargo.toml").write_text('[workspace]\nresolver="3"\nmembers=["api","consumer","independent"]\n')
        for name in ("api", "consumer", "independent"):
            path = self.root / name
            (path / "src").mkdir(parents=True)
            dependency = '\n[dependencies]\napi={path="../api"}\n' if name == "consumer" else ""
            (path / "Cargo.toml").write_text(f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2024"\n{dependency}')
            (path / "src/lib.rs").write_text("pub fn value()->u32 { 1 }\n")
        (self.root / "Cargo.lock").write_text("version = 4\n" + "".join(f'\n[[package]]\nname="{name}"\nversion="0.1.0"\n' for name in ("api", "consumer", "independent")))
        self.target = self.base / "owned" / "target"
        self.target.parent.mkdir()

    def snapshot(self, command=None):
        with patch.object(guard, "configuration", return_value={}):
            return guard.snapshot(self.root, command or ["cargo", "test", "--workspace"], {}, {"cargo": "pinned", "rustc": "pinned"})

    def test_content_change_invalidates_owned_reverse_dependents_without_timestamp_change(self):
        before = self.snapshot()
        path = self.root / "api/src/lib.rs"
        timestamp = path.stat().st_mtime_ns
        path.write_text("pub fn value()->u32 { 2 }\n")
        os.utime(path, ns=(timestamp, timestamp))
        after = self.snapshot()
        self.assertNotEqual(before["source"], after["source"])
        self.assertEqual(guard.invalidated(before, after), ["api", "consumer"])
        self.assertEqual(guard.invalidated(after, after), [])

    def test_profile_feature_toolchain_and_root_inputs_invalidate_all_owned_packages(self):
        before = self.snapshot()
        for command in (["cargo", "test", "--release"], ["cargo", "test", "--all-features"], ["cargo", "test", "--features", "api/new"], ["cargo", "clippy", "--workspace"], ["cargo", "test", "-p", "api"], ["cargo", "test", "--workspace", "--all-targets"], ["cargo", "test", "--workspace", "--lib"]):
            self.assertEqual(guard.invalidated(before, self.snapshot(command)), ["api", "consumer", "independent"])
        changed = copy.deepcopy(before)
        changed["global"] = "changed toolchain/configuration"
        self.assertEqual(guard.invalidated(before, changed), ["api", "consumer", "independent"])
        lock = self.root / "Cargo.lock"
        lock.write_text(lock.read_text() + "\n# changed lock input\n")
        self.assertEqual(guard.invalidated(before, self.snapshot()), ["api", "consumer", "independent"])

    def test_identical_content_in_another_worktree_invalidates_all_owned_packages(self):
        before = self.snapshot()
        other = self.base / "other-worktree"
        shutil.copytree(self.root, other)
        with patch.object(guard, "configuration", return_value={}):
            after = guard.snapshot(other, ["cargo", "test", "--workspace"], {}, {"cargo": "pinned", "rustc": "pinned"})
        self.assertEqual(before["source"], after["source"])
        self.assertEqual(before["packages"], after["packages"])
        self.assertNotEqual(before["global"], after["global"])
        self.assertEqual(guard.invalidated(before, after), ["api", "consumer", "independent"])

    def test_exact_workspace_version_inheritance_is_supported_but_ambiguity_refuses(self):
        manifest = self.root / "Cargo.toml"
        manifest.write_text(manifest.read_text() + '\n[workspace.package]\nversion="0.1.0"\n')
        package = self.root / "api/Cargo.toml"
        original = package.read_text()
        package.write_text(original.replace('version="0.1.0"', 'version.workspace=true'))
        self.assertEqual(guard.workspace(self.root)[0]["api"], "api")
        for version in ('{workspace=false}', '{workspace=true, extra="unknown"}', 'false', '"0.2.0"'):
            package.write_text(original.replace('version="0.1.0"', "version=" + version))
            with self.assertRaises(guard.Refused):
                guard.workspace(self.root)
        package.write_text(original.replace('version="0.1.0"', 'version.workspace=true'))
        manifest.write_text(manifest.read_text().replace('version="0.1.0"', 'version={workspace=true}'))
        with self.assertRaisesRegex(guard.Refused, "package version required"):
            guard.workspace(self.root)

    def test_owned_lock_names_refuse_registry_git_other_version_and_duplicate_collisions(self):
        lock = self.root / "Cargo.lock"
        original = lock.read_text()
        api = '\n[[package]]\nname="api"\nversion="0.1.0"\n'
        extras = (api + 'source="registry+https://example.invalid/index"\n', api + 'source="git+https://example.invalid/repo#012345"\n', api.replace('"0.1.0"', '"0.2.0"'), api)
        for extra in extras:
            lock.write_text(original + extra)
            with self.assertRaisesRegex(guard.Refused, "ambiguous locked"):
                guard.workspace(self.root)
        for source in ('source="registry+https://example.invalid/index"\n', 'source="git+https://example.invalid/repo#012345"\n', 'checksum="uncertain"\n'):
            lock.write_text(original.replace(api, api + source))
            with self.assertRaisesRegex(guard.Refused, "version/source mismatch"):
                guard.workspace(self.root)
        lock.write_text(original.replace(api, ""))
        with self.assertRaisesRegex(guard.Refused, "ambiguous locked"):
            guard.workspace(self.root)

    def test_locked_name_collision_refuses_before_any_cleanup_or_original_command(self):
        with patch.object(guard, "target_path", return_value=self.target):
            guard.initialize(self.root, self.target)
        lock = self.root / "Cargo.lock"
        lock.write_text(lock.read_text() + '\n[[package]]\nname="api"\nversion="9.0.0"\n')
        with patch.object(guard, "target_path", return_value=self.target), patch.object(guard.os, "sched_getaffinity", return_value={14, 15}), patch.object(guard.subprocess, "check_output", side_effect=["cargo 1.97.1 (pinned)\n", "rustc 1.97.1 (pinned)\n"]), patch.object(guard, "original_process") as original:
            with self.assertRaisesRegex(guard.Refused, "ambiguous locked"):
                guard.run(self.root, self.target, ["cargo", "test", "--workspace"], self.base / "collision-receipt", 0, 10)
        original.assert_not_called()
        self.assertFalse(guard.read_json(self.target.parent / guard.MARKER)["pending"])
        self.assertFalse(self.target.exists())

    def test_unmapped_shared_input_change_invalidates_all_even_with_preserved_timestamp(self):
        path = self.root / "shared.bin"
        path.write_bytes(b"first body")
        timestamp = path.stat().st_mtime_ns
        before = self.snapshot()
        path.write_bytes(b"other body")
        os.utime(path, ns=(timestamp, timestamp))
        after = self.snapshot()
        self.assertEqual(before["packages"], after["packages"])
        self.assertNotEqual(before["global"], after["global"])
        self.assertEqual(guard.invalidated(before, after), ["api", "consumer", "independent"])

    def test_package_manifest_change_and_pending_receipt_force_safe_invalidation(self):
        before = self.snapshot()
        path = self.root / "api/Cargo.toml"
        path.write_text(path.read_text() + '\n[features]\nnew=[]\n')
        after = self.snapshot()
        self.assertEqual(guard.invalidated(before, after), ["api", "consumer"])
        self.assertEqual(guard.invalidated(after, after, pending=True), ["api", "consumer", "independent"])

    def test_unknown_target_is_never_claimed_and_existing_evidence_is_not_removed(self):
        self.target.mkdir()
        (self.target / "retained.json").write_text("{}\n")
        with patch.object(guard, "target_path", return_value=self.target):
            with self.assertRaisesRegex(guard.Refused, "empty"):
                guard.initialize(self.root, self.target)
        self.assertTrue((self.target / "retained.json").exists())
        self.assertFalse((self.target.parent / guard.MARKER).exists())

    def test_absent_target_gets_external_path_uid_and_exact_package_ownership(self):
        with patch.object(guard, "target_path", return_value=self.target):
            guard.initialize(self.root, self.target)
        marker = guard.read_json(self.target.parent / guard.MARKER)
        self.assertEqual(marker["uid"], os.getuid())
        self.assertEqual(marker["target"], str(self.target))
        self.assertEqual(marker["layout"], {name: name for name in ("api", "consumer", "independent")})
        self.assertIsNone(marker["snapshot"])
        self.assertFalse(self.target.exists())
        self.assertEqual(marker["version"], 2)

    def test_existing_empty_target_and_foreign_parent_are_never_adopted(self):
        self.target.mkdir()
        with patch.object(guard, "target_path", return_value=self.target):
            with self.assertRaisesRegex(guard.Refused, "absent target"):
                guard.initialize(self.root, self.target)
        self.target.rmdir()
        foreign = self.target.parent / "retained-evidence"
        foreign.write_text("do not adopt\n")
        with patch.object(guard, "target_path", return_value=self.target):
            with self.assertRaisesRegex(guard.Refused, "empty owned parent"):
                guard.initialize(self.root, self.target)
        self.assertEqual(foreign.read_text(), "do not adopt\n")
        self.assertFalse((self.target.parent / guard.MARKER).exists())

    def test_pristine_bootstrap_never_invokes_cleanup_before_first_cargo_command(self):
        with patch.object(guard, "target_path", return_value=self.target):
            guard.initialize(self.root, self.target)
        receipt = self.base / "bootstrap-receipt"
        commands = []
        def original(command, root, environment, directory, name, timeout):
            commands.append(command)
            self.assertFalse(self.target.exists())
            self.assertEqual({path.name for path in self.target.parent.iterdir()}, {guard.MARKER, guard.GUARD})
            self.target.mkdir()
            (self.target / "CACHEDIR.TAG").write_text("test double of Cargo's own cache tag\n")
            return {"command": command, "exit_code": 0}

        with patch.object(guard, "target_path", return_value=self.target), patch.object(guard.os, "sched_getaffinity", return_value={14, 15}), patch.object(guard.subprocess, "check_output", side_effect=["cargo 1.97.1 (pinned)\n", "rustc 1.97.1 (pinned)\n"]), patch.object(guard, "configuration", return_value={}), patch.object(guard, "original_process", side_effect=original):
            guard.run(self.root, self.target, ["cargo", "test", "--workspace"], receipt, 0, 10)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][:2], ["cargo", "test"])
        report = guard.read_json(receipt / "receipt.json")
        self.assertTrue(report["pristine_bootstrap"])
        self.assertEqual(report["invalidated"], [])
        self.assertTrue(report["accepted"])
        self.assertEqual((self.target / "CACHEDIR.TAG").read_text(), "test double of Cargo's own cache tag\n")
        self.assertFalse((self.target / guard.MARKER).exists())
        self.assertTrue((self.target.parent / guard.GUARD).exists())

    def test_foreign_initial_entry_and_untagged_interrupted_bootstrap_refuse(self):
        with patch.object(guard, "target_path", return_value=self.target):
            guard.initialize(self.root, self.target)
        marker = guard.read_json(self.target.parent / guard.MARKER)
        foreign = self.target.parent / "foreign"
        foreign.write_text("retained foreign input\n")
        with self.assertRaisesRegex(guard.Refused, "foreign entry"):
            guard.owner_entries(self.target)
        foreign.unlink()
        self.target.mkdir()
        with self.assertRaisesRegex(guard.Refused, "foreign initial target"):
            guard.pristine_bootstrap(self.target, marker)
        marker["pending"] = True
        with self.assertRaisesRegex(guard.Refused, "Cargo-tagged cleanup"):
            guard.pristine_bootstrap(self.target, marker)
        (self.target / "CACHEDIR.TAG").write_text("invalid tag retained for Cargo's own validation\n")
        self.assertFalse(guard.pristine_bootstrap(self.target, marker))
        self.assertEqual(guard.invalidated(None, self.snapshot(), pending=True), ["api", "consumer", "independent"])
        marker["snapshot"] = self.snapshot()
        self.target.rename(self.target.with_name("retained-interrupted-target"))
        with self.assertRaisesRegex(guard.Refused, "populated target is missing"):
            guard.pristine_bootstrap(self.target, marker)

    def test_initial_success_without_a_cargo_created_target_tag_is_not_accepted(self):
        with patch.object(guard, "target_path", return_value=self.target):
            guard.initialize(self.root, self.target)
        receipt = self.base / "untagged-receipt"
        def original(command, root, environment, directory, name, timeout):
            self.target.mkdir()
            return {"command": command, "exit_code": 0}

        with patch.object(guard, "target_path", return_value=self.target), patch.object(guard.os, "sched_getaffinity", return_value={14, 15}), patch.object(guard.subprocess, "check_output", side_effect=["cargo 1.97.1 (pinned)\n", "rustc 1.97.1 (pinned)\n"]), patch.object(guard, "configuration", return_value={}), patch.object(guard, "original_process", side_effect=original):
            with self.assertRaisesRegex(guard.Refused, "Cargo did not create"):
                guard.run(self.root, self.target, ["cargo", "test", "--workspace"], receipt, 0, 10)
        self.assertFalse(guard.read_json(receipt / "receipt.json")["accepted"])
        self.assertTrue(guard.read_json(self.target.parent / guard.MARKER)["pending"])
        self.assertFalse((self.target / "CACHEDIR.TAG").exists())

    def test_guard_and_each_observed_cargo_lock_refuse_an_active_owner(self):
        with guard.exclusive(self.target.parent / guard.GUARD):
            with self.assertRaisesRegex(guard.Refused, "active lock"):
                with guard.exclusive(self.target.parent / guard.GUARD):
                    self.fail("second guard admitted")
        for name in guard.LOCKS:
            path = self.target / "debug" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w") as owner:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(guard.Refused, "active lock"):
                    guard.idle_target(self.target)
            guard.idle_target(self.target)

    def test_symlinked_inputs_locks_and_external_dependencies_are_refused(self):
        source = self.root / "api/src/extra.rs"
        source.symlink_to(self.root / "consumer/src/lib.rs")
        with self.assertRaisesRegex(guard.Refused, "regular file"):
            self.snapshot()
        source.unlink()
        lock = self.target.parent / guard.GUARD
        lock.symlink_to(self.root / "Cargo.toml")
        with self.assertRaisesRegex(guard.Refused, "symlinked lock"):
            with guard.exclusive(lock):
                self.fail("symlink admitted")
        manifest = self.root / "api/Cargo.toml"
        manifest.write_text(manifest.read_text() + '\n[dependencies]\noutside={path="../../outside"}\n')
        with self.assertRaisesRegex(guard.Refused, "unowned path dependency"):
            guard.workspace(self.root)

    def test_commands_are_bounded_and_custom_layouts_are_refused(self):
        command = guard.validate_command(["cargo", "clippy", "--workspace", "--locked", "--", "-D", "warnings"])
        self.assertEqual(command.count("--locked"), 1)
        self.assertIn("-j2", command)
        self.assertEqual(command[-3:], ["--", "-D", "warnings"])
        for command in (["rm", "-rf", "/"], ["cargo", "test", "--target", "custom"], ["cargo", "build", "-j16"], ["cargo", "test", "--config", "custom"], ["cargo", "test", "--features"]):
            with self.assertRaises(guard.Refused):
                guard.validate_command(command)
        with self.assertRaisesRegex(guard.Refused, "wrappers"):
            guard.configuration(self.root, {"RUSTC_WRAPPER": "wrapper"})

    def test_configuration_is_content_bound_and_unknown_layout_config_is_refused(self):
        directory = self.root / ".cargo"
        directory.mkdir()
        config = directory / "config.toml"
        config.write_text('[build]\nrustflags=["-C", "debuginfo=1"]\n')
        before = guard.configuration(self.root, {"CARGO_HOME": str(self.base / "cargo-home")})
        config.write_text('[build]\nrustflags=["-C", "debuginfo=2"]\n')
        after = guard.configuration(self.root, {"CARGO_HOME": str(self.base / "cargo-home")})
        self.assertNotEqual(before, after)
        config.write_text('[build]\nbuild-dir="outside"\n')
        with self.assertRaisesRegex(guard.Refused, "layout"):
            guard.configuration(self.root, {"CARGO_HOME": str(self.base / "cargo-home")})

    def test_duplicate_json_keys_and_source_drift_are_visible(self):
        path = self.base / "duplicate.json"
        path.write_text('{"version":1,"version":2}\n')
        with self.assertRaisesRegex(guard.Refused, "duplicate"):
            guard.read_json(path)
        before = self.snapshot()
        (self.root / "consumer/src/new.rs").write_text("// newly admitted input\n")
        self.assertNotEqual(before, self.snapshot())

    def test_run_refuses_source_drift_and_retains_original_result_outputs_and_pending(self):
        with patch.object(guard, "target_path", return_value=self.target):
            guard.initialize(self.root, self.target)
        receipt = self.base / "drift-receipt"
        results = []
        def original(command, root, environment, directory, name, timeout):
            self.assertFalse(self.target.exists())
            (root / "api/src/lib.rs").write_text("pub fn value()->u32 { 2 }\n")
            result = {"command": command, "exit_code": 0}
            for output, body in (("stdout", b"retained original stdout\n"), ("stderr", b"retained original stderr\n")):
                path = directory / (name + "." + output)
                path.write_bytes(body)
                result[output] = {"path": str(path), "sha256": guard.digest(body), "bytes": len(body)}
            results.append(result)
            return result

        with patch.object(guard, "target_path", return_value=self.target), patch.object(guard.os, "sched_getaffinity", return_value={14, 15}), patch.object(guard.subprocess, "check_output", side_effect=["cargo 1.97.1 (pinned)\n", "rustc 1.97.1 (pinned)\n"]), patch.object(guard, "configuration", return_value={}), patch.object(guard, "original_process", side_effect=original):
            with self.assertRaisesRegex(guard.Refused, "source/configuration drift"):
                guard.run(self.root, self.target, ["cargo", "test", "--workspace"], receipt, 0, 10)
        report = guard.read_json(receipt / "receipt.json")
        self.assertFalse(report["accepted"])
        self.assertEqual(len(results), 1)
        self.assertEqual(report["commands"], results)
        self.assertNotEqual(report["source_before"], report["source_after"])
        marker = guard.read_json(self.target.parent / guard.MARKER)
        self.assertTrue(marker["pending"])
        self.assertIsNone(marker["snapshot"])
        for output, expected in (("stdout", b"retained original stdout\n"), ("stderr", b"retained original stderr\n")):
            recorded = report["commands"][0][output]
            self.assertEqual(Path(recorded["path"]).read_bytes(), expected)
            self.assertEqual(recorded["sha256"], guard.digest(expected))

    def test_invalid_sentinel_state_is_refused_before_cleanup(self):
        layout = {"api": "api"}
        marker = {"version": 2, "target": str(self.target), "uid": os.getuid(), "layout": layout, "pending": False, "snapshot": None}
        guard.validate_marker(marker, self.target, layout)
        for field, value in (("version", 1), ("uid", os.getuid() + 1), ("target", str(self.base)), ("pending", "false"), ("snapshot", {}), ("layout", {"unowned": "unowned"})):
            invalid = dict(marker, **{field: value})
            with self.assertRaises(guard.Refused):
                guard.validate_marker(invalid, self.target, layout)

    def test_fixture_requires_exact_source_owned_ids_statuses_and_executable(self):
        binary = self.target / "debug/deps/handoff_fixture-actual"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b"actual fixture executable")
        message = json.dumps({"reason": "compiler-artifact", "executable": str(binary), "profile": {"test": True}, "target": {"src_path": str(self.root / "api/src/lib.rs")}})
        body = message + "\n\nrunning 1 test\ntest new_only ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
        result = fixture.corpus(body, self.root, self.target, {"new_only": "ok"})
        self.assertEqual(result["executable"], str(binary))
        self.assertEqual(result["tests"], {"new_only": "ok"})
        for invalid in (body.replace("new_only", "old_only"), body.replace(" ... ok", " ... FAILED"), body.replace("test new_only ... ok", "test new_only ... ok\ntest new_only ... ok"), body.replace("running 1 test", "running 2 tests"), body.replace("1 passed", "0 passed"), body.replace(str(binary), str(self.base / "outside")), body.replace(str(self.root / "api/src/lib.rs"), str(self.root / "consumer/src/lib.rs")), message + "\n" + body):
            with self.assertRaises(fixture.guard.Refused):
                fixture.corpus(invalid, self.root, self.target, {"new_only": "ok"})

    def test_original_command_exit_and_both_output_hashes_are_preserved(self):
        directory = self.base / "original-command"
        directory.mkdir()
        command = [sys.executable, "-c", "import sys; print('original stdout'); print('original stderr', file=sys.stderr); sys.exit(7)"]
        result = guard.original_process(command, self.root, os.environ, directory, "original", 10)
        self.assertEqual(result["command"], command)
        self.assertEqual(result["exit_code"], 7)
        for name, expected in (("stdout", b"original stdout\n"), ("stderr", b"original stderr\n")):
            self.assertEqual(Path(result[name]["path"]).read_bytes(), expected)
            self.assertEqual(result[name]["sha256"], guard.digest(expected))

    def test_timed_out_original_is_reaped_and_its_output_is_retained(self):
        directory = self.base / "timed-out-command"
        directory.mkdir()
        command = [sys.executable, "-c", "import os,time; print(os.getpid(), flush=True); time.sleep(60)"]
        created = []
        original_spawn = guard.subprocess.Popen
        def retain_original(*args, **kwargs):
            child = original_spawn(*args, **kwargs)
            created.append(child)
            return child

        with patch.object(guard.subprocess, "Popen", side_effect=retain_original):
            with self.assertRaisesRegex(guard.Refused, "deadline"):
                guard.original_process(command, self.root, os.environ, directory, "original", 1)
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].returncode, -signal.SIGKILL)
        self.assertEqual(int((directory / "original.stdout").read_text().strip()), created[0].pid)


if __name__ == "__main__":
    unittest.main()
