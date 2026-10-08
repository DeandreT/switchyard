//! Synthetic parser/byte controls. These are not restored SDK package evidence.

#[allow(dead_code)]
#[path = "sdk_child/mod.rs"]
mod sdk_child;

use std::{fs, io, path::PathBuf};

use sdk_child::{
    SdkRun, identity,
    pins::{PinInputs, SdkPin},
};
use serde_json::{Map, Value, json};

const SERVICE_BUS: &str = "Azure.Messaging.ServiceBus";
const CORE: &str = "Azure.Core";
const CORE_VERSION: &str = "1.0.0";

fn conformance() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

fn props(pin: SdkPin, range: &str) -> Vec<u8> {
    format!(
        "<Project><PropertyGroup>\
         <SwitchyardSdkSelector>{}</SwitchyardSdkSelector>\
         <SwitchyardServiceBusVersion>{range}</SwitchyardServiceBusVersion>\
         <RestorePackagesWithLockFile>true</RestorePackagesWithLockFile>\
         <RestoreLockedMode>true</RestoreLockedMode>\
         </PropertyGroup></Project>",
        pin.selector(),
    )
    .into_bytes()
}

struct Fixture {
    root: tempfile::TempDir,
    pin: SdkPin,
    project: PathBuf,
    packages: PathBuf,
    output: PathBuf,
    project_xml: Vec<u8>,
    props_xml: Vec<u8>,
    lock: Value,
    approved: Value,
    assets: Value,
}

impl Fixture {
    fn new(pin: SdkPin) -> io::Result<Self> {
        let root = tempfile::tempdir()?;
        let project = root.path().join("Project.csproj");
        let packages = root.path().join("packages");
        let output = root.path().join("bin");
        fs::create_dir(&packages)?;
        fs::create_dir(&output)?;
        let project_xml = fs::read(
            conformance().join("dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj"),
        )?;
        let props_xml = fs::read(
            conformance()
                .join("pins")
                .join(pin.selector())
                .join("SdkPin.props"),
        )?;
        fs::write(&project, &project_xml)?;
        let mut approved_packages = Vec::new();
        let mut locked = Map::new();
        let mut target = Map::new();
        let mut libraries = Map::new();
        for (role, id, version, bytes) in [
            (
                "service_bus",
                SERVICE_BUS,
                pin.version(),
                format!("synthetic {} servicebus", pin.selector()),
            ),
            (
                "core",
                CORE,
                CORE_VERSION,
                String::from("synthetic approved core"),
            ),
        ] {
            let package_path = format!("{}/{version}", id.to_ascii_lowercase());
            let runtime_asset = format!("lib/net10.0/{id}.dll");
            let cache_file = packages.join(&package_path).join(&runtime_asset);
            fs::create_dir_all(cache_file.parent().unwrap())?;
            fs::write(&cache_file, bytes.as_bytes())?;
            fs::write(output.join(format!("{id}.dll")), bytes.as_bytes())?;
            let hash = identity::hash_file(&cache_file)?;
            approved_packages.push(json!({
                "role": role, "id": id, "version": version,
                "runtime_asset": runtime_asset, "sha256": hash,
            }));
            let content_hash = format!("synthetic-content-hash-{role}");
            let mut dependency = json!({
                "type": "Transitive", "resolved": version, "contentHash": content_hash,
            });
            if id == SERVICE_BUS {
                dependency["type"] = json!("Direct");
                dependency["requested"] = json!(format!("[{version}, {version}]"));
            }
            locked.insert(id.to_owned(), dependency);
            let mut runtime = Map::new();
            runtime.insert(runtime_asset.clone(), json!({}));
            let mut compile = Map::new();
            compile.insert(format!("ref/net10.0/{id}.dll"), json!({}));
            target.insert(
                format!("{id}/{version}"),
                json!({
                    "type": "package", "runtime": runtime,
                    "compile": compile,
                }),
            );
            libraries.insert(
                format!("{id}/{version}"),
                json!({
                    "type": "package", "path": package_path,
                    "sha512": content_hash, "files": [runtime_asset],
                }),
            );
        }
        let lock = json!({"version": 1, "dependencies": {"net10.0": locked}});
        let approved = json!({
            "schema": 1, "selector": pin.selector(), "framework": "net10.0",
            "packages": approved_packages,
        });
        let mut folders = Map::new();
        folders.insert(packages.to_string_lossy().into_owned(), json!({}));
        let assets = json!({
            "version": 3,
            "targets": {"net10.0": target}, "libraries": libraries,
            "packageFolders": folders,
            "project": {
                "restore": {"projectPath": project, "packagesPath": packages, "fallbackFolders": []},
                "frameworks": {"net10.0": {"dependencies": {
                    "Azure.Messaging.ServiceBus": {"target": "Package", "version": format!("[{}, {}]", pin.version(), pin.version())},
                }}},
            },
        });
        Ok(Self {
            root,
            pin,
            project,
            packages,
            output,
            project_xml,
            props_xml,
            lock,
            approved,
            assets,
        })
    }

    fn inputs(&self) -> io::Result<PinInputs> {
        PinInputs::parse(
            self.pin,
            &self.project_xml,
            &self.props_xml,
            &serde_json::to_vec(&self.lock)?,
            &serde_json::to_vec(&self.approved)?,
        )
    }

    fn verify(&self) -> io::Result<Vec<sdk_child::pins::ResolvedAsset>> {
        self.inputs()?.verify_assets_value(
            &self.assets,
            &self.project,
            &self.packages,
            &self.output,
        )
    }

    fn cache_file(&self, id: &str) -> PathBuf {
        let version = if id == SERVICE_BUS {
            self.pin.version()
        } else {
            CORE_VERSION
        };
        self.packages.join(format!(
            "{}/{version}/lib/net10.0/{id}.dll",
            id.to_ascii_lowercase()
        ))
    }
}

fn rejected<T>(result: io::Result<T>) {
    match result {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}"),
        Ok(_) => panic!("negative SDK pin control was accepted"),
    }
}

#[test]
fn both_explicit_selectors_and_shared_transport_templates_accept_their_own_inputs() -> io::Result<()>
{
    for pin in [SdkPin::DeclaredCurrent, SdkPin::Previous] {
        let mut fixture = Fixture::new(pin)?;
        let resolved = fixture.verify()?;
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].package_version, pin.version());
        assert_eq!(resolved[1].package_version, CORE_VERSION);
        fixture.project_xml = fs::read(
            conformance().join("dotnet-websockets/Switchyard.Conformance.DotNetWebSockets.csproj"),
        )?;
        assert_eq!(fixture.verify()?, resolved);
    }
    Ok(())
}

#[test]
fn minimum_ranges_and_wrong_selector_inputs_are_rejected_without_fallback() -> io::Result<()> {
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.props_xml = props(fixture.pin, fixture.pin.version());
    rejected(fixture.inputs());
    fixture.props_xml = props(SdkPin::Previous, "[7.20.2]");
    rejected(fixture.inputs());
    fixture.props_xml = props(fixture.pin, "[7.21.0]");
    fixture.approved["packages"][0]["version"] = json!("7.20.2");
    rejected(fixture.inputs());
    Ok(())
}

#[test]
fn lock_and_assets_wrong_pins_are_independent_of_valid_loaded_byte_hashes() -> io::Result<()> {
    for id in [SERVICE_BUS, CORE] {
        let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
        fixture.lock["dependencies"]["net10.0"][id]["resolved"] = json!("0.0.1");
        rejected(fixture.inputs());
        let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
        let version = if id == SERVICE_BUS {
            fixture.pin.version()
        } else {
            CORE_VERSION
        };
        let correct_key = format!("{id}/{version}");
        let libraries = fixture.assets["libraries"].as_object_mut().unwrap();
        let original = libraries.remove(&correct_key).unwrap();
        libraries.insert(format!("{id}/0.0.1"), original);
        rejected(fixture.verify());
    }
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.lock["dependencies"]["net10.0"][SERVICE_BUS]["requested"] = json!("7.21.0");
    rejected(fixture.inputs());
    fixture.lock["dependencies"]["net10.0"][SERVICE_BUS]["requested"] = json!("[7.21.0, 7.21.0]");
    fixture.assets["project"]["frameworks"]["net10.0"]["dependencies"][SERVICE_BUS]["version"] =
        json!("7.21.0");
    rejected(fixture.verify());
    Ok(())
}

#[test]
fn each_mixed_output_dll_is_rejected_against_unchanged_approved_package_bytes() -> io::Result<()> {
    for id in [SERVICE_BUS, CORE] {
        let fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
        let inputs = fixture.inputs()?;
        assert!(fixture.verify().is_ok());
        fs::write(
            fixture.output.join(format!("{id}.dll")),
            b"other pin output",
        )?;
        rejected(inputs.verify_assets_value(
            &fixture.assets,
            &fixture.project,
            &fixture.packages,
            &fixture.output,
        ));
    }
    Ok(())
}

#[test]
fn poisoned_cached_asset_and_matching_output_cannot_redefine_approved_hash() -> io::Result<()> {
    for id in [SERVICE_BUS, CORE] {
        let fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
        let inputs = fixture.inputs()?;
        assert!(fixture.verify().is_ok());
        fs::write(fixture.cache_file(id), b"cached other pin bytes")?;
        fs::write(
            fixture.output.join(format!("{id}.dll")),
            b"cached other pin bytes",
        )?;
        rejected(inputs.verify_assets_value(
            &fixture.assets,
            &fixture.project,
            &fixture.packages,
            &fixture.output,
        ));
    }
    Ok(())
}

#[test]
fn compile_assets_and_ambiguous_runtime_candidates_do_not_substitute_for_selected_runtime()
-> io::Result<()> {
    let key = format!("{SERVICE_BUS}/7.21.0");
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["targets"]["net10.0"][&key]
        .as_object_mut()
        .unwrap()
        .remove("runtime");
    rejected(fixture.verify());
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["targets"]["net10.0"][&key]["runtime"]
        [format!("lib/net8.0/{SERVICE_BUS}.dll")] = json!({});
    rejected(fixture.verify());
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["targets"]["net10.0/linux-x64"] = json!({});
    rejected(fixture.verify());
    Ok(())
}

#[test]
fn ambiguous_package_names_and_content_hash_mismatch_are_rejected() -> io::Result<()> {
    let key = format!("{SERVICE_BUS}/7.21.0");
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    let original = fixture.assets["libraries"][&key].clone();
    fixture.assets["libraries"][key.to_ascii_lowercase()] = original;
    rejected(fixture.verify());
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["libraries"][&key]["sha512"] = json!("another package content hash");
    rejected(fixture.verify());
    Ok(())
}

#[test]
fn cache_fallbacks_and_package_path_traversal_are_rejected() -> io::Result<()> {
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["packageFolders"][fixture.root.path().to_string_lossy().as_ref()] = json!({});
    rejected(fixture.verify());
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["project"]["restore"]["fallbackFolders"] = json!([fixture.root.path()]);
    rejected(fixture.verify());
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.assets["libraries"][format!("{SERVICE_BUS}/7.21.0")]["path"] = json!("../../other-pin");
    rejected(fixture.verify());
    let mut fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    fixture.approved["packages"][0]["runtime_asset"] = json!("../Azure.Messaging.ServiceBus.dll");
    rejected(fixture.inputs());
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlinked_package_or_output_cannot_escape_owned_directories() -> io::Result<()> {
    for cached in [true, false] {
        let fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
        let outside = fixture.root.path().join("outside.dll");
        let destination = if cached {
            fixture.cache_file(SERVICE_BUS)
        } else {
            fixture.output.join(format!("{SERVICE_BUS}.dll"))
        };
        fs::copy(&destination, &outside)?;
        fs::remove_file(&destination)?;
        std::os::unix::fs::symlink(&outside, &destination)?;
        rejected(fixture.verify());
    }
    Ok(())
}

#[test]
fn malformed_duplicate_or_oversized_inputs_never_become_package_evidence() -> io::Result<()> {
    let fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    let lock = serde_json::to_vec(&fixture.lock)?;
    let approved = serde_json::to_vec(&fixture.approved)?;
    for bad_approval in [b"{".as_slice(), b"{\"schema\":1,\"schema\":1}".as_slice()] {
        rejected(PinInputs::parse(
            fixture.pin,
            &fixture.project_xml,
            &fixture.props_xml,
            &lock,
            bad_approval,
        ));
    }
    let oversized = vec![b' '; sdk_child::pins::INPUT_BYTES + 1];
    rejected(PinInputs::parse(
        fixture.pin,
        &fixture.project_xml,
        &fixture.props_xml,
        &oversized,
        &approved,
    ));
    let mut wrong_schema = fixture.approved.clone();
    wrong_schema["schema"] = json!(2);
    rejected(PinInputs::parse(
        fixture.pin,
        &fixture.project_xml,
        &fixture.props_xml,
        &lock,
        &serde_json::to_vec(&wrong_schema)?,
    ));
    let mut wrong_schema = fixture.assets.clone();
    wrong_schema["version"] = json!(4);
    rejected(fixture.inputs()?.verify_assets_value(
        &wrong_schema,
        &fixture.project,
        &fixture.packages,
        &fixture.output,
    ));
    Ok(())
}

#[test]
fn restored_lock_must_remain_the_original_selected_input() -> io::Result<()> {
    let fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    let inputs = fixture.inputs()?;
    let copy = fixture.root.path().join("packages.lock.json");
    fs::write(&copy, serde_json::to_vec(&fixture.lock)?)?;
    inputs.verify_lock(&copy)?;
    let mut replaced = fixture.lock.clone();
    replaced["dependencies"]["net10.0"][SERVICE_BUS]["resolved"] = json!("7.20.2");
    fs::write(&copy, serde_json::to_vec(&replaced)?)?;
    rejected(inputs.verify_lock(&copy));
    Ok(())
}

#[test]
fn duplicate_pin_properties_are_rejected() -> io::Result<()> {
    let fixture = Fixture::new(SdkPin::DeclaredCurrent)?;
    let duplicate = props(fixture.pin, "[7.21.0]");
    let mut xml = std::str::from_utf8(&duplicate).unwrap().to_owned();
    xml = xml.replace(
        "</PropertyGroup>",
        "<SwitchyardServiceBusVersion>[7.21.0]</SwitchyardServiceBusVersion></PropertyGroup>",
    );
    rejected(PinInputs::parse(
        fixture.pin,
        &fixture.project_xml,
        xml.as_bytes(),
        &serde_json::to_vec(&fixture.lock)?,
        &serde_json::to_vec(&fixture.approved)?,
    ));
    Ok(())
}

struct FileRestore {
    originals: Vec<(PathBuf, Vec<u8>)>,
    restored: bool,
}

impl FileRestore {
    fn capture(paths: &[PathBuf]) -> io::Result<Self> {
        let mut originals = Vec::with_capacity(paths.len());
        for path in paths {
            originals.push((
                path.clone(),
                sdk_child::pins::read_bounded(path, identity::ARTIFACT_BYTES as usize)?,
            ));
        }
        Ok(Self {
            originals,
            restored: false,
        })
    }

    fn restore(mut self) -> io::Result<()> {
        let mut first_error = None;
        for (path, bytes) in &self.originals {
            if let Err(error) = fs::write(path, bytes) {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.restored = true;
        Ok(())
    }
}

impl Drop for FileRestore {
    fn drop(&mut self) {
        if !self.restored {
            for (path, bytes) in &self.originals {
                let _ = fs::write(path, bytes);
            }
        }
    }
}

async fn refused_before_launch(run: &SdkRun, expected: &str) -> io::Result<()> {
    match run.run(&[], &[]).await {
        Err(error)
            if error.kind() == io::ErrorKind::InvalidData && error.to_string() == expected =>
        {
            Ok(())
        }
        Err(error) => Err(io::Error::other(format!(
            "SDK pin control refused for the wrong reason: {error}",
        ))),
        Ok(_) => Err(io::Error::other(
            "SDK pin control launched an unapproved child",
        )),
    }
}

async fn corrupt_owned_files(run: &SdkRun, paths: &[PathBuf], bytes: &[u8]) -> io::Result<()> {
    run.verify_package_assets()?;
    let originals = FileRestore::capture(paths)?;
    let refusal = async {
        for path in paths {
            fs::write(path, bytes)?;
        }
        refused_before_launch(
            run,
            "SDK package or output bytes do not match approved inputs",
        )
        .await
    }
    .await;
    originals.restore()?;
    refusal?;
    run.verify_package_assets()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires dotnet, locked NuGet restores and approved package asset inputs"]
async fn real_restored_pins_reject_wrong_pin_mixed_output_and_poisoned_cache_before_launch()
-> io::Result<()> {
    let source = conformance().join("dotnet-current");
    let project_name = "Switchyard.Conformance.DotNetCurrent.csproj";
    let mut current = SdkRun::prepare(&source, project_name, SdkPin::DeclaredCurrent)?;
    current.build().await?;
    let mut previous = SdkRun::prepare(&source, project_name, SdkPin::Previous)?;
    previous.build().await?;
    let current_assets = current.verify_package_assets()?;
    let previous_assets = previous.verify_package_assets()?;
    let current_bytes: Vec<_> = current_assets
        .iter()
        .map(|asset| {
            sdk_child::pins::read_bounded(
                &asset.package_location,
                identity::ARTIFACT_BYTES as usize,
            )
        })
        .collect::<io::Result<_>>()?;
    let previous_bytes: Vec<_> = previous_assets
        .iter()
        .map(|asset| {
            sdk_child::pins::read_bounded(
                &asset.package_location,
                identity::ARTIFACT_BYTES as usize,
            )
        })
        .collect::<io::Result<_>>()?;
    if current_assets[0].sha256 == previous_assets[0].sha256 {
        return Err(io::Error::other(
            "the two real ServiceBus pins did not produce distinct runtime bytes",
        ));
    }

    for (run, selected, selected_bytes, other_bytes, selected_pin, other_pin) in [
        (
            &current,
            &current_assets,
            &current_bytes,
            &previous_bytes,
            SdkPin::DeclaredCurrent,
            SdkPin::Previous,
        ),
        (
            &previous,
            &previous_assets,
            &previous_bytes,
            &current_bytes,
            SdkPin::Previous,
            SdkPin::DeclaredCurrent,
        ),
    ] {
        let assets_path = run.project_directory().join("obj/project.assets.json");
        run.verify_package_assets()?;
        let original = FileRestore::capture(std::slice::from_ref(&assets_path))?;
        let refusal = async {
            let mut assets: Value = serde_json::from_slice(&sdk_child::pins::read_bounded(
                &assets_path,
                sdk_child::pins::INPUT_BYTES,
            )?)?;
            let libraries = assets["libraries"].as_object_mut().unwrap();
            let selected_key = format!("{SERVICE_BUS}/{}", selected_pin.version());
            let wrong_key = format!("{SERVICE_BUS}/{}", other_pin.version());
            let selected_library = libraries.remove(&selected_key).ok_or_else(|| {
                io::Error::other("positive selected ServiceBus library disappeared")
            })?;
            libraries.insert(wrong_key, selected_library);
            fs::write(&assets_path, serde_json::to_vec(&assets)?)?;
            refused_before_launch(run, "resolved SDK package does not match the selected lock")
                .await
        }
        .await;
        original.restore()?;
        refusal?;
        run.verify_package_assets()?;

        for role in 0..2 {
            let mut different = other_bytes[role].clone();
            if different == selected_bytes[role] {
                // Identical cross-pin Core bytes are valid; use a byte-corruption control instead.
                different.push(0);
            }
            corrupt_owned_files(run, &[selected[role].output_location.clone()], &different).await?;
            corrupt_owned_files(
                run,
                &[
                    selected[role].package_location.clone(),
                    selected[role].output_location.clone(),
                ],
                &different,
            )
            .await?;
        }
        println!(
            "SDK_PIN_NEGATIVE_CONTROLS_VERIFIED {}",
            selected_pin.selector()
        );
    }
    Ok(())
}
