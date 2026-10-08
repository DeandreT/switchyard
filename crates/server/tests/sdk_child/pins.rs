//! Fixed package selection and approved runtime bytes, not transitive custody.

use std::{
    fmt, fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
};

use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};

use super::identity;

pub const INPUT_BYTES: usize = 2 * 1024 * 1024;
const XML_BYTES: usize = 64 * 1024;
const SERVICE_BUS: &str = "Azure.Messaging.ServiceBus";
const CORE: &str = "Azure.Core";
const FRAMEWORK: &str = "net10.0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SdkPin {
    DeclaredCurrent,
    Previous,
}

impl SdkPin {
    pub fn selector(self) -> &'static str {
        match self {
            Self::DeclaredCurrent => "declared-current",
            Self::Previous => "previous",
        }
    }

    pub fn version(self) -> &'static str {
        match self {
            Self::DeclaredCurrent => "7.21.0",
            Self::Previous => "7.20.2",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovedPin {
    schema: u32,
    selector: String,
    framework: String,
    packages: Vec<ApprovedAsset>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovedAsset {
    role: String,
    id: String,
    version: String,
    runtime_asset: String,
    sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResolvedAsset {
    pub role: String,
    pub package_id: String,
    pub package_version: String,
    pub runtime_asset: String,
    pub package_location: PathBuf,
    pub output_location: PathBuf,
    pub sha256: String,
}

pub struct PinInputs {
    pin: SdkPin,
    approved: ApprovedPin,
    lock: Value,
    lock_bytes: Vec<u8>,
}

impl PinInputs {
    pub fn read(pin: SdkPin, project: &Path, directory: &Path) -> io::Result<Self> {
        Self::parse(
            pin,
            &read_bounded(project, XML_BYTES)?,
            &read_bounded(&directory.join("SdkPin.props"), XML_BYTES)?,
            &read_bounded(&directory.join("packages.lock.json"), INPUT_BYTES)?,
            &read_bounded(&directory.join("approved-assets.json"), XML_BYTES)?,
        )
    }

    pub fn parse(
        pin: SdkPin,
        project: &[u8],
        props: &[u8],
        lock: &[u8],
        approved: &[u8],
    ) -> io::Result<Self> {
        validate_project(project)?;
        validate_props(pin, props)?;
        let approved: ApprovedPin = serde_json::from_value(parse_json(approved, XML_BYTES)?)
            .map_err(|_| invalid("invalid approved SDK asset inputs"))?;
        if approved.schema != 1
            || approved.selector != pin.selector()
            || approved.framework != FRAMEWORK
            || approved.packages.len() != 2
        {
            return Err(invalid("approved SDK inputs do not match the selected pin"));
        }
        for (asset, (role, id)) in approved
            .packages
            .iter()
            .zip([("service_bus", SERVICE_BUS), ("core", CORE)])
        {
            if asset.role != role
                || asset.id != id
                || !stable_version(&asset.version)
                || !hex_hash(&asset.sha256)
                || !relative_path(&asset.runtime_asset)
                || Path::new(&asset.runtime_asset)
                    .file_name()
                    .and_then(|name| name.to_str())
                    != Some(format!("{id}.dll").as_str())
            {
                return Err(invalid("invalid approved SDK runtime asset"));
            }
        }
        if approved.packages[0].version != pin.version() {
            return Err(invalid("approved ServiceBus package is the wrong pin"));
        }
        let lock_bytes = lock.to_vec();
        let lock = parse_json(lock, INPUT_BYTES)?;
        if lock.get("version").and_then(Value::as_u64) != Some(1) {
            return Err(invalid("unsupported SDK package lock schema"));
        }
        let frameworks = object(&lock, "dependencies")?;
        if frameworks.len() != 1 {
            return Err(invalid("SDK lock must have exactly one target framework"));
        }
        let dependencies = frameworks
            .get(FRAMEWORK)
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("SDK lock omitted net10.0"))?;
        for asset in &approved.packages {
            let dependency = unique_package(dependencies, &asset.id, false)?;
            if string(dependency, "resolved")? != asset.version
                || string(dependency, "contentHash")?.is_empty()
            {
                return Err(invalid("locked SDK package does not match approved inputs"));
            }
            if asset.id == SERVICE_BUS
                && (string(dependency, "type")? != "Direct"
                    || !exact_range(string(dependency, "requested")?, pin.version()))
            {
                return Err(invalid(
                    "ServiceBus lock input is not the exact selected range",
                ));
            }
        }
        Ok(Self {
            pin,
            approved,
            lock,
            lock_bytes,
        })
    }

    pub fn verify_lock(&self, path: &Path) -> io::Result<()> {
        if read_bounded(path, INPUT_BYTES)? != self.lock_bytes {
            return Err(invalid("SDK restore changed the selected lock file"));
        }
        Ok(())
    }

    pub fn verify_assets(
        &self,
        assets_path: &Path,
        project: &Path,
        packages: &Path,
        output: &Path,
    ) -> io::Result<Vec<ResolvedAsset>> {
        let assets = parse_json(&read_bounded(assets_path, INPUT_BYTES)?, INPUT_BYTES)?;
        self.verify_assets_value(&assets, project, packages, output)
    }

    pub fn verify_assets_value(
        &self,
        assets: &Value,
        project: &Path,
        packages: &Path,
        output: &Path,
    ) -> io::Result<Vec<ResolvedAsset>> {
        if assets.get("version").and_then(Value::as_u64) != Some(3) {
            return Err(invalid("unsupported SDK assets schema"));
        }
        let project = project.canonicalize()?;
        let packages = packages.canonicalize()?;
        let output = output.canonicalize()?;
        let folders = object(assets, "packageFolders")?;
        if folders.len() != 1
            || Path::new(folders.keys().next().expect("one package folder")).canonicalize()?
                != packages
        {
            return Err(invalid("SDK assets escaped the isolated package cache"));
        }
        let restore = assets
            .get("project")
            .and_then(|value| value.get("restore"))
            .ok_or_else(|| invalid("SDK assets omitted restore identity"))?;
        if Path::new(string(restore, "projectPath")?).canonicalize()? != project
            || Path::new(string(restore, "packagesPath")?).canonicalize()? != packages
            || restore
                .get("fallbackFolders")
                .is_some_and(|value| value.as_array().is_none_or(|folders| !folders.is_empty()))
        {
            return Err(invalid(
                "SDK restore identity or fallback folders are unexpected",
            ));
        }
        let frameworks = object(assets.get("project").unwrap(), "frameworks")?;
        if frameworks.len() != 1 || !frameworks.contains_key(FRAMEWORK) {
            return Err(invalid(
                "SDK project assets have unexpected target frameworks",
            ));
        }
        let direct = object(&frameworks[FRAMEWORK], "dependencies")?;
        let requested = unique_package(direct, SERVICE_BUS, false)?;
        if !exact_range(string(requested, "version")?, self.pin.version()) {
            return Err(invalid(
                "restored ServiceBus input is not the exact selected range",
            ));
        }
        let targets = object(assets, "targets")?;
        if targets.len() != 1 {
            return Err(invalid("SDK assets have ambiguous runtime targets"));
        }
        let target = targets
            .get(FRAMEWORK)
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("SDK assets omitted the selected net10.0 runtime target"))?;
        let libraries = object(assets, "libraries")?;
        let locked = &self.lock["dependencies"][FRAMEWORK];
        let mut resolved = Vec::with_capacity(2);
        for expected in &self.approved.packages {
            let library = unique_package(libraries, &expected.id, true)?;
            let selected = unique_package(target, &expected.id, true)?;
            let key = format!("{}/{}", expected.id, expected.version);
            if libraries.get(&key) != Some(library)
                || target.get(&key) != Some(selected)
                || string(library, "type")? != "package"
                || string(selected, "type")? != "package"
                || string(library, "sha512")? != string(&locked[&expected.id], "contentHash")?
            {
                return Err(invalid(
                    "resolved SDK package does not match the selected lock",
                ));
            }
            let package_path = string(library, "path")?;
            if !relative_path(package_path)
                || package_path
                    != format!("{}/{}", expected.id.to_ascii_lowercase(), expected.version)
            {
                return Err(invalid("resolved SDK package path is unexpected"));
            }
            let runtime = object(selected, "runtime")?;
            let filename = format!("{}.dll", expected.id);
            let matching: Vec<_> = runtime
                .keys()
                .filter(|path| {
                    Path::new(path).file_name().and_then(|name| name.to_str())
                        == Some(filename.as_str())
                })
                .collect();
            if matching.len() != 1 || matching[0] != &expected.runtime_asset {
                return Err(invalid("SDK runtime asset does not match approved inputs"));
            }
            if !library
                .get("files")
                .and_then(Value::as_array)
                .is_some_and(|files| {
                    files
                        .iter()
                        .any(|file| file.as_str() == Some(&expected.runtime_asset))
                })
            {
                return Err(invalid(
                    "selected SDK runtime asset is absent from package files",
                ));
            }
            let package_directory = packages.join(package_path).canonicalize()?;
            let package_location = package_directory
                .join(&expected.runtime_asset)
                .canonicalize()?;
            let output_location = output.join(filename).canonicalize()?;
            if !package_directory.starts_with(&packages)
                || !package_location.starts_with(&package_directory)
                || output_location.parent() != Some(output.as_path())
            {
                return Err(invalid("SDK runtime asset escaped its expected directory"));
            }
            if identity::hash_file(&package_location)? != expected.sha256
                || identity::hash_file(&output_location)? != expected.sha256
            {
                return Err(invalid(
                    "SDK package or output bytes do not match approved inputs",
                ));
            }
            resolved.push(ResolvedAsset {
                role: expected.role.clone(),
                package_id: expected.id.clone(),
                package_version: expected.version.clone(),
                runtime_asset: expected.runtime_asset.clone(),
                package_location,
                output_location,
                sha256: expected.sha256.clone(),
            });
        }
        Ok(resolved)
    }
}

fn validate_project(bytes: &[u8]) -> io::Result<()> {
    let document = xml_document(bytes)?;
    if !document.root_element().has_tag_name("Project")
        || document.root_element().attribute("Sdk") != Some("Microsoft.NET.Sdk")
        || xml_value(&document, "TargetFramework")? != FRAMEWORK
        || document.descendants().any(|node| {
            node.has_tag_name("TargetFrameworks")
                || node.has_tag_name("SwitchyardServiceBusVersion")
        })
    {
        return Err(invalid("unexpected SDK project template"));
    }
    let imports: Vec<_> = document
        .descendants()
        .filter(|node| node.has_tag_name("Import"))
        .collect();
    let references: Vec<_> = document
        .descendants()
        .filter(|node| node.has_tag_name("PackageReference"))
        .collect();
    if imports.len() != 1
        || imports[0].attribute("Project") != Some("SdkPin.props")
        || imports[0].attributes().len() != 1
        || references.len() != 1
        || references[0].attribute("Include") != Some(SERVICE_BUS)
        || references[0].attribute("Version") != Some("$(SwitchyardServiceBusVersion)")
        || references[0].attributes().len() != 2
        || references[0].children().any(|node| node.is_element())
    {
        return Err(invalid(
            "SDK project must import one explicit pin and one ServiceBus reference",
        ));
    }
    Ok(())
}

fn validate_props(pin: SdkPin, bytes: &[u8]) -> io::Result<()> {
    let document = xml_document(bytes)?;
    let root = document.root_element();
    if !root.has_tag_name("Project")
        || root.attributes().len() != 0
        || xml_value(&document, "SwitchyardSdkSelector")? != pin.selector()
        || xml_value(&document, "SwitchyardServiceBusVersion")? != format!("[{}]", pin.version())
        || xml_value(&document, "RestorePackagesWithLockFile")? != "true"
        || xml_value(&document, "RestoreLockedMode")? != "true"
        || document
            .descendants()
            .filter(|node| node.is_element())
            .any(|node| {
                !matches!(
                    node.tag_name().name(),
                    "Project"
                        | "PropertyGroup"
                        | "SwitchyardSdkSelector"
                        | "SwitchyardServiceBusVersion"
                        | "RestorePackagesWithLockFile"
                        | "RestoreLockedMode"
                ) || node.attributes().len() != 0
            })
    {
        return Err(invalid("SDK pin props are not the exact selected inputs"));
    }
    Ok(())
}

fn xml_document(bytes: &[u8]) -> io::Result<roxmltree::Document<'_>> {
    if bytes.len() > XML_BYTES {
        return Err(invalid("SDK XML input exceeded its byte ceiling"));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| invalid("SDK XML input is not UTF-8"))?;
    roxmltree::Document::parse(text).map_err(|_| invalid("malformed SDK XML input"))
}

fn xml_value<'a, 'input>(
    document: &'a roxmltree::Document<'input>,
    name: &str,
) -> io::Result<&'a str> {
    let mut matching = document
        .descendants()
        .filter(|node| node.has_tag_name(name));
    let node = matching
        .next()
        .ok_or_else(|| invalid("missing SDK XML property"))?;
    if matching.next().is_some()
        || node.attributes().len() != 0
        || node.children().any(|child| child.is_element())
        || !node.parent().is_some_and(|parent| {
            parent.has_tag_name("PropertyGroup") && parent.attributes().len() == 0
        })
    {
        return Err(invalid("ambiguous or conditional SDK XML property"));
    }
    node.text().ok_or_else(|| invalid("empty SDK XML property"))
}

fn exact_range(range: &str, version: &str) -> bool {
    range == format!("[{version}]") || range == format!("[{version}, {version}]")
}

fn stable_version(version: &str) -> bool {
    let parts: Vec<_> = version.split('.').collect();
    parts.len() == 3
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part.len() == 1 || !part.starts_with('0'))
        })
}

fn relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn hex_hash(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn unique_package<'a>(
    values: &'a Map<String, Value>,
    id: &str,
    versioned: bool,
) -> io::Result<&'a Value> {
    let mut matching = values.iter().filter(|(name, _)| {
        let name = name.as_str();
        let name = if versioned {
            name.split('/').next().unwrap_or(name)
        } else {
            name
        };
        name.eq_ignore_ascii_case(id)
    });
    let (name, value) = matching
        .next()
        .ok_or_else(|| invalid("missing selected SDK package"))?;
    if matching.next().is_some() || (!versioned && name != id) {
        return Err(invalid("ambiguous selected SDK package"));
    }
    Ok(value)
}

fn object<'a>(value: &'a Value, name: &str) -> io::Result<&'a Map<String, Value>> {
    value
        .get(name)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing SDK JSON object"))
}

fn string<'a>(value: &'a Value, name: &str) -> io::Result<&'a str> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("missing SDK JSON string"))
}

pub fn read_bounded(path: &Path, ceiling: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(ceiling as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > ceiling {
        return Err(invalid("SDK input exceeded its byte ceiling"));
    }
    Ok(bytes)
}

fn parse_json(bytes: &[u8], ceiling: usize) -> io::Result<Value> {
    if bytes.len() > ceiling {
        return Err(invalid("SDK JSON input exceeded its byte ceiling"));
    }
    serde_json::from_slice::<UniqueJson>(bytes)
        .map(|value| value.0)
        .map_err(|_| invalid("malformed or duplicate-key SDK JSON input"))
}

struct UniqueJson(Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueJson;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("JSON without duplicate object keys")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::from(value)))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::from(value)))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::from(value)))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::String(value.to_owned())))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::String(value)))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueJson>()? {
                    values.push(value.0);
                }
                Ok(UniqueJson(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut mapping: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = mapping.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON object key"));
                    }
                    values.insert(key, mapping.next_value::<UniqueJson>()?.0);
                }
                Ok(UniqueJson(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
