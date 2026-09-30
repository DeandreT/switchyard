use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/switchyard/admin/v1/admin.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-env-changed=PROTOC");
    let descriptor = PathBuf::from(env::var("OUT_DIR")?).join("switchyard.admin.v1.bin");
    tonic_prost_build::configure()
        .file_descriptor_set_path(descriptor)
        .generate_default_stubs(true)
        .compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}
