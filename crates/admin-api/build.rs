fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = "../../proto";
    let schema = "../../proto/switchyard/admin/v1/admin.proto";
    println!("cargo:rerun-if-changed={schema}");
    // System protoc; generated bindings stay in Cargo's OUT_DIR.
    tonic_prost_build::configure().compile_protos(&[schema], &[root])?;
    Ok(())
}
