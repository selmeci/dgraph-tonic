fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .out_dir("./src/api/v1_0_x")
        .compile(&["proto/api_v1.0.x.proto"], &["proto"])?;
    tonic_build::configure()
        .build_server(false)
        .out_dir("./src/api/v1_1_x")
        .compile(&["proto/api_v1.1.x.proto"], &["proto"])?;
    Ok(())
}
