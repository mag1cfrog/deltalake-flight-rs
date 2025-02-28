fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure protoc to find external dependencies
    let proto_dir = "proto";
    // let arrow_flight_proto = "proto/arrow/flight/protocol/flight.proto";
    
    // 2. Configure prost
    let mut prost_config = prost_build::Config::new();

    prost_config.bytes(["."]); // Use Bytes instead of Vec<u8>
    // prost_config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    // 3. Generate Tonic server code
    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(true) // Generate client code
        .extern_path(".arrow.flight.protocol", "::arrow_flight")
        .compile_protos_with_config(
            prost_config,
            &["proto/deltaflight.proto"], // Your protos
            &[proto_dir], // Include paths
        )?;

    Ok(())
}