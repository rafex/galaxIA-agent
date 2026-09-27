fn main() {
    println!("cargo:rerun-if-changed=protocol/fhs-protocol.proto");
    prost_build::Config::new()
        .compile_protos(&["protocol/fhs-protocol.proto"], &["protocol"])
        .expect("canonical FHS protobuf must compile");
}
