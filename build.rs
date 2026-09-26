fn main() {
    println!("cargo:rerun-if-changed=proto/wire.proto");
    prost_build::compile_protos(&["proto/wire.proto"], &["proto"])
        .expect("failed to compile proto/wire.proto (is `protoc` on PATH?)");
}
