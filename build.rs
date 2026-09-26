// build.rs — gera os tipos Rust do proto/wsm.proto via prost-build.
//
// O .proto é a ÚNICA fonte de verdade do protocolo; server e clientes (RN,
// via protobufjs) geram código a partir dele, então nunca dessincronizam.
//
// protoc: prost-build precisa do binário `protoc`. Em vez de exigir instalação
// manual no PATH, localizamos o protoc pré-compilado que vem com a crate
// `protoc-bin-vendored` (protoc estático por plataforma — funciona em dev local
// E no Docker sem deps extras). Se a env var PROTOC já existir, ela vence.

use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/wsm.proto");
    println!("cargo:rerun-if-changed=build.rs");

    // Garante que o protoc vendored esteja disponível no ambiente do build.
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(path) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", path);
        }
    }

    prost_build::Config::new()
        .out_dir(std::env::var("OUT_DIR").unwrap())
        .compile_protos(&["proto/wsm.proto"], &["proto"])
        .map_err(|e| {
            eprintln!("cargo:warning=prost-build falhou: {e}");
            e
        })?;

    println!("cargo:rerun-if-env-changed=PROTOC");
    Ok(())
}
