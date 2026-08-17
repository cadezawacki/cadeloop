fn main() {
    // R-080/R-112: vendored llhttp (generated C, MIT), strict mode.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vendor/llhttp");
    println!("cargo:rerun-if-changed={}", root.display());

    // Cross-CHECKING the msvc target from a non-Windows host (the local
    // `cargo check --target x86_64-pc-windows-msvc` used to validate the
    // Windows backends from Linux) has no MSVC C toolchain. `check` never
    // links, so skipping the C build keeps the Rust type-check honest;
    // real Windows builds (CI runners, host == target) compile it.
    let host = std::env::var("HOST").unwrap_or_default();
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("msvc") && !host.contains("windows") {
        println!(
            "cargo:warning=skipping llhttp C build for {target} cross-check (no MSVC on {host}); \
             link-requiring builds must run on Windows"
        );
        return;
    }

    cc::Build::new()
        .file(root.join("llhttp.c"))
        .file(root.join("api.c"))
        .file(root.join("http.c"))
        .include(&root)
        .opt_level(3)
        .warnings(false) // vendored generated C
        .compile("llhttp");
}
