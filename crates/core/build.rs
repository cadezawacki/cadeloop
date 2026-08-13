fn main() {
    // R-080/R-112: vendored llhttp (generated C, MIT), strict mode.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vendor/llhttp");
    println!("cargo:rerun-if-changed={}", root.display());
    cc::Build::new()
        .file(root.join("llhttp.c"))
        .file(root.join("api.c"))
        .file(root.join("http.c"))
        .include(&root)
        .opt_level(3)
        .warnings(false) // vendored generated C
        .compile("llhttp");
}
