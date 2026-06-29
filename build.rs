fn main() {
    cxx_build::bridge("src/lib.rs")
        .std("c++20")
        .flag_if_supported("-Wall")
        .flag_if_supported("-Wextra")
        .compile("zcpy_cxxbridge");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/ingestion.rs");
    println!("cargo:rerun-if-changed=build.rs");
}
