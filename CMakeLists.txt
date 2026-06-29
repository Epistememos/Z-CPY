cmake_minimum_required(VERSION 3.25)
project(zcpy
    VERSION     0.1.0
    DESCRIPTION "Zero-Copy Hybrid C++/Rust Time-Series Storage Engine"
    LANGUAGES   CXX
)

set(CMAKE_CXX_STANDARD          20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS        OFF)

# ── Corrosion: CMake ↔ Cargo bridge ──────────────────────────────────────────
# Fetches and builds Corrosion at configure time; no system install required.
include(FetchContent)
FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG        v0.5
)
FetchContent_MakeAvailable(Corrosion)

# Import the Rust crate.
# Produces a CMake imported target named `zcpy` backed by libzcpy.a (debug)
# or the release artifact depending on CMAKE_BUILD_TYPE.
corrosion_import_crate(MANIFEST_PATH Cargo.toml)

# Generate cxx bridge C++ glue from src/lib.rs.
# Creates an interface target `zcpy_bridge` that:
#   1. Compiles the cxx-generated translation unit (shim implementations).
#   2. Exports INTERFACE_INCLUDE_DIRECTORIES so downstream targets can resolve
#      the generated header at "src/lib.rs.h".
corrosion_add_cxxbridge(zcpy_bridge
    CRATE   zcpy
    FILES   src/lib.rs
)

# ── C++ storage-engine executable ────────────────────────────────────────────
add_executable(zcpy_engine
    src/cpp/main.cpp
    src/cpp/memtable.cpp
)

target_compile_features(zcpy_engine PRIVATE cxx_std_20)

target_compile_options(zcpy_engine PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -O2
    -march=native
    -fstrict-aliasing
)

target_include_directories(zcpy_engine PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Link order: bridge shim object must be resolved before the Rust archive on
# some linkers (the shim defines symbols that Rust's cxx runtime calls back).
target_link_libraries(zcpy_engine PRIVATE
    zcpy_bridge   # cxx-generated C++ stubs + propagates generated include dirs
    zcpy          # Rust static archive (libzcpy.a) via Corrosion
)
