use std::{
    env::{self, consts::EXE_EXTENSION},
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use current_platform::CURRENT_PLATFORM;
use structopt::StructOpt;

#[derive(StructOpt)]
enum Opt {
    Coverage,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();
    match opt {
        Opt::Coverage => do_coverage(),
    }
}

fn do_coverage() -> Result<(), Box<dyn Error>> {
    const COVERAGE_DIR: &str = "coverage";
    const COVERAGE_FILE_PART: &str = "coverage/part-%m.profraw";
    const COVERAGE_FILE_MERGED: &str = "coverage/merged.profdata";
    const COVERAGE_FILE_RESULT: &str = "coverage/lcov.info";
    const TOOLCHAIN: &str = "nightly";

    // Create the coverage directory if it does not exist
    fs::create_dir_all(COVERAGE_DIR)?;

    // Clear out pre-existing entries from the coverage directory
    for entry in fs::read_dir(COVERAGE_DIR)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let path = entry.path();
            if path.extension() == Some("profraw".as_ref()) {
                fs::remove_file(&path)?;
            }
        }
    }

    // Run `cargo test` and generate coverage files
    Command::new("rustup")
        .args([
            "run",
            TOOLCHAIN,
            "--",
            "cargo",
            "test",
            "--profile",
            "coverage",
        ])
        .envs([
            ("RUSTFLAGS", "-C instrument-coverage"),
            ("LLVM_PROFILE_FILE", COVERAGE_FILE_PART),
        ])
        .status()?;

    let cargo_path: PathBuf = String::from_utf8(
        Command::new("rustup")
            .args(["which", "cargo", "--toolchain", TOOLCHAIN])
            .output()?
            .stdout,
    )?
    .into();
    let toolchain_dir = cargo_path.parent().unwrap().parent().unwrap();
    let target_dir = toolchain_dir
        .join("lib")
        .join("rustlib")
        .join(CURRENT_PLATFORM);
    let target_bin_dir = target_dir.join("bin");

    let mut coverage_files = Vec::new();
    for entry in fs::read_dir(COVERAGE_DIR)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let path = entry.path();
            if path.extension() == Some("profraw".as_ref()) {
                coverage_files.push(path);
            }
        }
    }

    Command::new("llvm-profdata")
        .env("PATH", &target_bin_dir)
        .args(["merge", "-sparse"])
        .args(coverage_files)
        .args(["-o", COVERAGE_FILE_MERGED])
        .status()?;

    let mut binaries = Vec::new();
    for entry in fs::read_dir("target/coverage/deps")? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let path = entry.path();
            if path.extension().unwrap_or_default() == EXE_EXTENSION {
                binaries.push(path);
            }
        }
    }

    let new_path = env::join_paths(
        [target_bin_dir]
            .into_iter()
            .chain(env::var_os("PATH").map(Into::into)),
    )?;

    fs::write(
        COVERAGE_FILE_RESULT,
        Command::new("llvm-cov")
            .env("PATH", &new_path)
            .args([
                "export",
                "-instr-profile",
                COVERAGE_FILE_MERGED,
                "-format",
                "lcov",
            ])
            .args(
                binaries
                    .iter()
                    .flat_map(|binary| ["-object".as_ref(), binary.as_path()])
                    .skip(1),
            )
            .arg(".")
            .output()?
            .stdout,
    )?;

    for binary in binaries {
        let dir_path = Path::new(COVERAGE_DIR).join(binary.file_stem().unwrap());
        fs::create_dir_all(&dir_path)?;
        fs::write(
            dir_path.join("index.html"),
            Command::new("llvm-cov")
                .env("PATH", &new_path)
                .args([
                    "show",
                    "-instr-profile",
                    COVERAGE_FILE_MERGED,
                    "-format",
                    "html",
                ])
                .arg(binary)
                .arg(".")
                .output()?
                .stdout,
        )?;
    }

    Ok(())
}
