use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "worker-replay")]
    assets::build()?;
    Ok(())
}

#[cfg(feature = "worker-replay")]
mod assets {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        env,
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        process::Command,
    };

    // Sub-builds have their own target directory and no dependency on the host adapter.
    fn command(program: impl AsRef<std::ffi::OsStr>, guest: &Path) -> Command {
        let cflags = env::var("CFLAGS_wasm32_unknown_unknown").unwrap_or_default();
        let mapped_guest = guest.to_string_lossy().replace('\'', "'\\''");
        let mut command = Command::new(program);
        command.current_dir(guest)
            .env("CARGO_TARGET_DIR", guest.join("target"))
            .env("CC_SHELL_ESCAPED_FLAGS", "1")
            .env("CFLAGS_wasm32_unknown_unknown", format!("{cflags} '-ffile-prefix-map={mapped_guest}=/codemode-guest'"))
            .env_remove("RUSTFLAGS")
            .env("CARGO_ENCODED_RUSTFLAGS", format!("--remap-path-prefix={}=/codemode-guest\x1f--remap-path-prefix={}=/codemode-core",
                guest.display(), rig_codemode_core::SOURCE_DIRECTORY));
        command
    }

    fn run(command: &mut Command) -> Result<(), Box<dyn Error>> {
        let output = command.output()?;
        if !output.status.success() {
            return Err(format!(
                "{command:?} failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    fn tool(variable: &str, default: &str, version: &str) -> Result<OsString, Box<dyn Error>> {
        println!("cargo:rerun-if-env-changed={variable}");
        let path = env::var_os(variable).unwrap_or_else(|| default.into());
        let output = Command::new(&path)
            .arg("--version")
            .output()
            .map_err(|error| format!("Install {default} ({version}) or set {variable}: {error}"))?;
        if !output.status.success() || String::from_utf8_lossy(&output.stdout).trim() != version {
            return Err(format!(
                "{variable} must select {version}; build scripts do not install tools"
            )
            .into());
        }
        // Worker-build needs an explicit path so it cannot use its download fallback.
        if Path::new(&path).is_absolute() {
            return Ok(path);
        }
        for directory in env::split_paths(&env::var_os("PATH").unwrap_or_default()) {
            let candidate = directory.join(&path);
            if candidate.is_file() {
                return Ok(candidate.into_os_string());
            }
        }
        Ok(fs::canonicalize(&path)?.into_os_string())
    }

    pub fn build() -> Result<(), Box<dyn Error>> {
        let worker = cfg!(feature = "worker-replay");
        println!("cargo:rerun-if-changed=runtime/guest");
        println!(
            "cargo:rerun-if-changed={}",
            rig_codemode_core::SOURCE_DIRECTORY
        );
        println!("cargo:rerun-if-env-changed=PATH");
        for name in [
            "CC",
            "AR",
            "CFLAGS",
            "CC_wasm32_unknown_unknown",
            "AR_wasm32_unknown_unknown",
            "CFLAGS_wasm32_unknown_unknown",
            "CC_wasm32-unknown-unknown",
            "AR_wasm32-unknown-unknown",
            "CFLAGS_wasm32-unknown-unknown",
            "TARGET_CC",
            "TARGET_AR",
            "TARGET_CFLAGS",
        ] {
            println!("cargo:rerun-if-env-changed={name}");
        }
        let bindgen = tool("WASM_BINDGEN_BIN", "wasm-bindgen", "wasm-bindgen 0.2.129")?;
        let rustc = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let target = Command::new(rustc)
            .args([
                "--print",
                "target-libdir",
                "--target",
                "wasm32-unknown-unknown",
            ])
            .output()?;
        let target = PathBuf::from(String::from_utf8(target.stdout)?.trim());
        if !target.is_dir() {
            return Err(
                "Install the wasm32-unknown-unknown Rust target before building replay adapters"
                    .into(),
            );
        }
        let out = PathBuf::from(env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);
        let guest = out.join("guest");
        fs::create_dir_all(guest.join("src"))?;
        let manifest = fs::read_to_string("runtime/guest/Cargo.toml.in")?.replace(
            "@CORE@",
            &serde_json::to_string(rig_codemode_core::SOURCE_DIRECTORY)?,
        );
        fs::write(guest.join("Cargo.toml"), manifest)?;
        fs::copy("runtime/guest/Cargo.lock", guest.join("Cargo.lock"))?;
        for name in ["lib.rs", "workers.rs"] {
            fs::copy(
                Path::new("runtime/guest/src").join(name),
                guest.join("src").join(name),
            )?;
        }
        let mut files = serde_json::Map::new();
        if worker {
            let builder = tool("WORKER_BUILD_BIN", "worker-build", "0.8.3")?;
            let esbuild = tool("ESBUILD_BIN", "esbuild", "0.28.2")?;
            run(command(builder, &guest)
                .env("WASM_BINDGEN_BIN", &bindgen)
                .env("ESBUILD_BIN", esbuild)
                .args(["--release", "--no-opt", "--out-dir"])
                .arg(out.join("worker"))
                .args(["--", "--locked", "-j1", "--features", "workers"]))?;
            for name in ["index.js", "index_bg.wasm"] {
                fs::copy(out.join("worker").join(name), out.join(name))?;
                record(&out, name, &mut files)?;
            }
            fs::write(
                out.join("identity.txt"),
                format!(
                    "worker-replay-v1:quickjs-ng-0.16.2:{}:{}\n",
                    files["index.js"]["sha256"]
                        .as_str()
                        .ok_or("missing JS hash")?,
                    files["index_bg.wasm"]["sha256"]
                        .as_str()
                        .ok_or("missing WASM hash")?
                ),
            )?;
        }
        fs::write(
            out.join("fingerprint.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": 1, "engine": "quickjs-ng-0.16.2", "rquickjs": "0.14.0", "files": files
            }))?,
        )?;
        Ok(())
    }

    fn record(
        out: &Path,
        name: &str,
        files: &mut serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), Box<dyn Error>> {
        let bytes = fs::read(out.join(name))?;
        files.insert(name.into(), serde_json::json!({"bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes))}));
        Ok(())
    }
}
