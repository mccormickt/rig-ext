//! Process ownership for local Worker integration tests.
#![cfg(unix)]
#![allow(clippy::expect_used, clippy::panic)]

use std::{
    ffi::OsStr,
    fs,
    io::Write,
    net::TcpListener,
    os::unix::process::CommandExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

pub fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("scripts directory")
        .parent()
        .expect("repository directory")
        .to_owned()
}

pub fn command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    let home = PathBuf::from(std::env::var_os("HOME").expect("HOME"));
    let mut paths = vec![
        home.join(".cargo/bin"),
        home.join(".local/bin"),
        root().join("scripts/workers/node_modules/.bin"),
    ];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    command
        .current_dir(root())
        .env("PATH", std::env::join_paths(paths).expect("tool PATH"));
    command
}

pub fn http() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .no_proxy()
        // Test exchanges do not share idle connections across runtime fault cases.
        .pool_max_idle_per_host(0)
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(45))
        .build()
        .expect("HTTP client")
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("loopback listener")
        .local_addr()
        .expect("listener address")
        .port()
}

struct Process {
    child: Child,
    log: tempfile::NamedTempFile,
    errors: tempfile::NamedTempFile,
}

impl Process {
    fn start(command: &mut Command) -> Self {
        let log = tempfile::NamedTempFile::new().expect("process log");
        let errors = tempfile::NamedTempFile::new().expect("process errors");
        let child = command
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(log.reopen().expect("stdout log"))
            .stderr(errors.reopen().expect("stderr log"))
            .spawn()
            .unwrap_or_else(|error| panic!("cannot start {command:?}: {error}"));
        Self { child, log, errors }
    }

    fn output(&self) -> String {
        fs::read_to_string(self.log.path()).expect("read process log")
    }

    fn diagnostics(&self) -> String {
        format!(
            "{}\n{}",
            self.output(),
            fs::read_to_string(self.errors.path()).expect("read process errors")
        )
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if thread::panicking() {
            let _ = writeln!(
                std::io::stderr(),
                "Child process diagnostics:\n{}\n{}",
                fs::read_to_string(self.log.path()).unwrap_or_default(),
                fs::read_to_string(self.errors.path()).unwrap_or_default(),
            );
        }
        if matches!(self.child.try_wait(), Ok(None)) {
            // Let the owner dispose its runtime and temporary resources first.
            let _ = Command::new("kill")
                .args(["-TERM", "--", &self.child.id().to_string()])
                .output();
            let deadline = Instant::now() + Duration::from_secs(5);
            while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(25));
            }
        }
        // Stop any remaining members of the owned group, including subprocesses.
        let group = format!("-{}", self.child.id());
        let _ = Command::new("kill").args(["-KILL", "--", &group]).output();
        let _ = self.child.wait();
    }
}

pub fn run(command: &mut Command) -> String {
    let mut process = Process::start(command);
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        if let Some(status) = process.child.try_wait().expect("command status") {
            let output = process.output();
            assert!(
                status.success(),
                "{command:?}: {status}\n{}",
                process.diagnostics()
            );
            return output;
        }
        assert!(
            Instant::now() < deadline,
            "command timed out: {command:?}\n{}",
            process.diagnostics()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

pub struct Service {
    command: Command,
    process: Option<Process>,
}

impl Service {
    pub fn start(mut command: Command) -> Self {
        let process = Some(Process::start(&mut command));
        Self { command, process }
    }

    pub fn restart(&mut self) {
        self.process.take();
        self.process = Some(Process::start(&mut self.command));
    }

    pub fn wait_http(&mut self, url: &str) {
        let client = http();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let process = self.process.as_mut().expect("service process");
            assert!(
                process.child.try_wait().expect("service status").is_none(),
                "service exited: {:?}\n{}",
                self.command,
                process.diagnostics()
            );
            if client
                .get(url)
                .timeout(Duration::from_millis(500))
                .send()
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "service not ready at {url}\n{}",
                process.diagnostics()
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}
