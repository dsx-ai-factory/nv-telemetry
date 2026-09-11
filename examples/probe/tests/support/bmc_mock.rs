// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Owns a temporary standalone mock and discovers the fixture's resources.

use std::fs::File;
use std::io::Read as _;
use std::io::Seek as _;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use nv_redfish::bmc_http::reqwest::Client;
use nv_redfish::bmc_http::reqwest::ClientParams;
use nv_redfish::bmc_http::BmcCredentials;
use nv_redfish::bmc_http::CacheSettings;
use nv_redfish::bmc_http::HttpBmc;
use nv_redfish::bmc_http::HttpClient as _;
use nv_redfish::core::ModificationResponse;
use serde_json::json;
use serde_json::Value;
use url::Url;

pub(crate) type Bmc = HttpBmc<Client>;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub(crate) struct Server {
    child: ChildGuard,
    log: File,
    client: Client,
    base: Url,
    credentials: BmcCredentials,
    pub(crate) fixture: Value,
}

pub(crate) struct Resources {
    pub(crate) chassis: String,
    pub(crate) sensor: String,
    pub(crate) log: String,
    pub(crate) entries: String,
    /// `#ComputerSystem.Reset` on the log's system, as the system advertises it.
    pub(crate) system_reset: String,
    /// `#LogService.ClearLog` on the log service, as the service advertises it.
    pub(crate) clear_log: String,
    /// `#Manager.Reset` on the first manager, as the manager advertises it.
    pub(crate) manager_reset: String,
}

/// How long the child mock stays offline after `Manager.Reset`.
pub(crate) const BMC_RESET_WINDOW: Duration = Duration::from_secs(3);

fn action_target(resource: &Value, action: &str) -> String {
    text(&resource["Actions"][action], "target").to_owned()
}

pub(crate) fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .unwrap_or_else(|| panic!("missing string {field}: {value}"))
}

pub(crate) fn link(value: &Value, field: &str) -> String {
    text(&value[field], "@odata.id").to_owned()
}

fn members(value: &Value) -> impl Iterator<Item = &str> {
    value["Members"]
        .as_array()
        .expect("collection members")
        .iter()
        .map(|member| text(member, "@odata.id"))
}

// HttpClient does not re-export its HeaderMap type; infer empty headers
// instead of adding a dependency just to spell their type.
#[allow(clippy::default_trait_access)]
impl Server {
    pub(crate) async fn start() -> Self {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = workspace.join(
            std::env::var_os("BMC_MOCK_ROOT")
                .expect("set BMC_MOCK_ROOT to the built bare-metal-manager-core checkout"),
        );
        let fixture: Value = match std::env::var_os("BMC_MOCK_FIXTURE") {
            Some(path) => serde_json::from_reader(
                File::open(workspace.join(path)).expect("open mock fixture"),
            ),
            None => serde_json::from_str(include_str!("../fixtures/bmc-mock.json")),
        }
        .expect("mock fixture JSON");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve local port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let log = tempfile::tempfile().expect("mock log file");
        let child = ChildGuard(
            Command::new(root.join("target/debug/bmc-mock"))
                .args([
                    "--port",
                    &port.to_string(),
                    "--machine-role",
                    "host",
                    "--state-backend",
                    "internal",
                    "--hardware-profile",
                    text(&fixture, "profile"),
                    "--redfish-auth",
                    "--bmc-reset-duration",
                    &BMC_RESET_WINDOW.as_secs().to_string(),
                    "--cert-path",
                ])
                .arg(root.join("crates/bmc-mock"))
                .stdout(log.try_clone().unwrap())
                .stderr(log.try_clone().unwrap())
                .spawn()
                .expect("start the built standalone bmc-mock"),
        );
        let mut server = Self {
            child,
            log,
            client: Client::with_params(ClientParams::new().accept_invalid_certs(true)).unwrap(),
            base: Url::parse(&format!("https://127.0.0.1:{port}")).unwrap(),
            credentials: BmcCredentials::username_password(
                text(&fixture, "username").to_owned(),
                Some(text(&fixture, "factory_password").to_owned()),
            ),
            fixture,
        };
        let root = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    server.child.0.try_wait().unwrap().is_none(),
                    "mock exited during startup"
                );
                if let Ok(root) = server
                    .client
                    .get::<Value>(
                        server.url("/redfish/v1"),
                        &server.credentials,
                        None,
                        &Default::default(),
                    )
                    .await
                {
                    break root;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("mock readiness deadline");
        server.rotate_password(&root).await;
        server
    }

    fn url(&self, path: &str) -> Url {
        let url = self.base.join(path).expect("resource URL");
        assert_eq!(
            url.origin(),
            self.base.origin(),
            "test resources must stay on the child mock"
        );
        url
    }

    pub(crate) async fn get(&self, path: &str) -> Value {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.client
                .get(self.url(path), &self.credentials, None, &Default::default()),
        )
        .await
        .expect("GET deadline")
        .expect("GET succeeds")
    }

    /// One Redfish action POST; the mock answers 200 `{}` or 204.
    pub(crate) async fn post(&self, path: &str, body: &Value) {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            self.client.post::<_, Value>(
                self.url(path),
                body,
                &self.credentials,
                &Default::default(),
            ),
        )
        .await
        .expect("POST deadline")
        .expect("POST succeeds");
        assert!(matches!(
            response,
            ModificationResponse::Empty | ModificationResponse::Entity(_)
        ));
    }

    /// Appends `count` lifecycle entries — rounded up to a whole power
    /// cycle, so the host ends up on — by cycling the host's power, which
    /// the mock logs and announces as a real BMC does.
    pub(crate) async fn grow_log(&self, resources: &Resources, count: usize) {
        for _ in 0..count.div_ceil(2) {
            for reset in ["ForceOff", "On"] {
                self.post(&resources.system_reset, &json!({"ResetType": reset}))
                    .await;
            }
        }
    }

    async fn rotate_password(&mut self, root: &Value) {
        let service = self.get(&link(root, "AccountService")).await;
        let accounts = self.get(&link(&service, "Accounts")).await;
        for path in members(&accounts) {
            let account = self.get(path).await;
            if account["UserName"] == self.fixture["username"] {
                let response = self
                    .client
                    .patch::<_, Value>(
                        self.url(path),
                        "*".to_owned().into(),
                        &json!({"Password": self.fixture["password"]}),
                        &self.credentials,
                        &Default::default(),
                    )
                    .await
                    .expect("rotate test password");
                assert!(matches!(
                    response,
                    ModificationResponse::Empty | ModificationResponse::Entity(_)
                ));
                self.credentials = self.credentials_with(text(&self.fixture, "password"));
                return;
            }
        }
        panic!("fixture account missing from AccountService");
    }

    pub(crate) async fn discover(&self) -> Resources {
        let root = self.get("/redfish/v1").await;
        let chassis = self.get(&link(&root, "Chassis")).await;
        let mut sensor_resource = None;
        for path in members(&chassis) {
            let chassis = self.get(path).await;
            if chassis.get("Sensors").is_some() {
                let sensors = self.get(&link(&chassis, "Sensors")).await;
                let first = members(&sensors).next().map(str::to_owned);
                if let Some(sensor) = first {
                    sensor_resource = Some((path.to_owned(), sensor));
                    break;
                }
            }
        }
        let (chassis, sensor) = sensor_resource.expect("profile must expose a chassis sensor");
        let managers = self.get(&link(&root, "Managers")).await;
        let manager = self
            .get(members(&managers).next().expect("a manager"))
            .await;
        let manager_reset = action_target(&manager, "#Manager.Reset");
        let systems = self.get(&link(&root, "Systems")).await;
        for path in members(&systems) {
            let system = self.get(path).await;
            if system.get("LogServices").is_none() {
                continue;
            }
            let path = link(&system, "LogServices");
            let services = self.get(&path).await;
            assert_eq!(text(&services, "@odata.id"), path);
            assert_eq!(
                text(&services, "@odata.type"),
                "#LogServiceCollection.LogServiceCollection"
            );
            let first = members(&services).next().map(str::to_owned);
            if let Some(log) = first {
                let service = self.get(&log).await;
                return Resources {
                    chassis,
                    sensor,
                    entries: link(&service, "Entries"),
                    system_reset: action_target(&system, "#ComputerSystem.Reset"),
                    clear_log: action_target(&service, "#LogService.ClearLog"),
                    manager_reset,
                    log,
                };
            }
        }
        panic!("profile must expose a system log service");
    }

    pub(crate) async fn rules(&self, rules: &[Value]) {
        let response = self
            .client
            .delete::<Value>(
                self.url("/Injection/rules"),
                &self.credentials,
                &Default::default(),
            )
            .await
            .expect("clear rules");
        assert!(matches!(
            response,
            ModificationResponse::Empty | ModificationResponse::Entity(_)
        ));
        for rule in rules {
            let response = self
                .client
                .post::<_, Value>(
                    self.url("/Injection/rules"),
                    rule,
                    &self.credentials,
                    &Default::default(),
                )
                .await
                .expect("install rule");
            assert!(matches!(
                response,
                ModificationResponse::Empty | ModificationResponse::Entity(_)
            ));
        }
    }

    pub(crate) fn credentials_with(&self, password: &str) -> BmcCredentials {
        BmcCredentials::username_password(
            text(&self.fixture, "username").to_owned(),
            Some(password.to_owned()),
        )
    }

    pub(crate) fn bmc(&self) -> Arc<Bmc> {
        self.bmc_with(self.credentials.clone())
    }

    pub(crate) fn bmc_with(&self, credentials: BmcCredentials) -> Arc<Bmc> {
        Arc::new(HttpBmc::new(
            self.client.clone(),
            self.base.clone(),
            credentials,
            CacheSettings::default(),
        ))
    }

    pub(crate) async fn probe(
        &self,
        targets: &[(&str, &str)],
        count: usize,
        strict: bool,
        password: &str,
    ) -> i32 {
        let mut command = Command::new(env!("CARGO_BIN_EXE_nv-telemetry-probe"));
        command
            .args([
                "--mode",
                "http",
                "--endpoint-id",
                "http-test",
                "--base-url",
                self.base.as_str(),
                "--insecure",
                "--cadence-ms",
                "100",
                "--count",
                &count.to_string(),
            ])
            .env("PROBE_USERNAME", text(&self.fixture, "username"))
            .env("PROBE_PASSWORD", password)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        for (flag, path) in targets {
            command.args([flag, path]);
        }
        if strict {
            command.arg("--strict");
        }
        let mut child = ChildGuard(command.spawn().expect("probe runs"));
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                if let Some(status) = child.0.try_wait().expect("probe status") {
                    break status.code().expect("probe exited");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("probe deadline")
    }

    pub(crate) fn stop(&mut self) {
        let _ = self.child.0.kill();
        self.child.0.wait().expect("reap child mock");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.0.kill();
        let _ = self.child.0.wait();
        if std::thread::panicking() {
            let mut log = String::new();
            let _ = self.log.rewind();
            let _ = self.log.read_to_string(&mut log);
            #[allow(clippy::print_stderr)]
            {
                eprintln!("mock log:\n{log}");
            }
        }
    }
}
