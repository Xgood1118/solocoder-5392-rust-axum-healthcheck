use crate::models::ServiceConfig;
use anyhow::{Context, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    service: Vec<ServiceConfig>,
    #[serde(default)]
    alert: AlertConfig,
    #[serde(default)]
    server: ServerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AlertConfig {
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            webhooks: vec![],
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default = "default_level")]
    pub level: String,
}

fn default_level() -> String {
    "critical".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8080
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub services: Vec<ServiceConfig>,
    pub alert: AlertConfig,
    pub server: ServerConfig,
}

pub fn load_config(path: &Path) -> Result<AppConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置文件失败: {}", path.display()))?;

    let config_file: ConfigFile = toml::from_str(&content)
        .with_context(|| format!("解析 TOML 配置失败: {}", path.display()))?;

    Ok(AppConfig {
        services: config_file.service,
        alert: config_file.alert,
        server: config_file.server,
    })
}

pub async fn watch_config(
    path: PathBuf,
    tx: mpsc::Sender<AppConfig>,
) -> Result<RecommendedWatcher> {
    let (watch_tx, mut watch_rx) = mpsc::channel(16);

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            if event.kind.is_modify() || event.kind.is_create() {
                let _ = watch_tx.try_send(());
            }
        }
    })?;

    watcher.watch(&path, RecursiveMode::NonRecursive)?;

    let watch_path = path.clone();
    tokio::spawn(async move {
        while watch_rx.recv().await.is_some() {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            match load_config(&watch_path) {
                Ok(config) => {
                    if tx.send(config).await.is_err() {
                        tracing::warn!("配置更新接收端已关闭，停止监听");
                        break;
                    }
                    tracing::info!("配置已重新加载");
                }
                Err(e) => {
                    tracing::error!("重新加载配置失败: {}", e);
                }
            }
        }
    });

    Ok(watcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_load_basic_config() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
            [[service]]
            name = "test-service"
            url = "http://localhost:8080"
            check_type = "http"
            check_path = "/health"
            interval_secs = 30
            timeout_secs = 5
            unhealthy_threshold = 3
            degraded_threshold = 2
            healthy_threshold = 2
            dependencies = ["dep1", "dep2"]
            "#
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.services.len(), 1);
        assert_eq!(config.services[0].name, "test-service");
        assert_eq!(config.services[0].dependencies.len(), 2);
    }

    #[test]
    fn test_load_config_with_defaults() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
            [[service]]
            name = "test-service"
            url = "http://localhost:8080"
            check_type = "http"
            "#
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.services.len(), 1);
        assert_eq!(config.services[0].interval_secs, 30);
        assert_eq!(config.services[0].timeout_secs, 5);
        assert_eq!(config.services[0].unhealthy_threshold, 3);
        assert_eq!(config.services[0].degraded_threshold, 2);
        assert_eq!(config.services[0].healthy_threshold, 2);
    }

    #[test]
    fn test_load_tcp_check_config() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
            [[service]]
            name = "db-service"
            url = "localhost:5432"
            check_type = "tcp"
            "#
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.services.len(), 1);
        match config.services[0].check_type {
            crate::models::CheckType::Tcp => (),
            _ => panic!("expected tcp check type"),
        }
    }

    #[test]
    fn test_load_command_check_config() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
            [[service]]
            name = "custom-check"
            url = "ignored"
            check_type = "command"
            command = "/bin/check.sh"
            command_args = ["--verbose"]
            "#
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.services.len(), 1);
        assert_eq!(config.services[0].command.as_deref(), Some("/bin/check.sh"));
        assert_eq!(config.services[0].command_args, vec!["--verbose"]);
    }

    #[test]
    fn test_load_passive_service_config() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
            [[service]]
            name = "passive-service"
            url = "http://localhost"
            check_type = "http"
            passive = true
            heartbeat_timeout_secs = 120
            "#
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert!(config.services[0].passive);
        assert_eq!(config.services[0].heartbeat_timeout_secs, 120);
    }

    #[test]
    fn test_load_alert_config() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            r#"
            [alert]
            webhooks = [
                { url = "http://webhook1/critical", level = "critical" },
                { url = "http://webhook2/warn", level = "warn" },
            ]
            "#
            .as_bytes(),
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.alert.webhooks.len(), 2);
        assert_eq!(config.alert.webhooks[0].level, "critical");
        assert_eq!(config.alert.webhooks[1].url, "http://webhook2/warn");
    }

    #[test]
    fn test_load_server_config() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
            [server]
            host = "127.0.0.1"
            port = 9090
            "#
        )
        .unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 9090);
    }

    #[test]
    fn test_invalid_toml_returns_error() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "invalid toml [[[").unwrap();

        let result = load_config(file.path());
        assert!(result.is_err());
    }
}
