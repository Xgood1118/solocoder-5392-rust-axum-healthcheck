use crate::models::{CheckType, ServiceConfig};
use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub success: bool,
    pub error: Option<String>,
}

pub async fn run_check(config: &ServiceConfig) -> CheckResult {
    let timeout = Duration::from_secs(config.timeout_secs);

    let result = tokio::time::timeout(timeout, do_check(config)).await;

    match result {
        Ok(Ok(())) => CheckResult {
            success: true,
            error: None,
        },
        Ok(Err(e)) => CheckResult {
            success: false,
            error: Some(e.to_string()),
        },
        Err(_) => CheckResult {
            success: false,
            error: Some(format!("检查超时 ({}秒)", config.timeout_secs)),
        },
    }
}

async fn do_check(config: &ServiceConfig) -> Result<()> {
    match config.check_type {
        CheckType::Http => check_http(config).await,
        CheckType::Tcp => check_tcp(config).await,
        CheckType::Command => check_command(config).await,
    }
}

async fn check_http(config: &ServiceConfig) -> Result<()> {
    let base_url = config.url.trim_end_matches('/');
    let path = if config.check_path.starts_with('/') {
        &config.check_path
    } else {
        config.check_path.as_str()
    };
    let url = format!("{}{}", base_url, path);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()?;

    let response = client.get(&url).send().await.map_err(|e| {
        anyhow!("HTTP 请求失败: {}", e)
    })?;

    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(anyhow!("HTTP 状态码: {}", status))
    }
}

async fn check_tcp(config: &ServiceConfig) -> Result<()> {
    let addr = &config.url;
    TcpStream::connect(addr).await.map_err(|e| {
        anyhow!("TCP 连接失败: {}", e)
    })?;
    Ok(())
}

async fn check_command(config: &ServiceConfig) -> Result<()> {
    let cmd = config
        .command
        .as_deref()
        .ok_or_else(|| anyhow!("未配置命令"))?;

    let mut command = Command::new(cmd);
    for arg in &config.command_args {
        command.arg(arg);
    }

    let output = command.output().await.map_err(|e| {
        anyhow!("执行命令失败: {}", e)
    })?;

    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!(
            "命令退出码: {}, stderr: {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CheckType;

    fn make_config(check_type: CheckType) -> ServiceConfig {
        ServiceConfig {
            name: "test".to_string(),
            url: "http://localhost:9999".to_string(),
            check_type,
            check_path: "/health".to_string(),
            interval_secs: 30,
            timeout_secs: 2,
            unhealthy_threshold: 3,
            degraded_threshold: 2,
            healthy_threshold: 2,
            dependencies: vec![],
            passive: false,
            heartbeat_timeout_secs: 60,
            alert_after_secs: 60,
            command: None,
            command_args: vec![],
        }
    }

    #[tokio::test]
    async fn test_http_check_timeout() {
        let mut cfg = make_config(CheckType::Http);
        cfg.url = "http://10.255.255.1:9999".to_string();
        cfg.timeout_secs = 1;

        let result = run_check(&cfg).await;
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn test_tcp_check_connection_refused() {
        let mut cfg = make_config(CheckType::Tcp);
        cfg.url = "127.0.0.1:1".to_string();
        cfg.timeout_secs = 1;

        let result = run_check(&cfg).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_command_check_missing_command() {
        let mut cfg = make_config(CheckType::Command);
        cfg.command = None;

        let result = run_check(&cfg).await;
        assert!(!result.success);
        assert!(result.error.unwrap().contains("未配置命令"));
    }

    #[tokio::test]
    async fn test_command_check_failure() {
        let mut cfg = make_config(CheckType::Command);
        cfg.command = Some("cmd.exe".to_string());
        cfg.command_args = vec!["/c".to_string(), "exit 1".to_string()];

        let result = run_check(&cfg).await;
        assert!(!result.success);
    }
}
