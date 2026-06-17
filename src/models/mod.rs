use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    Unknown,
    Healthy,
    Degraded,
    Unhealthy,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceStatus::Unknown => write!(f, "unknown"),
            ServiceStatus::Healthy => write!(f, "healthy"),
            ServiceStatus::Degraded => write!(f, "degraded"),
            ServiceStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckType {
    Http,
    Tcp,
    Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub name: String,
    pub url: String,
    pub check_type: CheckType,
    #[serde(default = "default_check_path")]
    pub check_path: String,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_degraded_threshold")]
    pub degraded_threshold: u32,
    #[serde(default = "default_healthy_threshold")]
    pub healthy_threshold: u32,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub passive: bool,
    #[serde(default = "default_heartbeat_timeout_secs")]
    pub heartbeat_timeout_secs: u64,
    #[serde(default = "default_alert_after_secs")]
    pub alert_after_secs: u64,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub command_args: Vec<String>,
}

fn default_check_path() -> String {
    "/health".to_string()
}

fn default_interval_secs() -> u64 {
    30
}

fn default_timeout_secs() -> u64 {
    5
}

fn default_unhealthy_threshold() -> u32 {
    3
}

fn default_degraded_threshold() -> u32 {
    2
}

fn default_healthy_threshold() -> u32 {
    2
}

fn default_heartbeat_timeout_secs() -> u64 {
    60
}

fn default_alert_after_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Serialize)]
pub struct StateHistoryEntry {
    pub from: ServiceStatus,
    pub to: ServiceStatus,
    pub timestamp: DateTime<Utc>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceState {
    pub name: String,
    pub status: ServiceStatus,
    pub last_check: Option<DateTime<Utc>>,
    pub last_success: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_successes: u32,
    pub consecutive_failures: u32,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub status_since: DateTime<Utc>,
}

impl ServiceState {
    pub fn new(name: String) -> Self {
        Self {
            name,
            status: ServiceStatus::Unknown,
            last_check: None,
            last_success: None,
            last_error: None,
            consecutive_successes: 0,
            consecutive_failures: 0,
            last_heartbeat: None,
            status_since: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportSummary {
    pub total: usize,
    pub healthy: usize,
    pub degraded: usize,
    pub unhealthy: usize,
    pub unknown: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub name: String,
    #[serde(default)]
    pub status: Option<ServiceStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencyNode {
    pub name: String,
    pub status: ServiceStatus,
    pub dependencies: Vec<DependencyNode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RootCauseResult {
    pub service: String,
    pub depth: usize,
    pub status: ServiceStatus,
}
