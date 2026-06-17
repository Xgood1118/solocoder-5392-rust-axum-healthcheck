use crate::config::AlertConfig;
use crate::models::{ServiceState, ServiceStatus};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
struct AlertMessage {
    service: String,
    status: ServiceStatus,
    event: String,
    timestamp: DateTime<Utc>,
    last_success: Option<DateTime<Utc>>,
    error: Option<String>,
}

pub struct AlertManager {
    config: Arc<RwLock<AlertConfig>>,
    last_alert_time: HashMap<String, DateTime<Utc>>,
    last_alert_status: HashMap<String, ServiceStatus>,
    alerting_services: HashSet<String>,
}

impl AlertManager {
    pub fn new(config: AlertConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            last_alert_time: HashMap::new(),
            last_alert_status: HashMap::new(),
            alerting_services: HashSet::new(),
        }
    }

    pub fn update_config(&self, config: AlertConfig) {
        let current = self.config.clone();
        tokio::spawn(async move {
            let mut guard = current.write().await;
            *guard = config;
        });
    }

    pub async fn check_and_send_alerts(
        &mut self,
        service_name: &str,
        state: &ServiceState,
        alert_after_secs: u64,
    ) {
        match state.status {
            ServiceStatus::Unhealthy => {
                let now = Utc::now();
                let duration = now - state.status_since;

                if duration.num_seconds() >= alert_after_secs as i64 {
                    let should_send = match self.last_alert_time.get(service_name) {
                        Some(last_time) => {
                            let cooldown = chrono::Duration::seconds(alert_after_secs as i64);
                            now - *last_time >= cooldown
                        }
                        None => true,
                    };

                    if should_send {
                        self.send_alert(service_name, state, "服务不可用").await;
                        self.last_alert_time
                            .insert(service_name.to_string(), now);
                        self.last_alert_status
                            .insert(service_name.to_string(), ServiceStatus::Unhealthy);
                        self.alerting_services.insert(service_name.to_string());
                    }
                }
            }
            ServiceStatus::Degraded => {
                if self.alerting_services.contains(service_name) {
                    let last_status = self
                        .last_alert_status
                        .get(service_name)
                        .copied()
                        .unwrap_or(ServiceStatus::Unknown);

                    if last_status == ServiceStatus::Unhealthy {
                        self.send_alert(service_name, state, "服务降级为亚健康").await;
                        self.last_alert_time
                            .insert(service_name.to_string(), Utc::now());
                        self.last_alert_status
                            .insert(service_name.to_string(), ServiceStatus::Degraded);
                    }
                }
            }
            ServiceStatus::Healthy => {
                if self.alerting_services.remove(service_name) {
                    self.send_alert(service_name, state, "服务已恢复").await;
                    self.last_alert_status
                        .insert(service_name.to_string(), ServiceStatus::Healthy);
                }
            }
            ServiceStatus::Unknown => {}
        }
    }

    async fn send_alert(&self, service_name: &str, state: &ServiceState, event: &str) {
        let message = AlertMessage {
            service: service_name.to_string(),
            status: state.status,
            event: event.to_string(),
            timestamp: Utc::now(),
            last_success: state.last_success,
            error: state.last_error.clone(),
        };

        let config = self.config.read().await;
        let webhooks = config.webhooks.clone();
        drop(config);

        if webhooks.is_empty() {
            tracing::info!(
                "告警: {} - {} (状态: {})",
                service_name,
                event,
                state.status
            );
            return;
        }

        let level = match state.status {
            ServiceStatus::Unhealthy => "critical",
            ServiceStatus::Degraded => "warn",
            _ => "info",
        };

        for webhook in &webhooks {
            if webhook.level == level || level == "critical" {
                let url = webhook.url.clone();
                let msg = message.clone();
                tokio::spawn(async move {
                    if let Err(e) = send_webhook(&url, &msg).await {
                        tracing::warn!("发送 webhook 告警失败 ({}): {}", url, e);
                    }
                });
            }
        }
    }
}

async fn send_webhook(url: &str, message: &AlertMessage) -> Result<(), reqwest::Error> {
    let client = reqwest::Client::new();
    client.post(url).json(message).send().await?;
    Ok(())
}
