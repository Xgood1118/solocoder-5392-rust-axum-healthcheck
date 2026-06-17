use crate::models::{ServiceConfig, ServiceState, ServiceStatus, StateHistoryEntry};
use chrono::Utc;
use ringbuf::{HeapRb, Rb};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const HISTORY_CAPACITY: usize = 100;

pub type SharedState = Arc<RwLock<AppState>>;

pub struct AppState {
    pub services: HashMap<String, ServiceState>,
    pub configs: HashMap<String, ServiceConfig>,
    pub history: HashMap<String, HeapRb<StateHistoryEntry>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
            configs: HashMap::new(),
            history: HashMap::new(),
        }
    }

    pub fn add_service(&mut self, config: ServiceConfig) {
        let name = config.name.clone();
        self.services
            .insert(name.clone(), ServiceState::new(name.clone()));
        self.configs.insert(name.clone(), config);
        self.history
            .insert(name, HeapRb::new(HISTORY_CAPACITY));
    }

    pub fn remove_service(&mut self, name: &str) {
        self.services.remove(name);
        self.configs.remove(name);
        self.history.remove(name);
    }

    pub fn update_service_config(&mut self, config: ServiceConfig) {
        let name = config.name.clone();
        self.configs.insert(name, config);
    }

    pub fn apply_check_result(
        &mut self,
        service_name: &str,
        success: bool,
        error: Option<String>,
    ) -> Option<ServiceStatus> {
        let config = self.configs.get(service_name)?.clone();
        let state = self.services.get_mut(service_name)?;

        let now = Utc::now();
        state.last_check = Some(now);

        if success {
            state.last_success = Some(now);
            state.last_error = None;
            state.consecutive_successes += 1;
            state.consecutive_failures = 0;
        } else {
            state.last_error = error.clone();
            state.consecutive_failures += 1;
            state.consecutive_successes = 0;
        }

        let old_status = state.status;
        let new_status = next_status(state.status, success, state, &config);

        if old_status != new_status {
            state.status = new_status;
            state.status_since = now;

            let reason = if success {
                format!(
                    "连续 {} 次检查成功",
                    state.consecutive_successes
                )
            } else {
                format!(
                    "连续 {} 次检查失败: {}",
                    state.consecutive_failures,
                    error.as_deref().unwrap_or("未知错误")
                )
            };

            if let Some(history) = self.history.get_mut(service_name) {
                history.push_overwrite(StateHistoryEntry {
                    from: old_status,
                    to: new_status,
                    timestamp: now,
                    reason,
                });
            }

            Some(new_status)
        } else {
            None
        }
    }

    pub fn record_heartbeat(&mut self, service_name: &str) -> Option<ServiceStatus> {
        let config = self.configs.get(service_name)?.clone();
        let state = self.services.get_mut(service_name)?;

        let now = Utc::now();
        state.last_heartbeat = Some(now);
        state.last_check = Some(now);
        state.last_success = Some(now);
        state.last_error = None;
        state.consecutive_successes += 1;
        state.consecutive_failures = 0;

        let old_status = state.status;
        let new_status = next_status(state.status, true, state, &config);

        if old_status != new_status {
            state.status = new_status;
            state.status_since = now;

            if let Some(history) = self.history.get_mut(service_name) {
                history.push_overwrite(StateHistoryEntry {
                    from: old_status,
                    to: new_status,
                    timestamp: now,
                    reason: "收到心跳".to_string(),
                });
            }

            Some(new_status)
        } else {
            None
        }
    }

    pub fn check_heartbeat_timeout(&mut self, service_name: &str) -> Option<ServiceStatus> {
        let config = self.configs.get(service_name)?.clone();
        let state = self.services.get(service_name)?;

        if !config.passive {
            return None;
        }

        let now = Utc::now();
        let timeout = chrono::Duration::seconds(config.heartbeat_timeout_secs as i64);

        let timed_out = match state.last_heartbeat {
            Some(last) => now - last > timeout,
            None => true,
        };

        if timed_out {
            self.apply_check_result(service_name, false, Some("心跳超时".to_string()))
        } else {
            None
        }
    }

    pub fn get_history(&self, service_name: &str) -> Vec<StateHistoryEntry> {
        self.history
            .get(service_name)
            .map(|rb| rb.iter().rev().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_report_summary(&self) -> crate::models::ReportSummary {
        let mut summary = crate::models::ReportSummary {
            total: self.services.len(),
            healthy: 0,
            degraded: 0,
            unhealthy: 0,
            unknown: 0,
        };

        for state in self.services.values() {
            match state.status {
                ServiceStatus::Healthy => summary.healthy += 1,
                ServiceStatus::Degraded => summary.degraded += 1,
                ServiceStatus::Unhealthy => summary.unhealthy += 1,
                ServiceStatus::Unknown => summary.unknown += 1,
            }
        }

        summary
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

fn next_status(
    current: ServiceStatus,
    success: bool,
    state: &ServiceState,
    config: &ServiceConfig,
) -> ServiceStatus {
    match current {
        ServiceStatus::Unknown => {
            if success {
                ServiceStatus::Healthy
            } else {
                ServiceStatus::Unhealthy
            }
        }
        ServiceStatus::Healthy => {
            if success {
                ServiceStatus::Healthy
            } else if state.consecutive_failures >= config.degraded_threshold {
                ServiceStatus::Degraded
            } else {
                ServiceStatus::Healthy
            }
        }
        ServiceStatus::Degraded => {
            if success {
                if state.consecutive_successes >= config.healthy_threshold {
                    ServiceStatus::Healthy
                } else {
                    ServiceStatus::Degraded
                }
            } else if state.consecutive_failures >= config.unhealthy_threshold {
                ServiceStatus::Unhealthy
            } else {
                ServiceStatus::Degraded
            }
        }
        ServiceStatus::Unhealthy => {
            if success {
                if state.consecutive_successes >= config.healthy_threshold {
                    ServiceStatus::Degraded
                } else {
                    ServiceStatus::Unhealthy
                }
            } else {
                ServiceStatus::Unhealthy
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(name: &str) -> ServiceConfig {
        ServiceConfig {
            name: name.to_string(),
            url: "http://localhost".to_string(),
            check_type: crate::models::CheckType::Http,
            check_path: "/health".to_string(),
            interval_secs: 30,
            timeout_secs: 5,
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

    #[test]
    fn test_initial_status_is_unknown() {
        let state = ServiceState::new("test".to_string());
        assert_eq!(state.status, ServiceStatus::Unknown);
    }

    #[test]
    fn test_unknown_to_healthy_on_first_success() {
        let mut app = AppState::new();
        app.add_service(make_config("svc"));
        let result = app.apply_check_result("svc", true, None);
        assert_eq!(result, Some(ServiceStatus::Healthy));
    }

    #[test]
    fn test_unknown_to_unhealthy_on_first_failure() {
        let mut app = AppState::new();
        app.add_service(make_config("svc"));
        let result = app.apply_check_result("svc", false, Some("err".to_string()));
        assert_eq!(result, Some(ServiceStatus::Unhealthy));
    }

    #[test]
    fn test_healthy_to_degraded_after_threshold_failures() {
        let mut app = AppState::new();
        let mut cfg = make_config("svc");
        cfg.degraded_threshold = 2;
        app.add_service(cfg);

        app.apply_check_result("svc", true, None);

        let result = app.apply_check_result("svc", false, Some("err".to_string()));
        assert_eq!(result, None, "第1次失败不应降级");

        let result = app.apply_check_result("svc", false, Some("err".to_string()));
        assert_eq!(result, Some(ServiceStatus::Degraded), "第2次失败应降级到亚健康");
    }

    #[test]
    fn test_degraded_to_unhealthy_after_threshold_failures() {
        let mut app = AppState::new();
        let mut cfg = make_config("svc");
        cfg.degraded_threshold = 2;
        cfg.unhealthy_threshold = 3;
        app.add_service(cfg);

        app.apply_check_result("svc", true, None);
        app.apply_check_result("svc", false, Some("err".to_string()));
        app.apply_check_result("svc", false, Some("err".to_string()));

        let state = app.services.get("svc").unwrap();
        assert_eq!(state.status, ServiceStatus::Degraded);
        assert_eq!(state.consecutive_failures, 2);

        let result = app.apply_check_result("svc", false, Some("err".to_string()));
        assert_eq!(result, Some(ServiceStatus::Unhealthy), "第3次失败应降级到不可用");
    }

    #[test]
    fn test_unhealthy_to_degraded_after_threshold_successes() {
        let mut app = AppState::new();
        let mut cfg = make_config("svc");
        cfg.healthy_threshold = 2;
        app.add_service(cfg);

        app.apply_check_result("svc", false, Some("err".to_string()));
        assert_eq!(app.services.get("svc").unwrap().status, ServiceStatus::Unhealthy);

        let result = app.apply_check_result("svc", true, None);
        assert_eq!(result, None, "第1次成功不应升级");

        let result = app.apply_check_result("svc", true, None);
        assert_eq!(result, Some(ServiceStatus::Degraded), "第2次成功应升级到亚健康");
    }

    #[test]
    fn test_degraded_to_healthy_after_threshold_successes() {
        let mut app = AppState::new();
        let mut cfg = make_config("svc");
        cfg.degraded_threshold = 1;
        cfg.healthy_threshold = 2;
        app.add_service(cfg);

        app.apply_check_result("svc", true, None);
        app.apply_check_result("svc", false, Some("err".to_string()));
        assert_eq!(app.services.get("svc").unwrap().status, ServiceStatus::Degraded);

        let result = app.apply_check_result("svc", true, None);
        assert_eq!(result, None, "第1次成功不应升级");

        let result = app.apply_check_result("svc", true, None);
        assert_eq!(result, Some(ServiceStatus::Healthy), "第2次成功应升级到健康");
    }

    #[test]
    fn test_success_resets_failure_counter() {
        let mut app = AppState::new();
        let mut cfg = make_config("svc");
        cfg.degraded_threshold = 3;
        app.add_service(cfg);

        app.apply_check_result("svc", true, None);
        app.apply_check_result("svc", false, Some("err".to_string()));
        app.apply_check_result("svc", false, Some("err".to_string()));

        let state = app.services.get("svc").unwrap();
        assert_eq!(state.consecutive_failures, 2);

        app.apply_check_result("svc", true, None);

        let state = app.services.get("svc").unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.consecutive_successes, 1);
    }

    #[test]
    fn test_history_is_recorded() {
        let mut app = AppState::new();
        app.add_service(make_config("svc"));

        app.apply_check_result("svc", true, None);
        app.apply_check_result("svc", false, Some("err".to_string()));
        app.apply_check_result("svc", false, Some("err".to_string()));

        let history = app.get_history("svc");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].to, ServiceStatus::Degraded);
        assert_eq!(history[1].to, ServiceStatus::Healthy);
    }

    #[test]
    fn test_report_summary() {
        let mut app = AppState::new();

        let cfg1 = make_config("svc1");
        app.add_service(cfg1);
        app.apply_check_result("svc1", true, None);

        let cfg2 = make_config("svc2");
        app.add_service(cfg2);
        app.apply_check_result("svc2", false, Some("err".to_string()));
        app.apply_check_result("svc2", true, None);
        app.apply_check_result("svc2", true, None);

        let cfg3 = make_config("svc3");
        app.add_service(cfg3);

        let summary = app.get_report_summary();
        assert_eq!(summary.total, 3);
        assert_eq!(summary.healthy, 1);
        assert_eq!(summary.degraded, 1);
        assert_eq!(summary.unknown, 1);
    }
}
