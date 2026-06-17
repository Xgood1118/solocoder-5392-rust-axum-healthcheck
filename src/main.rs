use healthcheck_service::alert::AlertManager;
use healthcheck_service::config::{load_config, watch_config, AppConfig};
use healthcheck_service::dependency::DependencyGraph;
use healthcheck_service::handlers::AppStateForHandlers;
use healthcheck_service::state::AppState;
use healthcheck_service::state::SharedState;

use axum::{
    routing::{get, post},
    Router,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_path = PathBuf::from(config_path);

    tracing::info!("加载配置文件: {}", config_path.display());
    let app_config = load_config(&config_path)?;

    let shared_state: SharedState = Arc::new(tokio::sync::RwLock::new(AppState::new()));
    let dependency_graph = Arc::new(tokio::sync::RwLock::new(DependencyGraph::new()));

    {
        let mut state = shared_state.write().await;
        let mut graph = dependency_graph.write().await;

        for svc_cfg in &app_config.services {
            state.add_service(svc_cfg.clone());
            graph.add_service(svc_cfg.name.clone(), svc_cfg.dependencies.clone());
        }
    }

    let (check_tx, mut check_rx) = mpsc::channel::<String>(100);
    let (config_tx, mut config_rx) = mpsc::channel::<AppConfig>(16);

    let _watcher = watch_config(config_path.clone(), config_tx).await?;

    let check_tx_clone = check_tx.clone();

    tokio::spawn(async move {
        for svc_cfg in &app_config.services {
            if !svc_cfg.passive {
                let tx = check_tx_clone.clone();
                let name = svc_cfg.name.clone();
                let interval = svc_cfg.interval_secs;

                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(tokio::time::Duration::from_secs(interval));
                    loop {
                        interval.tick().await;
                        if tx.send(name.clone()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    });

    let state_for_check = shared_state.clone();
    let graph_for_check = dependency_graph.clone();
    let alert_manager = Arc::new(tokio::sync::Mutex::new(AlertManager::new(
        app_config.alert.clone(),
    )));

    let alert_clone = alert_manager.clone();
    tokio::spawn(async move {
        while let Some(service_name) = check_rx.recv().await {
            let state = state_for_check.clone();
            let graph = graph_for_check.clone();
            let alert_mgr = alert_clone.clone();

            tokio::spawn(async move {
                let config = {
                    let state = state.read().await;
                    state.configs.get(&service_name).cloned()
                };

                if let Some(cfg) = config {
                    if cfg.passive {
                        let mut state = state.write().await;
                        let _ = state.check_heartbeat_timeout(&service_name);
                        return;
                    }

                    let result = healthcheck_service::checker::run_check(&cfg).await;

                    let status_changed = {
                        let mut state = state.write().await;
                        state.apply_check_result(&service_name, result.success, result.error)
                    };

                    if let Some(new_status) = status_changed {
                        let mut g = graph.write().await;
                        g.update_status(&service_name, new_status);
                    }

                    {
                        let state = state.read().await;
                        if let Some(svc_state) = state.services.get(&service_name) {
                            let mut alert = alert_mgr.lock().await;
                            alert
                                .check_and_send_alerts(
                                    &service_name,
                                    svc_state,
                                    cfg.alert_after_secs,
                                )
                                .await;
                        }
                    }
                }
            });
        }
    });

    let state_for_config = shared_state.clone();
    let graph_for_config = dependency_graph.clone();
    let alert_for_config = alert_manager.clone();

    tokio::spawn(async move {
        while let Some(new_config) = config_rx.recv().await {
            tracing::info!("配置变更，重新加载");

            let mut state = state_for_config.write().await;
            let mut graph = graph_for_config.write().await;

            let current_names: std::collections::HashSet<_> =
                state.services.keys().cloned().collect();
            let new_names: std::collections::HashSet<_> =
                new_config.services.iter().map(|s| s.name.clone()).collect();

            for name in current_names.difference(&new_names) {
                state.remove_service(name);
            }

            for svc_cfg in &new_config.services {
                if current_names.contains(&svc_cfg.name) {
                    state.update_service_config(svc_cfg.clone());
                } else {
                    state.add_service(svc_cfg.clone());
                }
                graph.add_service(svc_cfg.name.clone(), svc_cfg.dependencies.clone());
            }

            let alert = alert_for_config.lock().await;
            alert.update_config(new_config.alert.clone());
            drop(alert);
        }
    });

    let handler_state = AppStateForHandlers {
        state: shared_state.clone(),
        dependency_graph: dependency_graph.clone(),
        check_tx: check_tx.clone(),
    };

    let app = Router::new()
        .route("/health", get(healthcheck_service::handlers::health_check))
        .route("/api/services", get(healthcheck_service::handlers::list_services))
        .route("/api/services/:name", get(healthcheck_service::handlers::get_service))
        .route(
            "/api/services/:name/check",
            post(healthcheck_service::handlers::trigger_check),
        )
        .route(
            "/api/services/:name/history",
            get(healthcheck_service::handlers::get_history),
        )
        .route("/api/heartbeat", post(healthcheck_service::handlers::heartbeat))
        .route("/api/report", get(healthcheck_service::handlers::get_report))
        .route(
            "/api/dependencies",
            get(healthcheck_service::handlers::get_dependencies),
        )
        .with_state(handler_state)
        .fallback(healthcheck_service::handlers::not_found);

    let addr = format!(
        "{}:{}",
        app_config.server.host, app_config.server.port
    );
    tracing::info!("健康检查服务启动于 {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
