use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::dependency::DependencyGraph;
use crate::models::HeartbeatRequest;
use crate::state::SharedState;

#[derive(Clone)]
pub struct AppStateForHandlers {
    pub state: SharedState,
    pub dependency_graph: std::sync::Arc<tokio::sync::RwLock<DependencyGraph>>,
    pub check_tx: tokio::sync::mpsc::Sender<String>,
}

pub fn problem(status: StatusCode, title: &str, detail: &str) -> impl IntoResponse {
    (
        status,
        Json(json!({
            "type": "about:blank",
            "title": title,
            "status": status.as_u16(),
            "detail": detail,
        })),
    )
}

pub async fn list_services(State(app_state): State<AppStateForHandlers>) -> impl IntoResponse {
    let state = app_state.state.read().await;

    let services: Vec<_> = state
        .services
        .values()
        .map(|s| {
            json!({
                "name": s.name,
                "status": s.status,
                "last_check": s.last_check,
                "last_success": s.last_success,
            })
        })
        .collect();

    Json(services)
}

pub async fn get_service(
    State(app_state): State<AppStateForHandlers>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let state = app_state.state.read().await;

    match state.services.get(&name) {
        Some(service) => {
            let config = state.configs.get(&name);
            let history = state.get_history(&name);

            let response = json!({
                "name": service.name,
                "status": service.status,
                "last_check": service.last_check,
                "last_success": service.last_success,
                "last_error": service.last_error,
                "consecutive_successes": service.consecutive_successes,
                "consecutive_failures": service.consecutive_failures,
                "status_since": service.status_since,
                "config": config,
                "history": history,
            });

            (StatusCode::OK, Json(response)).into_response()
        }
        None => problem(StatusCode::NOT_FOUND, "Not Found", &format!("服务 '{}' 不存在", name))
            .into_response(),
    }
}

pub async fn trigger_check(
    State(app_state): State<AppStateForHandlers>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let state = app_state.state.read().await;

    if !state.services.contains_key(&name) {
        return problem(StatusCode::NOT_FOUND, "Not Found", &format!("服务 '{}' 不存在", name))
            .into_response();
    }

    let tx = app_state.check_tx.clone();
    drop(state);

    match tx.send(name.clone()).await {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(json!({ "message": format!("已触发服务 '{}' 的检查", name) })),
        )
            .into_response(),
        Err(_) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service Unavailable",
            "检查任务通道已关闭",
        )
        .into_response(),
    }
}

pub async fn get_history(
    State(app_state): State<AppStateForHandlers>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let state = app_state.state.read().await;

    if !state.services.contains_key(&name) {
        return problem(StatusCode::NOT_FOUND, "Not Found", &format!("服务 '{}' 不存在", name))
            .into_response();
    }

    let history = state.get_history(&name);
    (StatusCode::OK, Json(json!({ "history": history }))).into_response()
}

pub async fn heartbeat(
    State(app_state): State<AppStateForHandlers>,
    Json(req): Json<HeartbeatRequest>,
) -> impl IntoResponse {
    let mut state = app_state.state.write().await;

    if !state.services.contains_key(&req.name) {
        return problem(
            StatusCode::NOT_FOUND,
            "Not Found",
            &format!("服务 '{}' 未注册", req.name),
        )
        .into_response();
    }

    let status_change = state.record_heartbeat(&req.name);

    let message = match status_change {
        Some(new_status) => format!("心跳已接收，状态更新为 {}", new_status),
        None => "心跳已接收".to_string(),
    };

    (
        StatusCode::OK,
        Json(json!({
            "message": message,
            "name": req.name,
            "status_changed": status_change.is_some(),
        })),
    )
        .into_response()
}

pub async fn get_report(State(app_state): State<AppStateForHandlers>) -> impl IntoResponse {
    let state = app_state.state.read().await;
    let summary = state.get_report_summary();

    Json(summary)
}

pub async fn get_dependencies(State(app_state): State<AppStateForHandlers>) -> impl IntoResponse {
    let graph = app_state.dependency_graph.read().await;
    let trees = graph.all_trees();
    let root_causes = graph.find_root_causes();

    Json(json!({
        "trees": trees,
        "root_causes": root_causes,
    }))
}

pub async fn health_check() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({ "status": "healthy", "service": "healthcheck-service" })),
    )
}

pub async fn not_found() -> impl IntoResponse {
    problem(StatusCode::NOT_FOUND, "Not Found", "请求的资源不存在")
}
