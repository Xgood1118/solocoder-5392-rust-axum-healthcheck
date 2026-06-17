use healthcheck_service::dependency::DependencyGraph;
use healthcheck_service::handlers::AppStateForHandlers;
use healthcheck_service::models::{CheckType, ServiceConfig, ServiceStatus};
use healthcheck_service::state::AppState;
use healthcheck_service::state::SharedState;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tower::ServiceExt;

fn make_test_config(name: &str) -> ServiceConfig {
    ServiceConfig {
        name: name.to_string(),
        url: "http://localhost".to_string(),
        check_type: CheckType::Http,
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

async fn setup_test_app() -> (Router, mpsc::Receiver<String>) {
    let shared_state: SharedState = Arc::new(RwLock::new(AppState::new()));
    let dependency_graph = Arc::new(RwLock::new(DependencyGraph::new()));

    {
        let mut state = shared_state.write().await;
        let mut graph = dependency_graph.write().await;

        let svc1 = make_test_config("service-a");
        let svc2 = make_test_config("service-b");
        let mut svc3 = make_test_config("service-c");
        svc3.passive = true;
        svc3.dependencies = vec!["service-a".to_string()];

        state.add_service(svc1.clone());
        state.add_service(svc2.clone());
        state.add_service(svc3.clone());

        graph.add_service("service-a".to_string(), vec![]);
        graph.add_service("service-b".to_string(), vec![]);
        graph.add_service("service-c".to_string(), vec!["service-a".to_string()]);

        state.apply_check_result("service-a", true, None);
        state.apply_check_result("service-b", true, None);
        state.apply_check_result("service-b", false, Some("connection refused".to_string()));
        state.apply_check_result("service-b", false, Some("connection refused".to_string()));

        graph.update_status("service-a", ServiceStatus::Healthy);
        graph.update_status("service-b", ServiceStatus::Degraded);
    }

    let (check_tx, check_rx) = mpsc::channel::<String>(100);

    let handler_state = AppStateForHandlers {
        state: shared_state.clone(),
        dependency_graph: dependency_graph.clone(),
        check_tx,
    };

    let app = Router::new()
        .route("/health", get(healthcheck_service::handlers::health_check))
        .route("/api/services", get(healthcheck_service::handlers::list_services))
        .route(
            "/api/services/:name",
            get(healthcheck_service::handlers::get_service),
        )
        .route(
            "/api/services/:name/check",
            post(healthcheck_service::handlers::trigger_check),
        )
        .route(
            "/api/services/:name/history",
            get(healthcheck_service::handlers::get_history),
        )
        .route(
            "/api/heartbeat",
            post(healthcheck_service::handlers::heartbeat),
        )
        .route("/api/report", get(healthcheck_service::handlers::get_report))
        .route(
            "/api/dependencies",
            get(healthcheck_service::handlers::get_dependencies),
        )
        .with_state(handler_state)
        .fallback(healthcheck_service::handlers::not_found);

    (app, check_rx)
}

async fn get_json(app: &Router, path: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    (status, json)
}

async fn post_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();

    let resp_body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&resp_body).unwrap();

    (status, json)
}

#[tokio::test]
async fn test_health_endpoint() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["service"], "healthcheck-service");
}

#[tokio::test]
async fn test_list_services() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/services").await;

    assert_eq!(status, StatusCode::OK);
    let services = body.as_array().unwrap();
    assert_eq!(services.len(), 3);

    let names: Vec<&str> = services
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"service-a"));
    assert!(names.contains(&"service-b"));
    assert!(names.contains(&"service-c"));
}

#[tokio::test]
async fn test_get_service_details() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/services/service-a").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "service-a");
    assert_eq!(body["status"], "healthy");
    assert!(body["last_check"].is_string());
    assert!(body["last_success"].is_string());
    assert!(body["history"].is_array());
}

#[tokio::test]
async fn test_get_service_not_found() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/services/nonexistent").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["title"], "Not Found");
    assert_eq!(body["status"], 404);
    assert!(body["detail"].as_str().unwrap().contains("不存在"));
}

#[tokio::test]
async fn test_report_summary() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/report").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    assert_eq!(body["healthy"], 1);
    assert_eq!(body["degraded"], 1);
    assert_eq!(body["unknown"], 1);
    assert_eq!(body["unhealthy"], 0);
}

#[tokio::test]
async fn test_heartbeat() {
    let (app, _rx) = setup_test_app().await;
    let payload = serde_json::json!({
        "name": "service-c"
    });

    let (status, body) = post_json(&app, "/api/heartbeat", payload).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "service-c");
    assert!(body["status_changed"].as_bool().unwrap());
}

#[tokio::test]
async fn test_heartbeat_service_not_found() {
    let (app, _rx) = setup_test_app().await;
    let payload = serde_json::json!({
        "name": "nonexistent-service"
    });

    let (status, body) = post_json(&app, "/api/heartbeat", payload).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["title"], "Not Found");
}

#[tokio::test]
async fn test_trigger_check() {
    let (app, mut rx) = setup_test_app().await;
    let (status, body) = post_json(&app, "/api/services/service-a/check", Value::Null).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["message"].as_str().unwrap().contains("触发"));

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg, "service-a");
}

#[tokio::test]
async fn test_trigger_check_not_found() {
    let (app, _rx) = setup_test_app().await;
    let (status, _body) = post_json(&app, "/api/services/nonexistent/check", Value::Null).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_history() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/services/service-b/history").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["history"].is_array());
    assert!(body["history"].as_array().unwrap().len() >= 1);
}

#[tokio::test]
async fn test_dependencies() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/dependencies").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["trees"].is_array());
    assert!(body["root_causes"].is_array());
}

#[tokio::test]
async fn test_not_found_fallback() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/nonexistent").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["title"], "Not Found");
    assert_eq!(body["status"], 404);
}

#[tokio::test]
async fn test_degraded_service_in_list() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/services").await;

    assert_eq!(status, StatusCode::OK);
    let services = body.as_array().unwrap();
    let svc_b = services
        .iter()
        .find(|s| s["name"] == "service-b")
        .unwrap();

    assert_eq!(svc_b["status"], "degraded");
}

#[tokio::test]
async fn test_problem_details_format() {
    let (app, _rx) = setup_test_app().await;
    let (status, body) = get_json(&app, "/api/services/unknown").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.get("type").is_some());
    assert!(body.get("title").is_some());
    assert!(body.get("status").is_some());
    assert!(body.get("detail").is_some());
}
