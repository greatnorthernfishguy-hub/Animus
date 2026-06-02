// src/adapters/http.rs
// GUI HTTP adapter — axum router serving GET /status, GET /history,
// GET /channels, POST /turn, POST /channels/:name/reconnect on ANIMUS_GUI_PORT.
//
// ---- Changelog ----
// [2026-05-31] Claude (Sonnet 4.6) — Anima GUI v1: HTTP adapter
// What: Axum HTTP server backing anima_gui.py's polling and /turn calls
// Why: GUI needs a local HTTP interface to Anima; CLI-only mode had no observable API
// How: 5 axum handlers share Arc<TurnPipeline> via State extractor
// -------------------

use crate::pipeline::{SourceType, TurnContext, TurnPipeline};
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use std::sync::Arc;

// ---- Request / Response types ----

#[derive(serde::Deserialize)]
struct TurnRequest {
    text: String,
    sender: String,
}

#[derive(serde::Serialize)]
struct TurnResponse {
    response: String,
}

#[derive(serde::Serialize)]
struct HistoryResponse {
    messages: Vec<serde_json::Value>,
}

#[derive(serde::Serialize)]
struct ChannelInfo {
    name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(serde::Serialize)]
struct ChannelsResponse {
    channels: Vec<ChannelInfo>,
}

#[derive(serde::Serialize)]
struct ReconnectResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// ---- Adapter ----

pub struct HttpAdapter {
    pipeline: Arc<TurnPipeline>,
    port: u16,
}

impl HttpAdapter {
    pub fn new(pipeline: Arc<TurnPipeline>, port: u16) -> Self {
        Self { pipeline, port }
    }

    pub async fn run(self: Arc<Self>) {
        let pipeline = Arc::clone(&self.pipeline);
        let app = Router::new()
            .route("/turn", post(handle_turn))
            .route("/status", get(handle_status))
            .route("/history", get(handle_history))
            .route("/channels", get(handle_channels))
            .route("/channels/:name/reconnect", post(handle_reconnect))
            .with_state(pipeline);

        let addr = format!("127.0.0.1:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("GUI HTTP server failed to bind");
        tracing::info!("Anima GUI HTTP server listening on {}", addr);
        axum::serve(listener, app)
            .await
            .expect("GUI HTTP server crashed");
    }
}

// ---- Handlers ----

async fn handle_turn(
    State(pipeline): State<Arc<TurnPipeline>>,
    Json(body): Json<TurnRequest>,
) -> Json<TurnResponse> {
    let ctx = TurnContext {
        text: body.text,
        channel_id: "gui".to_string(),
        user_id: body.sender,
        source: SourceType::Channel,
    };
    let response = pipeline.run(ctx).await;
    Json(TurnResponse { response })
}

async fn handle_status(State(pipeline): State<Arc<TurnPipeline>>) -> Json<serde_json::Value> {
    let status = pipeline.status.lock().unwrap().clone();
    Json(serde_json::json!({
        "stage": status.stage,
        "stage_state": status.stage_state,
        "last_tg_verdict": status.last_tg_verdict,
        "last_after_turn": status.last_after_turn,
        "anima_alive": true,
    }))
}

async fn handle_history(
    State(pipeline): State<Arc<TurnPipeline>>,
) -> Json<HistoryResponse> {
    Json(HistoryResponse {
        messages: pipeline.history_snapshot(),
    })
}

async fn handle_channels(
    State(_pipeline): State<Arc<TurnPipeline>>,
) -> Json<ChannelsResponse> {
    // v1: only the GUI HTTP channel exists
    Json(ChannelsResponse {
        channels: vec![ChannelInfo {
            name: "gui".to_string(),
            status: "connected".to_string(),
            error: None,
        }],
    })
}

async fn handle_reconnect(
    State(_pipeline): State<Arc<TurnPipeline>>,
    Path(name): Path<String>,
) -> Json<ReconnectResponse> {
    if name == "gui" {
        Json(ReconnectResponse { ok: true, error: None })
    } else {
        Json(ReconnectResponse {
            ok: false,
            error: Some(format!("channel '{}' not implemented in v1", name)),
        })
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_runner::AgentRunner;
    use crate::context_builder::ContextBuilder;
    use crate::tool_dispatcher::ToolDispatcher;
    use crate::tract_writer::TractWriter;
    use crate::trollguard::TrollGuardBridge;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn make_test_pipeline() -> Arc<TurnPipeline> {
        let tg = Arc::new(TrollGuardBridge::new("http://127.0.0.1:1"));
        let cb = Arc::new(ContextBuilder::new("http://127.0.0.1:1".to_string()));
        let tract = Arc::new(TractWriter::new("/tmp/test_animus_http.tract"));
        let dispatcher = Arc::new(ToolDispatcher::from_env());
        let runner = Arc::new(AgentRunner::new(
            dispatcher,
            "http://127.0.0.1:1".to_string(),
            8,
        ));
        Arc::new(TurnPipeline::new(
            tg,
            cb,
            tract,
            runner,
            "http://127.0.0.1:1".to_string(),
        ))
    }

    fn build_test_router(pipeline: Arc<TurnPipeline>) -> Router {
        Router::new()
            .route("/turn", post(handle_turn))
            .route("/status", get(handle_status))
            .route("/history", get(handle_history))
            .route("/channels", get(handle_channels))
            .route("/channels/:name/reconnect", post(handle_reconnect))
            .with_state(pipeline)
    }

    #[tokio::test]
    async fn status_returns_idle() {
        let app = build_test_router(make_test_pipeline());
        let response = app
            .oneshot(Request::builder().uri("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["stage"], "IDLE");
        assert_eq!(json["anima_alive"], true);
    }

    #[tokio::test]
    async fn history_initially_empty() {
        let app = build_test_router(make_test_pipeline());
        let response = app
            .oneshot(Request::builder().uri("/history").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["messages"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn channels_returns_gui() {
        let app = build_test_router(make_test_pipeline());
        let response = app
            .oneshot(Request::builder().uri("/channels").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let channels = json["channels"].as_array().unwrap();
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0]["name"], "gui");
        assert_eq!(channels[0]["status"], "connected");
    }

    #[tokio::test]
    async fn reconnect_unknown_channel_returns_error() {
        let app = build_test_router(make_test_pipeline());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/channels/whatsapp/reconnect")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], false);
        assert!(json["error"].as_str().unwrap().contains("not implemented"));
    }
}
