//! The admin HTTP surface — liveness, readiness, and Prometheus metrics — served
//! on a **separate** port from the S3 endpoint (no SigV4, no S3 path collision).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use soma_meta::{MetadataStore, NodeState, Quota, RateLimit};

/// Shared state for the admin router.
#[derive(Clone)]
pub struct AdminState {
    /// Handle used to render the Prometheus exposition.
    pub metrics: PrometheusHandle,
    /// Set to `true` once the node is ready to serve.
    pub ready: Arc<AtomicBool>,
    /// Metadata handle for cluster ops (drain) and per-bucket QoS, when this role
    /// has one (gateway / standalone).
    pub meta: Option<Arc<dyn MetadataStore>>,
}

/// Build the admin router.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        // Mark a node Draining (its data migrates off before removal) / undo.
        .route("/admin/drain", post(drain))
        .route("/admin/undrain", post(undrain))
        // Per-bucket QoS: set/clear quota and rate limit, read current config+usage.
        .route("/admin/quota", get(get_quota).put(put_quota))
        .route("/admin/ratelimit", put(put_ratelimit))
        .with_state(state)
}

/// Liveness: the process is up.
async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

/// Readiness: the node has opened its stores and is serving.
async fn readyz(State(state): State<AdminState>) -> Response {
    if state.ready.load(Ordering::Relaxed) {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

/// `POST /admin/drain?node=<id>`: gracefully decommission a node — mark it
/// `Draining` so the rebalance controller migrates its data off before it is
/// removed.
async fn drain(
    State(state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    set_state(&state, q.get("node"), NodeState::Draining, "draining").await
}

/// `POST /admin/undrain?node=<id>`: cancel a drain (mark the node `Active` again).
async fn undrain(
    State(state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    set_state(&state, q.get("node"), NodeState::Active, "active").await
}

/// Shared body for drain/undrain.
async fn set_state(
    state: &AdminState,
    node: Option<&String>,
    new_state: NodeState,
    label: &str,
) -> Response {
    let Some(meta) = state.meta.clone() else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "cluster ops not available on this role\n",
        )
            .into_response();
    };
    let Some(node) = node.cloned() else {
        return (StatusCode::BAD_REQUEST, "missing ?node=<id>\n").into_response();
    };
    let res = tokio::task::spawn_blocking(move || meta.set_node_state(&node, new_state)).await;
    match res {
        Ok(Ok(())) => (StatusCode::OK, format!("node marked {label}\n")).into_response(),
        Ok(Err(e)) => (StatusCode::NOT_FOUND, format!("{e}\n")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response(),
    }
}

/// `PUT /admin/quota?bucket=<name>&max_bytes=<size>&max_objects=<n>`: set a
/// bucket's storage quota (MinIO `mc quota set` style). `max_bytes` accepts a
/// human-readable size (`"10GiB"`) or a raw byte count; omit or `0` for unlimited
/// in either dimension. Setting both to zero clears the quota.
async fn put_quota(
    State(state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(meta) = state.meta.clone() else {
        return qos_unavailable();
    };
    let Some(bucket) = q.get("bucket").cloned() else {
        return (StatusCode::BAD_REQUEST, "missing ?bucket=<name>\n").into_response();
    };
    let max_bytes = match parse_size_param(q.get("max_bytes")) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let max_objects = match parse_u64_param(q.get("max_objects")) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let quota = Quota {
        max_bytes,
        max_objects,
    };
    let res = tokio::task::spawn_blocking(move || meta.set_bucket_quota(&bucket, quota)).await;
    blocking_result(res, "quota updated")
}

/// `PUT /admin/ratelimit?bucket=<name>&rps=<f>&burst=<f>`: set a bucket's request
/// rate limit. `rps` of `0` (or omitted) clears the limit; `burst` defaults to
/// `rps` when omitted.
async fn put_ratelimit(
    State(state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(meta) = state.meta.clone() else {
        return qos_unavailable();
    };
    let Some(bucket) = q.get("bucket").cloned() else {
        return (StatusCode::BAD_REQUEST, "missing ?bucket=<name>\n").into_response();
    };
    let rps = match parse_f64_param(q.get("rps")) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let burst = match parse_f64_param(q.get("burst")) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let limit = RateLimit { rps, burst };
    let res = tokio::task::spawn_blocking(move || meta.set_bucket_rate_limit(&bucket, limit)).await;
    blocking_result(res, "rate limit updated")
}

/// `GET /admin/quota?bucket=<name>`: report a bucket's configured quota + rate
/// limit and its current live usage, as a small JSON object.
async fn get_quota(
    State(state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(meta) = state.meta.clone() else {
        return qos_unavailable();
    };
    let Some(bucket) = q.get("bucket").cloned() else {
        return (StatusCode::BAD_REQUEST, "missing ?bucket=<name>\n").into_response();
    };
    let res = tokio::task::spawn_blocking(move || {
        let b = meta
            .get_bucket(&bucket)?
            .ok_or_else(|| soma_meta::Error::NoSuchBucket(bucket.clone()))?;
        let usage = meta.bucket_usage(&bucket)?;
        Ok::<_, soma_meta::Error>((b.quota, b.rate_limit, usage))
    })
    .await;
    match res {
        Ok(Ok((quota, rate, usage))) => {
            let body = format!(
                "{{\"max_bytes\":{},\"max_objects\":{},\"rps\":{},\"burst\":{},\
                 \"used_bytes\":{},\"used_objects\":{}}}\n",
                quota.max_bytes,
                quota.max_objects,
                rate.rps,
                rate.burst,
                usage.bytes,
                usage.objects
            );
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
        Ok(Err(e)) => (StatusCode::NOT_FOUND, format!("{e}\n")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response(),
    }
}

/// Reply for QoS endpoints when this role has no metadata handle.
fn qos_unavailable() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        "per-bucket QoS not available on this role\n",
    )
        .into_response()
}

/// Map a `spawn_blocking` metadata result to an HTTP response.
fn blocking_result(
    res: Result<Result<(), soma_meta::Error>, tokio::task::JoinError>,
    ok_msg: &str,
) -> Response {
    match res {
        Ok(Ok(())) => (StatusCode::OK, format!("{ok_msg}\n")).into_response(),
        Ok(Err(e)) => (StatusCode::NOT_FOUND, format!("{e}\n")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response(),
    }
}

/// Parse an optional human-readable size (`"10GiB"`) or raw byte count; absent/empty
/// → `0` (unlimited).
fn parse_size_param(v: Option<&String>) -> Result<u64, String> {
    match v.map(String::as_str).filter(|s| !s.is_empty()) {
        None => Ok(0),
        Some(s) => s
            .parse::<bytesize::ByteSize>()
            .map(|b| b.as_u64())
            .map_err(|e| format!("invalid max_bytes '{s}': {e}\n")),
    }
}

/// Parse an optional `u64` parameter; absent/empty → `0`.
fn parse_u64_param(v: Option<&String>) -> Result<u64, String> {
    match v.map(String::as_str).filter(|s| !s.is_empty()) {
        None => Ok(0),
        Some(s) => s
            .parse::<u64>()
            .map_err(|e| format!("invalid integer '{s}': {e}\n")),
    }
}

/// Parse an optional non-negative `f64` parameter; absent/empty → `0.0`.
fn parse_f64_param(v: Option<&String>) -> Result<f64, String> {
    match v.map(String::as_str).filter(|s| !s.is_empty()) {
        None => Ok(0.0),
        Some(s) => match s.parse::<f64>() {
            Ok(f) if f >= 0.0 && f.is_finite() => Ok(f),
            _ => Err(format!("invalid rate '{s}': must be a non-negative number\n")),
        },
    }
}

/// Prometheus exposition.
async fn metrics(State(state): State<AdminState>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use tower::ServiceExt;

    fn state(ready: bool) -> AdminState {
        // A local, non-installed recorder handle is enough to render.
        let handle = PrometheusBuilder::new().build_recorder().handle();
        AdminState {
            metrics: handle,
            ready: Arc::new(AtomicBool::new(ready)),
            meta: None,
        }
    }

    async fn get(app: &Router, path: &str) -> (StatusCode, String) {
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn healthz_is_ok() {
        let app = router(state(false));
        let (status, body) = get(&app, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("ok"));
    }

    #[tokio::test]
    async fn readyz_reflects_flag() {
        let (status, _) = get(&router(state(false)), "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let (status, _) = get(&router(state(true)), "/readyz").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint_responds() {
        let app = router(state(true));
        let (status, _body) = get(&app, "/metrics").await;
        // An empty registry renders an empty body; the endpoint still answers 200.
        assert_eq!(status, StatusCode::OK);
    }

    async fn post(app: &Router, path: &str) -> StatusCode {
        let req = Request::builder()
            .method("POST")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        app.clone().oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn drain_without_meta_is_not_implemented() {
        // The state helper has meta = None (as a non-gateway role would).
        let app = router(state(true));
        assert_eq!(
            post(&app, "/admin/drain?node=node-0").await,
            StatusCode::NOT_IMPLEMENTED
        );
    }
}
