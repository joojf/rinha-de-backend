use axum::{extract::State, routing::{get, post}, Json, Router};
use chrono::Utc;

use crate::models::{IngestIn, SummaryOut, SummaryQuery, SummarySide};
use crate::redis_ops::{sum_range};
use crate::state::AppState;

pub fn build_public_router(state: AppState) -> Router {
    Router::new().route("/payments-summary", get(summary_handler)).with_state(state)
}

pub fn build_ingest_router(state: AppState, tx: tokio::sync::mpsc::Sender<crate::models::JobPayload>) -> Router {
    let txc = tx.clone();
    Router::new().route("/ingest", post(move |st, body| ingest_handler(st, body, txc.clone()))).with_state(state)
}

async fn ingest_handler(State(_app): State<AppState>, Json(input): Json<IngestIn>, tx: tokio::sync::mpsc::Sender<crate::models::JobPayload>) -> axum::response::Response {
    if !input.amount.is_finite() || input.amount <= 0.0 { return axum::response::Response::builder().status(400).body(axum::body::Body::empty()).unwrap(); }
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let job = crate::models::JobPayload { correlation_id: input.correlation_id, amount: input.amount, requested_at: now };
    let _ = tx.try_send(job);
    axum::response::Response::builder().status(202).body(axum::body::Body::empty()).unwrap()
}

async fn summary_handler(State(app): State<AppState>, axum::extract::Query(q): axum::extract::Query<SummaryQuery>) -> Json<SummaryOut> {
    let mut redis = app.redis.clone();
    if q.from.is_none() && q.to.is_none() {
        let (dc, da, fc, fa): (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = redis::pipe()
            .get("summary:default:total_count").get("summary:default:total_amount_cents")
            .get("summary:fallback:total_count").get("summary:fallback:total_amount_cents")
            .query_async(&mut redis).await.unwrap_or((None,None,None,None));
        return Json(SummaryOut {
            default: SummarySide { total_requests: dc.unwrap_or(0) as u64, total_amount: (da.unwrap_or(0) as f64)/100.0 },
            fallback: SummarySide { total_requests: fc.unwrap_or(0) as u64, total_amount: (fa.unwrap_or(0) as f64)/100.0 },
        });
    }
    let from = q.from.as_deref().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_millis()).unwrap_or(i64::MIN/2);
    let to = q.to.as_deref().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_millis()).unwrap_or(i64::MAX/2);
    if to < from { return Json(SummaryOut::default()); }
    let (dc, da) = sum_range(&mut redis, "default", from, to).await;
    let (fc, fa) = sum_range(&mut redis, "fallback", from, to).await;
    Json(SummaryOut { default: SummarySide { total_requests: dc, total_amount: da }, fallback: SummarySide { total_requests: fc, total_amount: fa } })
}


