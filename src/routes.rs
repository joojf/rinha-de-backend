use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use crate::models::*;
use crate::redis_ops::*;
use crate::state::{AppError, AppState};
use crate::timeutil::{format_rfc3339_millis, parse_rfc3339};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/payments", post(handle_payment))
        .route("/payments-summary", get(handle_summary))
        .with_state(state)
}

// POST /payments
async fn handle_payment(
    State(state): State<AppState>,
    Json(input): Json<PaymentIn>,
) -> Result<impl IntoResponse, AppError> {
    if !input.amount.is_finite() || input.amount <= 0.0 {
        return Err(AppError::InvalidAmount);
    }

    let now = Utc::now();
    let now_str = format_rfc3339_millis(now);

    // idempotência via SET NX EX
    let mut redis = state.redis.clone();
    let corr_key = format!("corr:{}", input.correlation_id);
    let set_res: Option<String> = redis::cmd("SET")
        .arg(&corr_key)
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(state.idemp_ttl)
        .query_async(&mut redis)
        .await
        .map_err(anyhow::Error::from)?;
    if set_res.is_none() {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({"status":"duplicate"})),
        ));
    }

    // enfileira job e responde 202
    let job = JobPayload {
        correlation_id: input.correlation_id,
        amount: input.amount,
        requested_at: now_str.clone(),
        attempts: 0,
        proc_name: None,
    };
    let job_json = serde_json::to_string(&job).map_err(anyhow::Error::from)?;
    let _: () = redis
        .lpush(&state.queue_key, job_json)
        .await
        .map_err(anyhow::Error::from)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"status":"queued"})),
    ))
}

// GET /payments-summary
async fn handle_summary(
    State(state): State<AppState>,
    Query(q): Query<SummaryQuery>,
) -> Result<impl IntoResponse, AppError> {
    let mut redis = state.redis.clone();
    if q.from.is_none() && q.to.is_none() {
        let default = read_totals(&mut redis, "default").await?;
        let fallback = read_totals(&mut redis, "fallback").await?;
        let out = SummaryOut { default, fallback };
        return Ok((StatusCode::OK, Json(out)));
    }

    let from = q
        .from
        .as_deref()
        .and_then(parse_rfc3339)
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let to =
        q.to.as_deref()
            .and_then(parse_rfc3339)
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
    let (from_ms, to_ms) = (from.timestamp_millis(), to.timestamp_millis());
    if to_ms < from_ms {
        return Ok((StatusCode::OK, Json(SummaryOut::default())));
    }
    let default = sum_range(&mut redis, "default", from_ms, to_ms).await?;
    let fallback = sum_range(&mut redis, "fallback", from_ms, to_ms).await?;

    Ok((StatusCode::OK, Json(SummaryOut { default, fallback })))
}
