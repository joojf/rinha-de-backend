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

    // check de idempotência via set nx ex
    let mut redis = state.redis.clone();
    let corr_key = format!("corr:{}", input.correlation_id);
    let set_res: bool = redis::cmd("SET")
        .arg(&corr_key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(state.idemp_ttl)
        .query_async(&mut redis)
        .await
        .map_err(anyhow::Error::from)?;
    if !set_res {
        return Ok((StatusCode::OK, Json(serde_json::json!({"s":"dup"}))));
    }

    // enfileira job para processamento assíncrono
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

    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"s":"ok"}))))
}

// GET /payments-summary
async fn handle_summary(
    State(state): State<AppState>,
    Query(q): Query<SummaryQuery>,
) -> Result<impl IntoResponse, AppError> {
    let mut redis = state.redis.clone();
    if q.from.is_none() && q.to.is_none() {
        // caminho rápido: lê ambos totais em pipeline único
        let mut pipe = redis::pipe();
        pipe.get(&format!("summary:default:total_count"))
            .get(&format!("summary:default:total_amount_cents"))
            .get(&format!("summary:fallback:total_count"))
            .get(&format!("summary:fallback:total_amount_cents"));
        
        let (def_count, def_amount, fb_count, fb_amount): (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = 
            pipe.query_async(&mut redis).await.map_err(anyhow::Error::from)?;
        
        let out = SummaryOut {
            default: SummarySide {
                total_requests: def_count.unwrap_or(0) as u64,
                total_amount: def_amount.unwrap_or(0) as f64 / 100.0,
            },
            fallback: SummarySide {
                total_requests: fb_count.unwrap_or(0) as u64,
                total_amount: fb_amount.unwrap_or(0) as f64 / 100.0,
            },
        };
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
