use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use tokio::time::Instant;
use redis::AsyncCommands;

use crate::models::*;
use crate::redis_ops::*;
use crate::state::{AppError, AppState};
use crate::timeutil::{format_rfc3339_millis, parse_rfc3339};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/payments", post(handle_payment))
        .route("/payments-summary", get(handle_summary))
        .route("/admin/purge-payments", post(handle_admin_purge))
        .with_state(state)
}

// POST /payments
async fn handle_payment(
    State(state): State<AppState>,
    Json(input): Json<PaymentIn>,
) -> Result<impl IntoResponse, AppError> {
    let t0 = Instant::now();
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

    let elapsed_ms = t0.elapsed().as_millis() as u64;
    if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments {}ms corr={} amt={}", elapsed_ms, input.correlation_id, input.amount); }
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"s":"ok"}))))
}

// GET /payments-summary
async fn handle_summary(
    State(state): State<AppState>,
    Query(q): Query<SummaryQuery>,
) -> Result<impl IntoResponse, AppError> {
    let t0 = Instant::now();
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
        let elapsed_ms = t0.elapsed().as_millis() as u64;
        if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments-summary fast {}ms", elapsed_ms); }
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

    let elapsed_ms = t0.elapsed().as_millis() as u64;
    if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments-summary ranged {}ms from={} to={}", elapsed_ms, q.from.as_deref().unwrap_or(""), q.to.as_deref().unwrap_or("")); }
    Ok((StatusCode::OK, Json(SummaryOut { default, fallback })))
}

// POST /admin/purge-payments
async fn handle_admin_purge(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    // valida token
    let token = headers
        .get("X-Rinha-Token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if token != state.admin_token {
        return Ok((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"message":"unauthorized"}))));
    }

    let mut redis = state.redis.clone();
    // limpa estruturas principais
    let _: () = redis::pipe()
        .del("pay:default")
        .del("pay:fallback")
        .del("payamtc")
        .del("summary:default:total_count")
        .del("summary:default:total_amount_cents")
        .del("summary:fallback:total_count")
        .del("summary:fallback:total_amount_cents")
        .del(&state.queue_key)
        .query_async(&mut redis)
        .await
        .map_err(anyhow::Error::from)?;

    // remove chaves derivadas por padrão
    async fn scan_del(redis: &mut redis::aio::ConnectionManager, pattern: &str) -> Result<(), AppError> {
        let mut cursor: u64 = 0;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(redis)
                .await
                .map_err(anyhow::Error::from)?;
            if !keys.is_empty() {
                let _: () = redis::cmd("DEL")
                    .arg(keys)
                    .query_async(redis)
                    .await
                    .map_err(anyhow::Error::from)?;
            }
            if next == 0 { break; }
            cursor = next;
        }
        Ok(())
    }

    // processed:* e corr:* podem ser muitos; varre com SCAN
    scan_del(&mut redis, "processed:*").await?;
    scan_del(&mut redis, "corr:*").await?;

    Ok((StatusCode::OK, Json(serde_json::json!({"message":"All payments purged."}))))
}
