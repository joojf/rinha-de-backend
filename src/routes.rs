use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use tokio::time::Instant;

use crate::models::*;
use crate::state::{AppError, AppState};
use crate::timeutil::{format_rfc3339_millis, parse_rfc3339};
use crate::memstore::{MsProc, MsPayload};

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

    // idempotência via memstore
    if let Some(ms) = &state.memstore {
        if let Ok(Some(_)) = ms.get(&input.correlation_id.to_string()).await {
            return Ok((StatusCode::OK, Json(serde_json::json!({"s":"dup"}))));
        }
    }

    // enfileira job para processamento assíncrono
    let job = JobPayload {
        correlation_id: input.correlation_id,
        amount: input.amount,
        requested_at: now_str.clone(),
        attempts: 0,
        proc_name: None,
    };
    let _ = state.queue_tx.send(job);

    let elapsed_ms = t0.elapsed().as_millis() as u64;
    if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments {}ms corr={} amt={}", elapsed_ms, input.correlation_id, input.amount); }
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"s":"ok"}))))
}

// GET /payments-summary
async fn handle_summary(
    State(mut state): State<AppState>,
    Query(q): Query<SummaryQuery>,
) -> Result<impl IntoResponse, AppError> {
    let t0 = Instant::now();
    if q.from.is_none() && q.to.is_none() {
        // memstore: agrega tudo; redis: caminho rápido antigo
        if let Some(ms) = &state.memstore {
            let mut def_cnt=0u64; let mut def_amt=0f64; let mut fb_cnt=0u64; let mut fb_amt=0f64;
            if let Ok(all) = ms.get_all().await {
                for (_id, p) in all {
                    match p.proc_name { MsProc::Default => { def_cnt+=1; def_amt+=p.amount; }, MsProc::Fallback => { fb_cnt+=1; fb_amt+=p.amount; } }
                }
            }
            let elapsed_ms = t0.elapsed().as_millis() as u64;
            if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments-summary mem all {}ms", elapsed_ms); }
            return Ok((StatusCode::OK, Json(SummaryOut{ default: SummarySide{ total_requests: def_cnt, total_amount: def_amt }, fallback: SummarySide{ total_requests: fb_cnt, total_amount: fb_amt } })));
        }
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
    if let Some(ms) = &state.memstore {
        let mut def = SummarySide::default();
        let mut fb = SummarySide::default();
        if let Ok(all) = ms.get_all().await {
            for (_id, p) in all {
                if p.requested_at_ms < from_ms || p.requested_at_ms > to_ms { continue; }
                match p.proc_name { MsProc::Default => { def.total_requests+=1; def.total_amount+=p.amount; }, MsProc::Fallback => { fb.total_requests+=1; fb.total_amount+=p.amount; } }
            }
        }
        let elapsed_ms = t0.elapsed().as_millis() as u64;
        if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments-summary mem ranged {}ms", elapsed_ms); }
        return Ok((StatusCode::OK, Json(SummaryOut{ default: def, fallback: fb })));
    }
    let elapsed_ms = t0.elapsed().as_millis() as u64;
    if elapsed_ms >= state.trace_slow_ms { eprintln!("SLOW /payments-summary ranged {}ms from={} to={}", elapsed_ms, q.from.as_deref().unwrap_or(""), q.to.as_deref().unwrap_or("")); }
    Ok((StatusCode::OK, Json(SummaryOut::default())))
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

    // limpa memstore se configurado
    if let Some(ms) = &state.memstore { let _ = ms.purge().await; }

    Ok((StatusCode::OK, Json(serde_json::json!({"message":"All payments purged."}))))
}
