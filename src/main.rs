use std::{net::SocketAddr, time::Duration};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, TimeZone, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

// Estruturas de payload
#[derive(Debug, Deserialize)]
struct PaymentIn {
    #[serde(rename = "correlationId")]
    correlation_id: Uuid,
    amount: f64,
}

#[derive(Debug, Serialize)]
struct ProcessorPayment<'a> {
    #[serde(rename = "correlationId")]
    correlation_id: &'a Uuid,
    amount: f64,
    #[serde(rename = "requestedAt")]
    requested_at: String,
}

#[derive(Clone)]
struct AppState {
    http: Client,
    redis: ConnectionManager,
    default_base: String,
    fallback_base: String,
    req_timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct SummaryQuery {
    from: Option<String>,
    to: Option<String>,
}

#[derive(Debug, Serialize, Default)]
struct SummarySide {
    #[serde(rename = "totalRequests")]
    total_requests: u64,
    #[serde(rename = "totalAmount")]
    total_amount: f64,
}

#[derive(Debug, Serialize, Default)]
struct SummaryOut {
    default: SummarySide,
    fallback: SummarySide,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("invalid amount")]
    InvalidAmount,
    #[error("duplicate correlation id")]
    Duplicate,
    #[error("processors unavailable")]
    Unavailable,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let code = match self {
            AppError::InvalidAmount => StatusCode::BAD_REQUEST,
            AppError::Duplicate => StatusCode::OK, // idempotência: já processado
            AppError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, Json(serde_json::json!({ "error": self.to_string() }))).into_response()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configurações via env
    let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9999".to_string())
        .parse()
        .expect("BIND_ADDR inválido");
    let default_base =
        std::env::var("DEFAULT_URL").unwrap_or_else(|_| "http://localhost:8001".to_string());
    let fallback_base =
        std::env::var("FALLBACK_URL").unwrap_or_else(|_| "http://localhost:8002".to_string());
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let req_timeout = std::env::var("REQ_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(120));

    let http = Client::builder()
        .http1_only()
        .pool_idle_timeout(Duration::from_secs(10))
        .tcp_keepalive(Duration::from_secs(30))
        .timeout(req_timeout)
        .build()?;

    let client = redis::Client::open(redis_url)?;
    let manager = ConnectionManager::new(client).await?;

    let state = AppState {
        http,
        redis: manager,
        default_base,
        fallback_base,
        req_timeout,
    };

    let app = Router::new()
        .route("/payments", post(handle_payment))
        .route("/payments-summary", get(handle_summary))
        .with_state(state);

    println!("listening on {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// POST /payments
async fn handle_payment(
    State(state): State<AppState>,
    Json(input): Json<PaymentIn>,
) -> Result<impl IntoResponse, AppError> {
    // validações básicas
    if !input.amount.is_finite() || input.amount <= 0.0 {
        return Err(AppError::InvalidAmount);
    }

    let now = Utc::now();
    let now_str = format_rfc3339_millis(now);

    // idempotência pelo correlationId
    let mut redis = state.redis.clone();
    let corr_key = format!("corr:{}", input.correlation_id);
    let inserted: bool = redis
        .set_nx(&corr_key, 1)
        .await
        .map_err(anyhow::Error::from)?;
    if !inserted {
        // já processado; não reencaminha e responde OK
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({"status":"duplicate"})),
        ));
    }
    // manter por um tempo razoável (12h)
    let _: () = redis
        .expire(&corr_key, 60 * 60 * 12)
        .await
        .map_err(anyhow::Error::from)?;

    let payload = ProcessorPayment {
        correlation_id: &input.correlation_id,
        amount: input.amount,
        requested_at: now_str,
    };

    // tenta default primeiro
    let default_url = format!("{}/payments", state.default_base);
    let fallback_url = format!("{}/payments", state.fallback_base);

    let mut chosen: Option<&str> = None;
    let resp_default = state.http.post(&default_url).json(&payload).send().await;

    let ok_default = matches!(&resp_default, Ok(r) if r.status().is_success());
    if ok_default {
        chosen = Some("default");
    } else {
        // tenta fallback
        let resp_fb = state.http.post(&fallback_url).json(&payload).send().await;
        if matches!(&resp_fb, Ok(r) if r.status().is_success()) {
            chosen = Some("fallback");
        }
    }

    // atualiza métricas ou falha
    if let Some(proc_name) = chosen {
        update_counters(&mut redis, proc_name, now, input.amount).await?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"status":"ok","processor":proc_name})),
        ));
    }

    Err(AppError::Unavailable)
}

// GET /payments-summary
async fn handle_summary(
    State(state): State<AppState>,
    Query(q): Query<SummaryQuery>,
) -> Result<impl IntoResponse, AppError> {
    let mut redis = state.redis.clone();

    // sem período: retorna totais acumulados
    if q.from.is_none() && q.to.is_none() {
        let default = read_totals(&mut redis, "default").await?;
        let fallback = read_totals(&mut redis, "fallback").await?;
        let out = SummaryOut { default, fallback };
        return Ok((StatusCode::OK, Json(out)));
    }

    // com período: soma buckets por segundo [from, to]
    let from = q
        .from
        .as_deref()
        .and_then(parse_rfc3339)
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let to =
        q.to.as_deref()
            .and_then(parse_rfc3339)
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
    let (from_s, to_s) = (from.timestamp(), to.timestamp());
    if to_s < from_s {
        return Ok((StatusCode::OK, Json(SummaryOut::default())));
    }

    let default = sum_range(&mut redis, "default", from_s, to_s).await?;
    let fallback = sum_range(&mut redis, "fallback", from_s, to_s).await?;
    let out = SummaryOut { default, fallback };
    Ok((StatusCode::OK, Json(out)))
}

// Atualiza contadores no Redis: bucket por segundo e totais
async fn update_counters(
    redis: &mut ConnectionManager,
    proc_name: &str,
    ts: DateTime<Utc>,
    amount: f64,
) -> Result<(), AppError> {
    let sec = ts.timestamp();
    let bucket_count = format!("summary:{}:{}:count", proc_name, sec);
    let bucket_amount = format!("summary:{}:{}:amount", proc_name, sec);
    let total_count = format!("summary:{}:total_count", proc_name);
    let total_amount = format!("summary:{}:total_amount", proc_name);

    let mut pipe = redis::pipe();
    pipe.atomic()
        .incr(&bucket_count, 1)
        .ignore()
        .incr(&total_count, 1)
        .ignore()
        .cmd("INCRBYFLOAT")
        .arg(&bucket_amount)
        .arg(amount)
        .ignore()
        .cmd("INCRBYFLOAT")
        .arg(&total_amount)
        .arg(amount)
        .ignore();
    let _: () = pipe.query_async(redis).await.map_err(anyhow::Error::from)?;
    Ok(())
}

async fn read_totals(
    redis: &mut ConnectionManager,
    proc_name: &str,
) -> Result<SummarySide, AppError> {
    let total_count = format!("summary:{}:total_count", proc_name);
    let total_amount = format!("summary:{}:total_amount", proc_name);
    let (count, amount): (Option<i64>, Option<f64>) = redis
        .mget((&total_count, &total_amount))
        .await
        .map_err(anyhow::Error::from)?;
    Ok(SummarySide {
        total_requests: count.unwrap_or(0) as u64,
        total_amount: amount.unwrap_or(0.0),
    })
}

async fn sum_range(
    redis: &mut ConnectionManager,
    proc_name: &str,
    from_s: i64,
    to_s: i64,
) -> Result<SummarySide, AppError> {
    // varre em blocos para limitar roundtrips
    const SLICE: i64 = 200; // segundos por lote
    let mut total_count: u64 = 0;
    let mut total_amount: f64 = 0.0;
    let mut cur = from_s;
    while cur <= to_s {
        let end = (cur + SLICE - 1).min(to_s);
        let mut keys_count = Vec::with_capacity((end - cur + 1) as usize);
        let mut keys_amount = Vec::with_capacity((end - cur + 1) as usize);
        for s in cur..=end {
            keys_count.push(format!("summary:{}:{}:count", proc_name, s));
            keys_amount.push(format!("summary:{}:{}:amount", proc_name, s));
        }
        // MGET counts
        let counts: Vec<Option<i64>> = redis.mget(keys_count).await.map_err(anyhow::Error::from)?;
        let amounts: Vec<Option<f64>> =
            redis.mget(keys_amount).await.map_err(anyhow::Error::from)?;
        for c in counts.into_iter().flatten() {
            total_count = total_count.saturating_add(c as u64);
        }
        for a in amounts.into_iter().flatten() {
            total_amount += a;
        }
        cur = end + 1;
    }
    Ok(SummarySide {
        total_requests: total_count,
        total_amount,
    })
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn format_rfc3339_millis(ts: DateTime<Utc>) -> String {
    // formata com milissegundos e sufixo Z
    // Comentário: padronizamos para comparação e legibilidade
    let secs = ts.timestamp();
    let nsec = ts.timestamp_subsec_nanos();
    let millis = nsec / 1_000_000;
    let dt = Utc.timestamp_opt(secs, 0).unwrap();
    format!("{}.{:03}Z", dt.format("%Y-%m-%dT%H:%M:%S"), millis)
}
