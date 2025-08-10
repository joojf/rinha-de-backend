use std::{net::SocketAddr, sync::Arc, time::Duration};

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
use tokio::sync::RwLock;
use tokio::time::{Instant, sleep};
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

#[derive(Debug, Serialize, Deserialize)]
struct JobPayload {
    #[serde(rename = "correlationId")]
    correlation_id: Uuid,
    amount: f64,
    #[serde(rename = "requestedAt")]
    requested_at: String,
    #[serde(default)]
    attempts: u32,
}

#[derive(Clone)]
struct AppState {
    http: Client,
    redis: ConnectionManager,
    default_base: String,
    fallback_base: String,
    req_timeout: Duration,
    // Estado compartilhado: health dos serviços e circuit breaker
    health: Arc<RwLock<HealthState>>,
    cb_fail_threshold: u32,
    cb_open_duration: Duration,
    queue_key: String,
    idemp_ttl: u64,
    max_retries: u32,
    retry_backoff: Duration,
    timeout_margin: Duration,
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

// Estado de health + circuit breaker
#[derive(Debug, Default, Clone)]
struct HealthState {
    default: ProcStatus,
    fallback: ProcStatus,
}

#[derive(Debug, Default, Clone)]
struct ProcStatus {
    health: ProcHealth,
    circuit: Circuit,
}

#[derive(Debug, Default, Clone)]
struct ProcHealth {
    failing: Option<bool>,
    min_response_time_ms: Option<u64>,
    last_checked: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
struct Circuit {
    failures: u32,
    open_until: Option<Instant>,
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    failing: bool,
    #[serde(rename = "minResponseTime")]
    min_response_time: u64,
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
    let cb_fail_threshold = std::env::var("CB_FAIL_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(3);
    let cb_open_duration = std::env::var("CB_OPEN_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(1500));
    let queue_key = std::env::var("QUEUE_KEY").unwrap_or_else(|_| "q:payments".to_string());
    let idemp_ttl = std::env::var("IDEMP_TTL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(24 * 60 * 60);
    let max_retries = std::env::var("MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(3);
    let retry_backoff = std::env::var("RETRY_BACKOFF_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(100));
    let timeout_margin = std::env::var("TIMEOUT_MARGIN_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(5));

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
        health: Arc::new(RwLock::new(HealthState::default())),
        cb_fail_threshold,
        cb_open_duration,
        queue_key: queue_key.clone(),
        idemp_ttl,
        max_retries,
        retry_backoff,
        timeout_margin,
    };

    // Inicia health-checkers em background (1 chamada a cada 5s por serviço)
    spawn_health_checker(state.clone(), true);
    spawn_health_checker(state.clone(), false);

    // Inicia worker consumidor de fila
    spawn_worker(state.clone());

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
    // Usa SET NX EX (uma ida só)
    // SET key 1 NX EX idemp_ttl
    let set_res: Option<String> = redis::cmd("SET")
        .arg(&corr_key)
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(state.idemp_ttl)
        .query_async(&mut redis)
        .await
        .map_err(anyhow::Error::from)?;
    let inserted = set_res.is_some();
    if !inserted {
        // já processado; não reencaminha e responde OK
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({"status":"duplicate"})),
        ));
    }
    // Enfileira o job e responde 202 imediatamente
    let job = JobPayload {
        correlation_id: input.correlation_id,
        amount: input.amount,
        requested_at: now_str,
        attempts: 0,
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
    // Janelas exatas em milissegundos usando ZSET + HASH
    let key_z = format!("pay:{}", proc_name);
    let from_ms = from_s.saturating_mul(1000);
    let to_ms = to_s.saturating_mul(1000);

    // totalRequests via ZCOUNT
    let count_in_range: i64 = redis
        .zcount(&key_z, from_ms, to_ms)
        .await
        .map_err(anyhow::Error::from)?;

    // Busca IDs em blocos e soma amounts via HASH
    let mut total_amount: f64 = 0.0;
    let mut cursor = from_ms;
    const CHUNK: isize = 512;
    loop {
        let ids: Vec<String> = redis
            .zrangebyscore_limit(&key_z, cursor, to_ms, 0, CHUNK)
            .await
            .map_err(anyhow::Error::from)?;
        if ids.is_empty() {
            break;
        }
        let amts: Vec<Option<f64>> = redis
            .hget("payamt", &ids)
            .await
            .map_err(anyhow::Error::from)?;
        for a in amts.into_iter().flatten() {
            total_amount += a;
        }
        // avança cursor usando o último elemento
        if let Some(last_id) = ids.last() {
            let score: Result<f64, _> = redis.zscore(&key_z, last_id).await;
            match score {
                Ok(s) => cursor = (s as i64) + 1,
                Err(_) => break,
            }
        } else {
            break;
        }
    }
    Ok(SummarySide {
        total_requests: count_in_range as u64,
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

// Escolha de destino com base no estado atual
enum ProcChoice {
    Default,
    Fallback,
    None,
}

async fn choose_target(state: &AppState) -> ProcChoice {
    let now = Instant::now();
    let hs = state.health.read().await;
    let def_open = hs
        .default
        .circuit
        .open_until
        .map(|t| t > now)
        .unwrap_or(false);
    let fb_open = hs
        .fallback
        .circuit
        .open_until
        .map(|t| t > now)
        .unwrap_or(false);
    let def_ok = hs.default.health.failing.map(|f| !f).unwrap_or(true);
    let fb_ok = hs.fallback.health.failing.map(|f| !f).unwrap_or(true);

    if !def_open && def_ok {
        return ProcChoice::Default;
    }
    if !fb_open && fb_ok {
        return ProcChoice::Fallback;
    }
    ProcChoice::Default
}

// Atualiza CB após a tentativa
async fn update_circuit_after(state: &AppState, proc_name: &str, success: bool) {
    let now = Instant::now();
    let mut hs = state.health.write().await;
    let ps = match proc_name {
        "default" => &mut hs.default,
        _ => &mut hs.fallback,
    };
    if success {
        ps.circuit.failures = 0;
        ps.circuit.open_until = None;
    } else {
        ps.circuit.failures = ps.circuit.failures.saturating_add(1);
        if ps.circuit.failures >= state.cb_fail_threshold {
            ps.circuit.open_until = Some(now + state.cb_open_duration);
            ps.circuit.failures = 0;
        }
    }
}

// Tarefa de health-check com 1 chamada a cada 5s
fn spawn_health_checker(state: AppState, is_default: bool) {
    let base = if is_default {
        state.default_base.clone()
    } else {
        state.fallback_base.clone()
    };
    let http = state.http.clone();
    let health = state.health.clone();
    tokio::spawn(async move {
        let url = format!("{}/payments/service-health", base);
        loop {
            let started = Instant::now();
            let result = http.get(&url).send().await;
            if let Ok(resp) = result {
                if resp.status().is_success() {
                    if let Ok(h) = resp.json::<HealthResponse>().await {
                        let mut hs = health.write().await;
                        let ps = if is_default {
                            &mut hs.default
                        } else {
                            &mut hs.fallback
                        };
                        ps.health.failing = Some(h.failing);
                        ps.health.min_response_time_ms = Some(h.min_response_time);
                        ps.health.last_checked = Some(Instant::now());
                    }
                }
            }
            let elapsed = started.elapsed();
            if elapsed < Duration::from_secs(5) {
                sleep(Duration::from_secs(5) - elapsed).await;
            }
        }
    });
}

// Worker que consome a fila do Redis e processa pagamentos
fn spawn_worker(state: AppState) {
    tokio::spawn(async move {
        let queue = state.queue_key.clone();
        let mut redis = state.redis.clone();
        loop {
            // BRPOP com timeout curto
            let res: redis::RedisResult<Option<(String, String)>> = redis.brpop(&queue, 1.0).await;
            match res {
                Ok(Some((_k, val))) => {
                    if let Ok(mut job) = serde_json::from_str::<JobPayload>(&val) {
                        let proc_choice = choose_target(&state).await;
                        let (proc_name, base) = match proc_choice {
                            ProcChoice::Default => ("default", state.default_base.clone()),
                            ProcChoice::Fallback => ("fallback", state.fallback_base.clone()),
                            ProcChoice::None => ("default", state.default_base.clone()),
                        };

                        // Timeout adaptativo baseado no health
                        let to = compute_timeout(&state, proc_name).await;
                        let url = format!("{}/payments", base);
                        let req = state.http.post(&url).timeout(to).json(&ProcessorPayment {
                            correlation_id: &job.correlation_id,
                            amount: job.amount,
                            requested_at: job.requested_at.clone(),
                        });
                        let resp = req.send().await;
                        let success = matches!(&resp, Ok(r) if r.status().is_success());
                        update_circuit_after(&state, proc_name, success).await;
                        if success {
                            // registra evento exato e totais
                            let now = Utc::now();
                            if let Err(e) = record_event(
                                &mut redis,
                                proc_name,
                                now,
                                &job.correlation_id,
                                job.amount,
                            )
                            .await
                            {
                                eprintln!("erro registrando evento: {}", e);
                            }
                        } else {
                            // retry com backoff e limite
                            job.attempts = job.attempts.saturating_add(1);
                            if job.attempts <= state.max_retries {
                                tokio::time::sleep(state.retry_backoff).await;
                                if let Ok(s) = serde_json::to_string(&job) {
                                    let _: Result<(), _> = redis.lpush(&queue, s).await;
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // timeout sem itens
                }
                Err(err) => {
                    eprintln!("erro na fila: {}", err);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    });
}

async fn record_event(
    redis: &mut ConnectionManager,
    proc_name: &str,
    ts: DateTime<Utc>,
    correlation_id: &Uuid,
    amount: f64,
) -> Result<(), AppError> {
    // ZSET por processador e HASH global de amounts
    let key_z = format!("pay:{}", proc_name);
    let epoch_ms = ts.timestamp_millis();
    let id = correlation_id.to_string();

    let mut pipe = redis::pipe();
    pipe.atomic()
        .cmd("ZADD")
        .arg(&key_z)
        .arg(epoch_ms)
        .arg(&id)
        .ignore()
        .cmd("HSETNX")
        .arg("payamt")
        .arg(&id)
        .arg(amount)
        .ignore();
    // Totais agregados mantidos para consultas sem janela
    let total_count = format!("summary:{}:total_count", proc_name);
    let total_amount = format!("summary:{}:total_amount", proc_name);
    pipe.incr(&total_count, 1)
        .ignore()
        .cmd("INCRBYFLOAT")
        .arg(&total_amount)
        .arg(amount)
        .ignore();
    let _: () = pipe.query_async(redis).await.map_err(anyhow::Error::from)?;
    Ok(())
}

async fn compute_timeout(state: &AppState, proc_name: &str) -> Duration {
    let hs = state.health.read().await;
    let (failing, min_rt) = if proc_name == "default" {
        (
            hs.default.health.failing,
            hs.default.health.min_response_time_ms,
        )
    } else {
        (
            hs.fallback.health.failing,
            hs.fallback.health.min_response_time_ms,
        )
    };
    if let (Some(false), Some(ms)) = (failing, min_rt) {
        let base = ms
            .saturating_sub(state.timeout_margin.as_millis() as u64)
            .max(20);
        Duration::from_millis(base)
    } else {
        state.req_timeout
    }
}
