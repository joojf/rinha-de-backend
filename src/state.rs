use std::{sync::Arc, time::Duration};

use crate::config::Config;
use redis::Client as RedisClient;
use redis::aio::ConnectionManager;
use reqwest::Client;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::Instant;

// Estado global compartilhado entre handlers e tarefas
#[derive(Clone)]
pub struct AppState {
    pub http: Client,
    pub redis: ConnectionManager,
    pub redis_client: RedisClient,
    pub default_base: String,
    pub fallback_base: String,
    pub req_timeout: Duration,
    pub health: Arc<RwLock<HealthState>>,
    pub cb_fail_threshold: u32,
    pub cb_open_duration: Duration,
    pub queue_key: String,
    pub idemp_ttl: u64,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub timeout_margin: Duration,
    pub admin_token: String,
}

#[derive(Debug, Default, Clone)]
pub struct HealthState {
    pub default: ProcStatus,
    pub fallback: ProcStatus,
}

#[derive(Debug, Default, Clone)]
pub struct ProcStatus {
    pub health: ProcHealth,
    pub circuit: Circuit,
}

#[derive(Debug, Default, Clone)]
pub struct ProcHealth {
    pub failing: Option<bool>,
    pub min_response_time_ms: Option<u64>,
    pub last_checked: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub struct Circuit {
    pub failures: u32,
    pub open_until: Option<Instant>,
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("invalid amount")]
    InvalidAmount,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        use axum::{Json, http::StatusCode};
        let code = match self {
            AppError::InvalidAmount => StatusCode::BAD_REQUEST,
            AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, Json(serde_json::json!({ "error": self.to_string() }))).into_response()
    }
}

pub async fn build_state(cfg: &Config) -> anyhow::Result<AppState> {
    let http = Client::builder()
        .http1_only()
        .pool_max_idle_per_host(64)
        .pool_idle_timeout(Duration::from_secs(10))
        .tcp_keepalive(Duration::from_secs(30))
        .timeout(cfg.req_timeout)
        .build()?;

    let client = redis::Client::open(cfg.redis_url.clone())?;
    let manager = ConnectionManager::new(client.clone()).await?;

    Ok(AppState {
        http,
        redis: manager,
        redis_client: client,
        default_base: cfg.default_base.clone(),
        fallback_base: cfg.fallback_base.clone(),
        req_timeout: cfg.req_timeout,
        health: Arc::new(RwLock::new(HealthState::default())),
        cb_fail_threshold: cfg.cb_fail_threshold,
        cb_open_duration: cfg.cb_open_duration,
        queue_key: cfg.queue_key.clone(),
        idemp_ttl: cfg.idemp_ttl,
        max_retries: cfg.max_retries,
        retry_backoff: cfg.retry_backoff,
        timeout_margin: cfg.timeout_margin,
        admin_token: cfg.admin_token.clone(),
    })
}

// Escolha de processador para envio
pub enum ProcChoice {
    Default,
    Fallback,
}

pub async fn choose_target(state: &AppState) -> ProcChoice {
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

pub async fn update_circuit_after(state: &AppState, proc_name: &str, success: bool) {
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

pub async fn compute_timeout(state: &AppState, proc_name: &str) -> Duration {
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
        // Ajuste: usar minResponseTime + margem, garantindo um piso razoável e respeitando req_timeout
        let mut base = ms + state.timeout_margin.as_millis() as u64;
        if base < 200 {
            base = 200;
        }
        let to = base.max(state.req_timeout.as_millis() as u64);
        Duration::from_millis(to)
    } else {
        state.req_timeout
    }
}
