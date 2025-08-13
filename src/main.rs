use reqwest::Client;
use redis::aio::ConnectionManager;
use tokio::sync::mpsc;
use tokio::time::Duration;

mod config;
mod state;
mod models;
mod redis_ops;
mod ingest;
mod pipeline;

use config::Config;
use state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // configurar http/redis/estado
    let cfg = Config::from_env()?;

    let http = Client::builder()
        .http1_only()
        .pool_max_idle_per_host(128)
        .pool_idle_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_millis(20))
        .timeout(cfg.req_timeout)
        .build()?;

    let redis_client = redis::Client::open(cfg.redis_url.clone())?;
    let redis_mgr = ConnectionManager::new(redis_client.clone()).await?;

    let app = AppState { http, redis_client, redis: redis_mgr, queue_key: cfg.queue_key.clone(), idemp_ttl: cfg.idemp_ttl, req_timeout: cfg.req_timeout, default_base: cfg.default_base.clone() };

    // canal para ingestão -> flusher redis
    let (tx, rx) = mpsc::channel::<models::JobPayload>(cfg.batch_size * 16);

    // subir servidor público para summary e métricas básicas
    let app_public = app.clone();
    let router = ingest::build_public_router(app_public);
    let listener = tokio::net::TcpListener::bind(cfg.bind_addr).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await { eprintln!("serve public erro: {e}"); }
    });

    // subir servidor interno para ingestão (nginx mirror envia aqui)
    let app_ing = app.clone();
    let tx_ing = tx.clone();
    let router_ing = ingest::build_ingest_router(app_ing, tx_ing);
    let listener_ing = tokio::net::TcpListener::bind(cfg.ingest_addr).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener_ing, router_ing).await { eprintln!("serve ingest erro: {e}"); }
    });

    // flusher: drena canal e rpush em lotes para redis
    tokio::spawn(pipeline::flusher_loop(app.clone(), rx, cfg.batch_size, cfg.batch_flush_ms));

    // workers: blpop, post default, idempotência e agregação
    for _ in 0..cfg.workers { tokio::spawn(pipeline::worker_loop(app.clone())); }

    // segurar main
    loop { tokio::time::sleep(Duration::from_secs(3600)).await; }
}
