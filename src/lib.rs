// Biblioteca principal: expõe start() para inicializar servidor
// Comentários em pt-BR conforme diretrizes

mod background;
mod config;
mod models;
mod redis_ops;
mod routes;
mod state;
mod timeutil;

pub use crate::config::Config;
pub use crate::state::AppState;

// Inicia health-checkers, worker, constrói o router e inicia o servidor
pub async fn start() -> anyhow::Result<()> {
    use axum::Router;
    use tokio::net::TcpListener;

    let cfg = config::Config::from_env()?;
    let state = state::build_state(&cfg).await?;

    // Tarefas em background
    background::spawn_health_checker(state.clone(), true);
    background::spawn_health_checker(state.clone(), false);
    // múltiplos workers para throughput; valor via env
    background::spawn_workers(state.clone(), cfg.workers);

    let app: Router = routes::router(state);

    println!("listening on {}", cfg.bind_addr);
    let listener = TcpListener::bind(cfg.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
