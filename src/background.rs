use chrono::Utc;
use redis::AsyncCommands;
use tokio::time::{Duration, Instant, sleep};

use crate::models::{HealthResponse, JobPayload, ProcessorPayment};
use crate::redis_ops::record_event;
use crate::state::{AppState, ProcChoice, choose_target, compute_timeout, update_circuit_after};

pub fn spawn_health_checker(state: AppState, is_default: bool) {
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

// worker que consome a fila do Redis e processa pagamentos
pub fn spawn_worker(state: AppState) {
    tokio::spawn(async move {
        // conexão dedicada para operações bloqueantes (BLPOP)
        let queue = state.queue_key.clone();
        #[allow(deprecated)]
        let mut redis_block = match state.redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("falha obtendo conexão dedicada ao Redis: {}", e);
                return;
            }
        };
        let mut redis_mgr = state.redis.clone();
        loop {
            let res: redis::RedisResult<Option<(String, String)>> =
                redis_block.blpop(&queue, 1.0).await;
            match res {
                Ok(Some((_k, val))) => {
                    if let Ok(mut job) = serde_json::from_str::<JobPayload>(&val) {
                        // fixa o processador escolhido por job para evitar duplo processamento em serviços distintos
                        let proc = if let Some(ref p) = job.proc_name {
                            p.as_str()
                        } else {
                            let choice = choose_target(&state).await;
                            let p = match choice {
                                ProcChoice::Default => "default",
                                ProcChoice::Fallback => "fallback",
                            };
                            job.proc_name = Some(p.to_string());
                            p
                        };
                        let (proc_name, base) = if proc == "default" {
                            ("default", state.default_base.clone())
                        } else {
                            ("fallback", state.fallback_base.clone())
                        };
                        // verificar se já processamos com sucesso este correlationId neste processador
                        let proc_key = format!("processed:{}:{}", proc_name, job.correlation_id);
                        let already_processed: Option<String> =
                            redis_mgr.get(&proc_key).await.unwrap_or(None);

                        let mut success = if already_processed.is_some() {
                            // já processado com sucesso anteriormente, pula o POST
                            true
                        } else {
                            let to = compute_timeout(&state, proc_name).await;
                            let url = format!("{}/payments", base);
                            let req = state.http.post(&url).timeout(to).json(&ProcessorPayment {
                                correlation_id: &job.correlation_id,
                                amount: job.amount,
                                requested_at: job.requested_at.clone(),
                            });
                            let resp = req.send().await;
                            match &resp {
                                Ok(r) if r.status().is_success() => true,
                                Ok(r) if r.status().as_u16() == 409 => true,
                                _ => false,
                            }
                        };
                        // se falhou ou expirou, tenta confirmar no processor via GET /payments/{id}
                        if !success {
                            let det_url = format!("{}/payments/{}", base, job.correlation_id);
                            let det = state
                                .http
                                .get(&det_url)
                                .timeout(Duration::from_millis(300))
                                .send()
                                .await;
                            if let Ok(r) = det {
                                if r.status().is_success() {
                                    // marcar como processado para evitar futuras tentativas
                                    let proc_key =
                                        format!("processed:{}:{}", proc_name, job.correlation_id);
                                    let _: Result<(), _> = redis_mgr
                                        .set_ex(&proc_key, 1, state.idemp_ttl as u64)
                                        .await;

                                    // considera como processado: registra localmente e evita retry
                                    let now = crate::timeutil::parse_rfc3339(&job.requested_at)
                                        .unwrap_or_else(Utc::now);
                                    let _ = record_event(
                                        &mut redis_mgr,
                                        proc_name,
                                        now,
                                        &job.correlation_id,
                                        job.amount,
                                    )
                                    .await;
                                    success = true;
                                }
                            }
                        }
                        update_circuit_after(&state, proc_name, success).await;
                        if success {
                            // marcar como processado com sucesso para evitar reprocessamento
                            let proc_key =
                                format!("processed:{}:{}", proc_name, job.correlation_id);
                            let _: Result<(), _> =
                                redis_mgr.set_ex(&proc_key, 1, state.idemp_ttl as u64).await;

                            // alinhar timestamp com o usado pelo processor (requestedAt)
                            let now = crate::timeutil::parse_rfc3339(&job.requested_at)
                                .unwrap_or_else(Utc::now);
                            let _ = record_event(
                                &mut redis_mgr,
                                proc_name,
                                now,
                                &job.correlation_id,
                                job.amount,
                            )
                            .await;
                        } else {
                            job.attempts = job.attempts.saturating_add(1);
                            if job.attempts <= state.max_retries {
                                tokio::time::sleep(state.retry_backoff).await;
                                if let Ok(s) = serde_json::to_string(&job) {
                                    // re-enfileira usando conexão de manager (não bloqueante)
                                    let _: Result<(), _> = redis_mgr.lpush(&queue, s).await;
                                }
                            }
                        }
                    }
                }
                Ok(None) => { /* timeout */ }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    });
}

// Spawna N workers
pub fn spawn_workers(state: AppState, n: usize) {
    for _ in 0..n {
        spawn_worker(state.clone());
    }
}
