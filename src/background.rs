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
            let result = http.get(&url).timeout(Duration::from_millis(200)).send().await;
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
        let slow_ms = state.trace_slow_ms;
        // conexão dedicada para blpop
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
            let t0 = Instant::now();
            let res: redis::RedisResult<Option<(String, String)>> =
                redis_block.blpop(&queue, 1.0).await;
            match res {
                Ok(Some((_k, val))) => {
                    if let Ok(mut job) = serde_json::from_str::<JobPayload>(&val) {
                        // escolhe processador por job (idempotente)
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
                        let (proc_name, base, sem) = if proc == "default" {
                            ("default", state.default_base.clone(), state.sem_default.clone())
                        } else {
                            ("fallback", state.fallback_base.clone(), state.sem_fallback.clone())
                        };
                        // check se já foi processado
                        let proc_key = format!("processed:{}:{}", proc_name, job.correlation_id);
                        let already_processed: bool = redis_mgr
                            .exists(&proc_key)
                            .await
                            .unwrap_or(false);

                        let mut success = if already_processed {
                            // já processado, pula post
                            true
                        } else {
                            let _permit = sem.clone().acquire_owned().await.ok();
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
                        // se falhou/timeout, confirma via GET com tentativas (evita perdas por timeout)
                        if !success {
                            let det_url = format!("{}/payments/{}", base, job.correlation_id);
                            // Até 5 tentativas rápidas, backoff pequeno; timeouts ajustados
                            let mut tries = 0u32;
                            while !success && tries < 5 {
                                tries += 1;
                                let _permit = sem.clone().acquire_owned().await.ok();
                                // timeout baseado no health min_response_time quando houver; com teto
                                let mut to = crate::state::compute_timeout(&state, proc_name).await;
                                if to < Duration::from_millis(200) { to = Duration::from_millis(200); }
                                if to > Duration::from_millis(800) { to = Duration::from_millis(800); }
                                let det = state
                                    .http
                                    .get(&det_url)
                                    .timeout(to)
                                    .send()
                                    .await;
                                if let Ok(r) = det {
                                    if r.status().is_success() {
                                        // marca como processado
                                        let proc_key =
                                            format!("processed:{}:{}", proc_name, job.correlation_id);
                                        let _: Result<(), _> = redis_mgr
                                            .set_ex(&proc_key, 1, state.idemp_ttl as u64)
                                            .await;

                                        // registra pagamento com timestamp do requestedAt
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
                                        break;
                                    }
                                }
                                // backoff curto e não bloqueante
                                let sleep_ms = 20u64 * (1 << (tries.saturating_sub(1)).min(4));
                                sleep(Duration::from_millis(sleep_ms)).await;
                            }
                        }
                        update_circuit_after(&state, proc_name, success).await;
                        if success {
                            // marca como processado
                            let proc_key =
                                format!("processed:{}:{}", proc_name, job.correlation_id);
                            let _: Result<(), _> =
                                redis_mgr.set_ex(&proc_key, 1, state.idemp_ttl as u64).await;

                            // registra evento com timestamp correto
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
                                // exponential backoff anti thundering herd
                                let delay = state.retry_backoff.as_millis() as u64 * (1 << (job.attempts - 1).min(3));
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                                if let Ok(s) = serde_json::to_string(&job) {
                                    // reenfileira job
                                    let _: Result<(), _> = redis_mgr.rpush(&queue, s).await;
                                }
                            }
                        }
                        let elapsed = t0.elapsed().as_millis() as u64;
                        if elapsed >= slow_ms { eprintln!(
                            "SLOW worker {}ms proc={} corr={} attempts={} already_processed={}",
                            elapsed, proc_name, job.correlation_id, job.attempts, already_processed
                        ); }
                    }
                }
                Ok(None) => { /* blpop timeout */ }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    });
}

// spawna n workers
pub fn spawn_workers(state: AppState, n: usize) {
    for _ in 0..n {
        spawn_worker(state.clone());
    }
}
