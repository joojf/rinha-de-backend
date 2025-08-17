use chrono::Utc;
use tokio::time::{Duration, Instant, sleep};

use crate::models::{HealthResponse, JobPayload, ProcessorPayment};
use crate::memstore::{MsPayload, MsProc};
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

// worker que consome a fila local (mpsc) e processa pagamentos
pub fn spawn_worker(state: AppState) {
    tokio::spawn(async move {
        let slow_ms = state.trace_slow_ms;
        loop {
            let t0 = Instant::now();
            // recebe do canal compartilhado
            let job_opt = {
                let mut rx = state.queue_rx.lock().await;
                rx.recv().await
            };
            match job_opt {
                Some(mut job) => {
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
                        // check se já foi processado via memstore
                        let already_processed: bool = if let Some(ms) = &state.memstore {
                            ms.get(&job.correlation_id.to_string()).await.ok().flatten().is_some()
                        } else { false };

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
                                        // registra pagamento com timestamp do requestedAt
                                        let now = crate::timeutil::parse_rfc3339(&job.requested_at)
                                            .unwrap_or_else(Utc::now);
                                        if let Some(ms) = &state.memstore {
                                            let _ = ms.set(
                                                &job.correlation_id.to_string(),
                                                &MsPayload {
                                                    amount: job.amount,
                                                    requested_at_ms: now.timestamp_millis(),
                                                    proc_name: if proc_name == "default" { MsProc::Default } else { MsProc::Fallback },
                                                },
                                            ).await;
                                        }
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
                            // registra evento com timestamp correto
                            let now = crate::timeutil::parse_rfc3339(&job.requested_at)
                                .unwrap_or_else(Utc::now);
                            if let Some(ms) = &state.memstore {
                                let _ = ms.set(
                                    &job.correlation_id.to_string(),
                                    &MsPayload {
                                        amount: job.amount,
                                        requested_at_ms: now.timestamp_millis(),
                                        proc_name: if proc_name == "default" { MsProc::Default } else { MsProc::Fallback },
                                    },
                                ).await;
                            }
                        } else {
                            job.attempts = job.attempts.saturating_add(1);
                            let attempts = job.attempts;
                            if attempts <= state.max_retries {
                                // exponential backoff anti thundering herd
                                let delay = state.retry_backoff.as_millis() as u64 * (1 << (attempts - 1).min(3));
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                                // reenfileira job
                                let _ = state.queue_tx.send(job.clone());
                            }
                        }
                        let elapsed = t0.elapsed().as_millis() as u64;
                        if elapsed >= slow_ms { eprintln!(
                            "SLOW worker {}ms proc={} corr={} already_processed={}",
                            elapsed, proc_name, job.correlation_id, already_processed
                        ); }
                }
                None => { tokio::time::sleep(Duration::from_millis(5)).await; }
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
