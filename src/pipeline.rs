use serde_json::json;
use redis::AsyncCommands;
use tokio::time::{Duration, sleep};

use crate::models::JobPayload;
use crate::redis_ops::record_aggregate;
use crate::state::AppState;

pub async fn flusher_loop(app: AppState, mut rx: tokio::sync::mpsc::Receiver<JobPayload>, batch_size: usize, batch_flush_ms: u64) {
    let mut redis = app.redis.clone();
    loop {
        let mut buf: Vec<String> = Vec::with_capacity(batch_size);
        match rx.recv().await { Some(j) => buf.push(serde_json::to_string(&j).unwrap_or_default()), None => break }
        let start = tokio::time::Instant::now();
        while buf.len() < batch_size && start.elapsed() < Duration::from_millis(batch_flush_ms) {
            match rx.try_recv() { Ok(j) => buf.push(serde_json::to_string(&j).unwrap_or_default()), Err(_) => break }
        }
        if !buf.is_empty() {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for v in &buf { pipe.rpush(&app.queue_key, v).ignore(); }
            let _ : Result<(), _> = pipe.query_async(&mut redis).await;
        }
    }
}

pub async fn worker_loop(app: AppState) {
    #[allow(deprecated)]
    let mut bl = match app.redis_client.get_async_connection().await { Ok(c) => c, Err(_) => return };
    let mut redis = app.redis.clone();
    loop {
        let res: redis::RedisResult<Option<(String, String)>> = bl.blpop(&app.queue_key, 1.0).await;
        let matched = match res { Ok(v) => v, Err(_) => { sleep(Duration::from_millis(10)).await; continue } };
        let Some((_k, val)) = matched else { continue };
        let Ok(job) = serde_json::from_str::<JobPayload>(&val) else { continue };

        let proc = "default";
        let proc_key = format!("processed:{}:{}", proc, job.correlation_id);
        let already: bool = redis.exists(&proc_key).await.unwrap_or(false);
        if already { continue; }

        let url = format!("{}/payments", app.default_base);
        let body = json!({"correlationId": job.correlation_id, "amount": job.amount, "requestedAt": job.requested_at});
        let ok = match app.http.post(&url).timeout(app.req_timeout).json(&body).send().await { Ok(r) if r.status().is_success() || r.status().as_u16()==409 => true, _ => false };
        if !ok { let _ : Result<(), _> = redis.rpush(&app.queue_key, val).await; sleep(Duration::from_millis(2)).await; continue; }

        let _ : Result<(), _> = redis.set_ex(&proc_key, 1, app.idemp_ttl as u64).await;
        let ts = chrono::DateTime::parse_from_rfc3339(&job.requested_at).ok().map(|d| d.with_timezone(&chrono::Utc)).unwrap_or_else(chrono::Utc::now);
        record_aggregate(&mut redis, proc, &job.correlation_id.to_string(), job.amount, &ts).await;
    }
}


