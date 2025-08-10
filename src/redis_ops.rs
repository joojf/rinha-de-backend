use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use uuid::Uuid;

use crate::models::SummarySide;
use crate::state::AppError;

// Atualiza contadores no Redis: bucket por segundo e totais
// função removida: não estava em uso

pub async fn read_totals(
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

pub async fn sum_range(
    redis: &mut ConnectionManager,
    proc_name: &str,
    from_s: i64,
    to_s: i64,
) -> Result<SummarySide, AppError> {
    let key_z = format!("pay:{}", proc_name);
    let from_ms = from_s.saturating_mul(1000);
    let to_ms = to_s.saturating_mul(1000);

    let count_in_range: i64 = redis
        .zcount(&key_z, from_ms, to_ms)
        .await
        .map_err(anyhow::Error::from)?;

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

pub async fn record_event(
    redis: &mut ConnectionManager,
    proc_name: &str,
    ts: DateTime<Utc>,
    correlation_id: &Uuid,
    amount: f64,
) -> Result<(), AppError> {
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
