use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use uuid::Uuid;

use crate::models::SummarySide;
use crate::state::AppError;

// Converte valor monetário em centavos inteiros para evitar erro de ponto flutuante
fn to_cents(amount: f64) -> i64 {
    (amount * 100.0).round() as i64
}

// Atualiza contadores no Redis: bucket por segundo e totais
// função removida: não estava em uso

pub async fn read_totals(
    redis: &mut ConnectionManager,
    proc_name: &str,
) -> Result<SummarySide, AppError> {
    let total_count = format!("summary:{}:total_count", proc_name);
    let total_amount_c = format!("summary:{}:total_amount_cents", proc_name);
    let (count, amount_c): (Option<i64>, Option<i64>) = redis
        .mget((&total_count, &total_amount_c))
        .await
        .map_err(anyhow::Error::from)?;
    Ok(SummarySide {
        total_requests: count.unwrap_or(0) as u64,
        total_amount: amount_c.unwrap_or(0) as f64 / 100.0,
    })
}

pub async fn sum_range(
    redis: &mut ConnectionManager,
    proc_name: &str,
    from_ms: i64,
    to_ms: i64,
) -> Result<SummarySide, AppError> {
    let key_z = format!("pay:{}", proc_name);
    // Snapshot estável do range para evitar inconsistências durante paginação
    let tmp_key = format!("tmp:sum:{}:{}", proc_name, Uuid::new_v4());
    let _: i64 = redis::cmd("ZRANGESTORE")
        .arg(&tmp_key)
        .arg(&key_z)
        .arg(from_ms)
        .arg(to_ms)
        .arg("BYSCORE")
        .query_async(redis)
        .await
        .map_err(anyhow::Error::from)?;

    let count_in_range: i64 = redis.zcard(&tmp_key).await.map_err(anyhow::Error::from)?;

    let mut total_amount_cents: i64 = 0;
    const CHUNK: isize = 2048;
    let mut offset: isize = 0;
    loop {
        let ids: Vec<String> = redis
            .zrange(&tmp_key, offset, offset + CHUNK - 1)
            .await
            .map_err(anyhow::Error::from)?;
        if ids.is_empty() {
            break;
        }
        let amts: Vec<Option<i64>> = redis
            .hget("payamtc", &ids)
            .await
            .map_err(anyhow::Error::from)?;
        for v in amts.into_iter().flatten() {
            total_amount_cents = total_amount_cents.saturating_add(v)
        }
        offset += ids.len() as isize;
    }
    // Limpeza do snapshot temporário
    let _: () = redis.del(&tmp_key).await.map_err(anyhow::Error::from)?;
    Ok(SummarySide {
        total_requests: count_in_range as u64,
        total_amount: total_amount_cents as f64 / 100.0,
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
    let cents = to_cents(amount);

    // HSET sempre sobrescreve o valor do id (idempotente para nossa soma)
    let _: () = redis
        .hset("payamtc", &id, cents)
        .await
        .map_err(anyhow::Error::from)?;
    // ZADD NX retorna 1 se novo membro foi adicionado, 0 se já existia
    let added: i64 = redis::cmd("ZADD")
        .arg(&key_z)
        .arg("NX")
        .arg(epoch_ms)
        .arg(&id)
        .query_async(redis)
        .await
        .map_err(anyhow::Error::from)?;

    // Atualiza totais apenas se o ID for novo (evita overcount em retries)
    if added == 1 {
        let total_count = format!("summary:{}:total_count", proc_name);
        let total_amount_c = format!("summary:{}:total_amount_cents", proc_name);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .incr(&total_count, 1)
            .ignore()
            .cmd("INCRBY")
            .arg(&total_amount_c)
            .arg(cents)
            .ignore();
        let _: () = pipe.query_async(redis).await.map_err(anyhow::Error::from)?;
    }
    Ok(())
}
