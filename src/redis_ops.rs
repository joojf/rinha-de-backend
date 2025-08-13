use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

pub async fn record_aggregate(redis: &mut ConnectionManager, proc: &str, id: &str, amount: f64, ts: &DateTime<Utc>) {
    let cents = ((amount * 100.0).round() as i64).max(0);
    let epoch_ms = ts.timestamp_millis();
    let key_z = format!("pay:{}", proc);
    let total_count = format!("summary:{}:total_count", proc);
    let total_amount = format!("summary:{}:total_amount_cents", proc);
    let script = redis::Script::new(r#"
local keyz = KEYS[1]
local total_count = KEYS[2]
local total_amount = KEYS[3]
local id = ARGV[1]
local score = tonumber(ARGV[2])
local cents = tonumber(ARGV[3])
redis.call('HSET','payamtc', id, cents)
local added = redis.call('ZADD', keyz, 'NX', score, id)
if added == 1 then
  redis.call('INCRBY', total_count, 1)
  redis.call('INCRBY', total_amount, cents)
end
return added
"#);
    let _ : i64 = script.key(key_z).key(total_count).key(total_amount).arg(id).arg(epoch_ms).arg(cents).invoke_async(redis).await.unwrap_or(0);
}

pub async fn sum_range(redis: &mut ConnectionManager, proc: &str, from_ms: i64, to_ms: i64) -> (u64, f64) {
    let key_z = format!("pay:{}", proc);
    let tmp = format!("tmp:{}:{}:{}", proc, from_ms, to_ms);
    let _ : redis::RedisResult<i64> = redis::cmd("ZRANGESTORE").arg(&tmp).arg(&key_z).arg(from_ms).arg(to_ms).arg("BYSCORE").query_async(redis).await;
    let mut total_cents: i64 = 0;
    let mut count: i64 = 0;
    const CH: isize = 2048;
    let mut off: isize = 0;
    loop {
        let ids: Vec<String> = match redis.zrange(&tmp, off, off+CH-1).await { Ok(v)=>v, Err(_)=>vec![] };
        if ids.is_empty() { break; }
        let vals: Vec<Option<i64>> = match redis.hget("payamtc", &ids).await { Ok(v)=>v, Err(_)=>vec![] };
        for v in vals.into_iter().flatten() { total_cents = total_cents.saturating_add(v); }
        count += ids.len() as i64;
        off += ids.len() as isize;
    }
    let _ : Result<(), _> = redis.del(&tmp).await;
    (count as u64, (total_cents as f64)/100.0)
}


