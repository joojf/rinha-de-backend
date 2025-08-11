use chrono::{DateTime, TimeZone, Utc};

pub fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn format_rfc3339_millis(ts: DateTime<Utc>) -> String {
    let secs = ts.timestamp();
    let nsec = ts.timestamp_subsec_nanos();
    let millis = nsec / 1_000_000;
    let dt = match Utc.timestamp_opt(secs, 0) {
        chrono::LocalResult::Single(dt) => dt,
        _ => return ts.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
    };
    format!("{}.{:03}Z", dt.format("%Y-%m-%dT%H:%M:%S"), millis)
}
