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
    let dt = Utc.timestamp_opt(secs, 0).unwrap();
    format!("{}.{:03}Z", dt.format("%Y-%m-%dT%H:%M:%S"), millis)
}
