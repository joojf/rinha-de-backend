use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct IngestIn {
    #[serde(rename = "correlationId")] pub correlation_id: Uuid,
    pub amount: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct JobPayload {
    #[serde(rename = "correlationId")] pub correlation_id: Uuid,
    pub amount: f64,
    #[serde(rename = "requestedAt")] pub requested_at: String,
}

#[derive(Deserialize)]
pub struct SummaryQuery { pub from: Option<String>, pub to: Option<String> }

#[derive(Serialize, Default)]
pub struct SummarySide { #[serde(rename = "totalRequests")] pub total_requests: u64, #[serde(rename = "totalAmount")] pub total_amount: f64 }

#[derive(Serialize, Default)]
pub struct SummaryOut { pub default: SummarySide, pub fallback: SummarySide }


