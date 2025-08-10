use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Estruturas de payload de entrada/saída

#[derive(Debug, Deserialize)]
pub struct PaymentIn {
    #[serde(rename = "correlationId")]
    pub correlation_id: Uuid,
    pub amount: f64,
}

#[derive(Debug, Serialize)]
pub struct ProcessorPayment<'a> {
    #[serde(rename = "correlationId")]
    pub correlation_id: &'a Uuid,
    pub amount: f64,
    #[serde(rename = "requestedAt")]
    pub requested_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobPayload {
    #[serde(rename = "correlationId")]
    pub correlation_id: Uuid,
    pub amount: f64,
    #[serde(rename = "requestedAt")]
    pub requested_at: String,
    #[serde(default)]
    pub attempts: u32,
}

#[derive(Debug, Deserialize)]
pub struct SummaryQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Serialize, Default)]
pub struct SummarySide {
    #[serde(rename = "totalRequests")]
    pub total_requests: u64,
    #[serde(rename = "totalAmount")]
    pub total_amount: f64,
}

#[derive(Debug, Serialize, Default)]
pub struct SummaryOut {
    pub default: SummarySide,
    pub fallback: SummarySide,
}

#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub failing: bool,
    #[serde(rename = "minResponseTime")]
    pub min_response_time: u64,
}
