use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::UnixStream};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsPayload {
    #[serde(rename = "a")] pub amount: f64,
    #[serde(rename = "r")] pub requested_at_ms: i64,
    #[serde(rename = "p")] pub proc_name: MsProc,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MsProc { Default, Fallback }

#[derive(Clone)]
pub struct MemStoreClient { socket_path: String }

#[derive(Debug, Clone, Copy)]
enum Op { Get = 1, GetAll = 2, Set = 3, DeleteAll = 6 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MsStatus { Ok = 1, Created = 2, NotFound = 3, BadRequest = 4 }

impl MemStoreClient {
    pub fn new(socket_path: String) -> Self { Self { socket_path } }

    async fn send(&self, op: Op, id: Option<&[u8;16]>, body: Option<&[u8]>) -> anyhow::Result<(MsStatus, Option<Vec<u8>>)> {
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        let mut len = 1usize;
        match op { Op::Get | Op::Set => len += 16, Op::GetAll | Op::DeleteAll => {} }
        if matches!(op, Op::Set) { len += body.map(|b| b.len()).unwrap_or(0); }
        let mut buf = Vec::with_capacity(4+len);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.push(op as u8);
        if matches!(op, Op::Get | Op::Set) {
            let id = id.expect("id required");
            buf.extend_from_slice(id);
        }
        if let Some(b) = body { buf.extend_from_slice(b); }
        stream.write_all(&buf).await?;
        stream.flush().await?;
        let mut lb = [0u8;4];
        stream.read_exact(&mut lb).await?;
        let resp_len = u32::from_be_bytes(lb) as usize;
        let mut rb = vec![0u8; resp_len];
        stream.read_exact(&mut rb).await?;
        let status = match rb.get(0).copied().unwrap_or(4) {
            1 => MsStatus::Ok,
            2 => MsStatus::Created,
            3 => MsStatus::NotFound,
            _ => MsStatus::BadRequest,
        };
        let payload = if rb.len() > 1 { Some(rb[1..].to_vec()) } else { None };
        Ok((status, payload))
    }

    fn id16(id_hex: &str) -> anyhow::Result<[u8;16]> {
        let mut out = [0u8;16];
        let mut bytes = [0u8;32];
        let mut j = 0usize;
        for b in id_hex.bytes() {
            if b == b'-' { continue; }
            if j >= 32 { break; }
            bytes[j] = b; j += 1;
        }
        if j != 32 { anyhow::bail!("invalid uuid hex length"); }
        fn h(x: u8) -> anyhow::Result<u8> {
            match x {
                b'0'..=b'9' => Ok(x - b'0'),
                b'a'..=b'f' => Ok(10 + x - b'a'),
                b'A'..=b'F' => Ok(10 + x - b'A'),
                _ => anyhow::bail!("invalid hex"),
            }
        }
        for i in 0..16 {
            let hi = h(bytes[2*i])?; let lo = h(bytes[2*i+1])?;
            out[i] = (hi << 4) | lo;
        }
        Ok(out)
    }

    pub async fn get(&self, id_hex: &str) -> anyhow::Result<Option<MsPayload>> {
        let id = Self::id16(id_hex)?;
        let (st, data) = self.send(Op::Get, Some(&id), None).await?;
        match (st, data) {
            (MsStatus::Ok, Some(b)) => Ok(Some(serde_json::from_slice(&b)?)),
            (MsStatus::NotFound, _) => Ok(None),
            _ => Ok(None),
        }
    }

    pub async fn set(&self, id_hex: &str, payload: &MsPayload) -> anyhow::Result<bool> {
        let id = Self::id16(id_hex)?;
        let body = serde_json::to_vec(payload)?;
        let (st, _) = self.send(Op::Set, Some(&id), Some(&body)).await?;
        Ok(matches!(st, MsStatus::Created | MsStatus::Ok))
    }

    pub async fn get_all(&self) -> anyhow::Result<Vec<(String, MsPayload)>> {
        let (st, data) = self.send(Op::GetAll, None, None).await?;
        if !matches!(st, MsStatus::Ok) { return Ok(Vec::new()); }
        let mut out = Vec::new();
        if let Some(bytes) = data {
            let mut off = 0usize;
            while off + 20 <= bytes.len() {
                let id_bytes = &bytes[off..off+16];
                let id_str = String::from_utf8_lossy(id_bytes).trim_end_matches('\0').to_string();
                off += 16;
                let len = u32::from_be_bytes([bytes[off],bytes[off+1],bytes[off+2],bytes[off+3]]) as usize;
                off += 4;
                if off + len > bytes.len() { break; }
                let body = &bytes[off..off+len];
                off += len;
                if let Ok(payload) = serde_json::from_slice::<MsPayload>(body) {
                    out.push((id_str, payload));
                }
            }
        }
        Ok(out)
    }

    pub async fn purge(&self) -> anyhow::Result<()> { let _ = self.send(Op::DeleteAll, None, None).await?; Ok(()) }
}
