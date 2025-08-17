use std::{collections::HashMap, env, fs, sync::Arc};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{UnixListener, UnixStream}, sync::RwLock};

type ResourceId = [u8; 16];
type ResourceBody = Vec<u8>;

#[derive(Clone)]
struct Database { inner: Arc<RwLock<HashMap<ResourceId, ResourceBody>>> }

impl Database {
    fn new() -> Self { Self { inner: Arc::new(RwLock::new(HashMap::new())) } }
}

#[derive(Debug, Clone, Copy)]
enum Operation { Get = 1, GetAll = 2, Set = 3, DeleteAll = 6 }

impl Operation { fn from_u8(v: u8) -> Option<Self> { match v {1=>Some(Self::Get),2=>Some(Self::GetAll),3=>Some(Self::Set),6=>Some(Self::DeleteAll), _=>None} } }

#[derive(Debug, Clone, Copy)]
enum Status { Ok=1, Created=2, NotFound=3, BadRequest=4 }

async fn read_message(stream: &mut UnixStream) -> anyhow::Result<(Operation, Option<ResourceId>, Option<ResourceBody>)> {
    let mut lenb=[0u8;4]; stream.read_exact(&mut lenb).await?; let len=u32::from_be_bytes(lenb) as usize;
    let mut buf=vec![0u8;len]; stream.read_exact(&mut buf).await?; if buf.is_empty(){anyhow::bail!("empty");}
    let op = Operation::from_u8(buf[0]).ok_or_else(|| anyhow::anyhow!("invalid op"))?;
    match op {
        Operation::GetAll | Operation::DeleteAll => Ok((op,None,None)),
        Operation::Get => { if buf.len()<17 { anyhow::bail!("len"); } let mut id=[0u8;16]; id.copy_from_slice(&buf[1..17]); Ok((op,Some(id),None)) },
        Operation::Set => {
            if buf.len()<17 { anyhow::bail!("len"); }
            let mut id=[0u8;16]; id.copy_from_slice(&buf[1..17]);
            let body = if buf.len()>17 { Some(buf[17..].to_vec()) } else { Some(Vec::new()) };
            Ok((op,Some(id),body))
        }
    }
}

async fn write_response(stream: &mut UnixStream, status: Status, payload: Option<&[u8]>) -> anyhow::Result<()> {
    let plen = payload.map_or(0, |p| p.len());
    let clen = 1 + plen;
    stream.write_all(&(clen as u32).to_be_bytes()).await?;
    stream.write_u8(status as u8).await?;
    if let Some(p)=payload { stream.write_all(p).await?; }
    stream.flush().await?;
    Ok(())
}

async fn handle_client(mut stream: UnixStream, db: Database) {
    loop {
        let msg = match read_message(&mut stream).await { Ok(m)=>m, Err(_)=>{ let _=write_response(&mut stream, Status::BadRequest, None).await; break; } };
        match msg {
            (Operation::GetAll,_,_) => {
                let map = db.inner.read().await; let mut payload=Vec::new();
                for (id,body) in map.iter() {
                    payload.extend_from_slice(id);
                    payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
                    payload.extend_from_slice(body);
                }
                let _ = write_response(&mut stream, Status::Ok, Some(&payload)).await;
            }
            (Operation::DeleteAll,_,_) => {
                db.inner.write().await.clear();
                let _ = write_response(&mut stream, Status::Ok, None).await;
            }
            (Operation::Get, Some(id), _) => {
                let map = db.inner.read().await;
                if let Some(b)=map.get(&id){ let _=write_response(&mut stream, Status::Ok, Some(b)).await; } else { let _=write_response(&mut stream, Status::NotFound, None).await; }
            }
            (Operation::Set, Some(id), Some(body)) => {
                let mut map = db.inner.write().await;
                let existed = map.insert(id, body).is_some();
                let status = if existed { Status::Ok } else { Status::Created };
                let _ = write_response(&mut stream, status, None).await;
            }
            _ => { let _ = write_response(&mut stream, Status::BadRequest, None).await; }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket_name = env::var("SOCKET_NAME").unwrap_or_else(|_| "memstore".to_string());
    let socket_path = format!("/tmp/{}.sock", socket_name);
    if fs::metadata(&socket_path).is_ok() { let _ = fs::remove_file(&socket_path); }
    let listener = UnixListener::bind(&socket_path)?;
    let mut perms = fs::metadata(&socket_path)?.permissions();
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o666);
    }
    fs::set_permissions(&socket_path, perms)?;
    println!("memstore listening on {}", socket_path);
    let db = Database::new();
    loop {
        let (stream, _) = listener.accept().await?;
        let dbx = db.clone();
        tokio::spawn(async move { handle_client(stream, dbx).await; });
    }
}

