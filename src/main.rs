#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rinha_de_backend::start().await
}
