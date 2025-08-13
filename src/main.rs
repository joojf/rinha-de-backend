#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // no-op para disparar build/publish da imagem (ci)
    rinha_de_backend::start().await
}
