FROM rust:1-bookworm AS build
WORKDIR /app

# Cache deps
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs && \
    cargo build --release && \
    rm -rf target/release/deps/rinha*

# Build
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
WORKDIR /app
RUN useradd -ms /bin/bash appuser
COPY --from=build /app/target/release/rinha-de-backend /usr/local/bin/app
COPY --from=build /app/target/release/memstore /usr/local/bin/memstore
USER appuser
EXPOSE 9999
ENV BIND_ADDR=0.0.0.0:9999 \
    DEFAULT_URL=http://payment-processor-default:8080 \
    FALLBACK_URL=http://payment-processor-fallback:8080 \
    REQ_TIMEOUT_MS=120
CMD ["/usr/local/bin/app"]
