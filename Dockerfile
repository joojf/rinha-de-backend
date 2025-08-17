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

FROM debian:bookworm-slim AS runtime-base
WORKDIR /app
RUN useradd -ms /bin/bash appuser
USER appuser

# Final image for the web app
FROM runtime-base AS runtime-app
COPY --from=build /app/target/release/rinha-de-backend /usr/local/bin/app
EXPOSE 9999
ENV BIND_ADDR=0.0.0.0:9999 \
    DEFAULT_URL=http://payment-processor-default:8080 \
    FALLBACK_URL=http://payment-processor-fallback:8080 \
    REQ_TIMEOUT_MS=120
CMD ["/usr/local/bin/app"]

# Final image for the memstore
FROM runtime-base AS runtime-memstore
COPY --from=build /app/target/release/memstore /usr/local/bin/memstore
ENV SOCKET_NAME=memstore
CMD ["/usr/local/bin/memstore"]
