# syntax=docker/dockerfile:1

# ============================================================================
# STAGE 1: build
# ============================================================================
# sockudo-ws v2.0.1 exige edição 2024 (Rust 1.85+). "1-bookworm" é a tag
# oficial que a própria Docker mantém sempre apontando pra última versão
# estável 1.x — garante 1.85+ sem prender a uma patch version específica.
# Se quiser build 100% reprodutível, troque por uma tag exata (ex: 1.90-bookworm).
FROM rust:1-bookworm AS builder

WORKDIR /app

# ⚡ Cache de dependências: copia só os manifestos primeiro. Enquanto
# Cargo.toml/Cargo.lock não mudarem, o Docker reaproveita esta camada
# inteira (incluindo o clone+build do sockudo-ws via git) em rebuilds
# futuros, mesmo que o main.rs mude.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

# Agora copia o código de verdade. Só isso invalida o cache a partir daqui.
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release --locked

# ============================================================================
# STAGE 2: runtime
# ============================================================================
# Mesma base (bookworm) do builder -> mesma versão de glibc, evita erro
# "GLIBC_2.XX not found" por incompatibilidade entre estágios.
FROM debian:bookworm-slim

# ca-certificates: necessário pro reqwest (rustls-tls) validar o TLS do
# keepalive/self-ping de saída. curl: só pro HEALTHCHECK abaixo.
# --no-install-recommends + limpar apt lists mantém a imagem enxuta.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Usuário não-root — não há motivo pra esse processo rodar como root.
RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin appuser

WORKDIR /app
COPY --from=builder /app/target/release/websocket-server ./websocket-server
RUN chown appuser:appuser ./websocket-server

USER appuser

# A porta está hardcoded como 0.0.0.0:8080 no código atual (não lê $PORT).
# Se seu provedor injeta uma porta dinâmica (Heroku-style), o main.rs
# precisa ler std::env::var("PORT") antes disso funcionar de verdade.
EXPOSE 8080

# ⚡ Healthcheck usando o endpoint /health que o próprio servidor expõe.
# --start-period dá tempo do binário subir antes da primeira checagem falhar
# e o orquestrador reiniciar o container à toa.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Exec form: sinais (SIGTERM) vão direto pro processo (PID 1), sem passar
# por uma shell intermediária — encerramento limpo quando o orquestrador
# para o container.
ENTRYPOINT ["./websocket-server"]
