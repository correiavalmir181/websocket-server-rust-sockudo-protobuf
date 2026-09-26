#!/bin/bash
set -e

# ============================================================================
# Teste de carga com 500 admins, usando o load-client v0.2 (100% Rust).
# Diferente do .sh antigo, NÃO precisamos mais chamar systemd-run aqui — o
# próprio load-client sobe o servidor, aplica CPUQuota/MemoryMax, mede o CPU
# real dele, e derruba tudo no final sozinho.
# ============================================================================

# --- ajuste estes três caminhos/valores conforme seu ambiente -------------
LOAD_CLIENT="./bench/load-client/target/release/load-client"
SERVER_BIN="/home/guiwolff/projects/admin/websocket-server-rust-sockudo-5-2/target/release/websocket-server"
URL="ws://127.0.0.1:8080"
# ---------------------------------------------------------------------------

ADMINS=100
CLIENTS_PER_ADMIN=1
DURATION=15
CPU_QUOTA="10%"
MEM_MAX="512M"
MSG_SIZE=1024
MAX_SIMULTANEOUS=3000   # precisa bater com o valor hardcoded no seu main.rs

TOTAL=$(( ADMINS * (CLIENTS_PER_ADMIN + 1) ))
if [ "$TOTAL" -ge "$MAX_SIMULTANEOUS" ]; then
    echo "⚠️  ADMINS ($ADMINS) x (CLIENTS_PER_ADMIN+1) = $TOTAL conexões,"
    echo "    isso é >= MAX_SIMULTANEOUS ($MAX_SIMULTANEOUS) do servidor."
    echo "    Reduza ADMINS/CLIENTS_PER_ADMIN antes de continuar."
    exit 1
fi
echo "Total de conexões no teste: $TOTAL (dentro do limite de $MAX_SIMULTANEOUS)"

# 500 admins x 2 sockets (admin+cliente) = 1000 conexões só deste processo.
# Sobe o limite de arquivos abertos pra não esbarrar no padrão de 1024 do
# Linux (ver diagnóstico anterior sobre os crashes com 200+/1000+ admins).
ulimit -n 65536

if [ ! -x "$LOAD_CLIENT" ]; then
    echo "❌ Não achei o binário em $LOAD_CLIENT — rode 'cargo build --release' antes."
    exit 1
fi
if [ ! -x "$SERVER_BIN" ]; then
    echo "❌ Não achei o binário do servidor em $SERVER_BIN — ajuste a variável SERVER_BIN no topo deste script."
    exit 1
fi

RESULT_FILE="resultado_$(date +%Y%m%d_%H%M%S).txt"
echo "Rodando: $ADMINS admins x $CLIENTS_PER_ADMIN cliente(s), ${DURATION}s, CPU=$CPU_QUOTA MEM=$MEM_MAX"
echo "Resultado sendo salvo em $RESULT_FILE"

# ⚡ --server-bin presente => o load-client sobe o servidor sozinho via
# systemd-run (precisa de sudo por causa disso), mede CPU real, e encerra
# tudo no final. Sem systemd-run manual aqui, sem trap de cleanup — o
# próprio binário já cuida disso (ver shutdown() no main.rs).
sudo "$LOAD_CLIENT" \
    --wsm \
    --url "$URL" \
    --server-bin "$SERVER_BIN" \
    --admins "$ADMINS" \
    --clients-per-admin "$CLIENTS_PER_ADMIN" \
    --duration "$DURATION" \
    --cpu-quota "$CPU_QUOTA" \
    --mem-max "$MEM_MAX" \
    --msg-size "$MSG_SIZE" \
    2>&1 | tee "$RESULT_FILE"

echo "Concluído. Resultado em $RESULT_FILE"
