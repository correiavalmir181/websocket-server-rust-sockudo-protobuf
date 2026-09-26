// Servidor WebSocket SUPER OTIMIZADO V4 - SIMPLIFICADO
// O servidor apenas roteia mensagens entre admin e clientes
// Comandos: Admin envia JSON com {"type": "ls", "path": "/"} e cliente responde

use chrono::Local;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{RwLock, mpsc},
    time::sleep,
};
// ⚡ sockudo-ws no lugar de tokio-tungstenite/hyper-tungstenite.
// Message::Text agora carrega Bytes (zero-copy) em vez de String — todo
// ponto que fazia pattern-match em Message::Text(text) foi ajustado pra
// converter Bytes -> &str/String explicitamente (ver comentários abaixo).
use sockudo_ws::{Config as WsConfig, Message, WebSocketStream, handshake::generate_accept_key};
use serde_json::value::RawValue;

// ⚡ Log que só existe em build de debug. Em `cargo build --release`, o
// `#[cfg(debug_assertions)]` remove a chamada inteira do binário — não só o
// texto não é impresso, os argumentos nem chegam a ser formatados/avaliados.
// Isso importa porque `println!` trava a thread inteira (I/O bloqueante) num
// runtime single-thread, e tira lock global do stdout a cada chamada — em
// um handler chamado por mensagem (ex: send_to_client), isso custa mais que
// a própria serialização JSON da mensagem.
macro_rules! debug_log {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        println!($($arg)*);
    };
}

// Compila o logging detalhado para fora do hot path por padrão: sem format,
// timestamp ou alocação quando a feature não está habilitada.
#[cfg(feature = "activity-logging")]
macro_rules! activity_log {
    ($($arg:tt)*) => {
        log_activity_async(format_args!($($arg)*))
    };
}

#[cfg(not(feature = "activity-logging"))]
macro_rules! activity_log {
    ($($arg:tt)*) => {};
}
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use http_body_util::Full;
use hyper::body::Bytes;
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use arc_swap::ArcSwap;

// ⚡ Módulo de KeepAlive HTTP
mod keepalive;

// ⚡ Protocolo binário WSM: envelope protobuf + payload ZSTD.
// Ver src/proto.rs — o servidor só lê o envelope (type + clientId) e repassa
// o payload comprimido byte a byte. Nunca descomprime no roteamento.
mod proto;
// Trait `Message` (decode/encode/encoded_len) precisa estar no scope pra
// chamar `proto::AdminEnvelope::decode(...)` aqui no main.rs.
use prost::Message as _;

// ⚡ Instrumentação temporária de profiling (feature `profile`)
#[cfg(feature = "profile")]
mod profile;

const PORT: u16 = 8080;
const ADMIN_PASSWORD: &str = "admin123";
const IDENTIFICATION_TIMEOUT: Duration = Duration::from_secs(10);
const LOG_FILE: &str = "./file_manager_server.log";

// ⚡ OTIMIZAÇÃO: Constantes para bounded channels e logging
//
// Buffer do canal por conexão. Era 32 — estourava facil sob carga (500 admins
// x comandos de 1KB), enchia, e o `try_send` DESCARTAVA comandos silenciosamente.
// Agora o drop não existe mais (backpressure real), mas um buffer maior ainda
// ajuda a absorver BURST (cliente em 3G que trava 200ms e volta). 256 mensagens
// por conexão = no pior caso ~256KB por conexão ativa; com 3000 conexões isso
// é 768MB no pior caso teórico — na prática raramente passa de algumas dezenas
// porque o backpressure freia o remetente ANTES de encher.
const CHANNEL_BUFFER_SIZE: usize = 256;        // Absorve burst de clientes lentos
// ⚡ Limite de quanto esperamos um receptor lento antes de desistir. Com
// backpressure, o remetente é freado (correto); mas não podemos esperar pra
// sempre num cliente que travou de vez — senão o admin fica bloqueado
// permanentemente. 30s é generoso pra rede móvel e ainda detecta cliente morto.
const SEND_TIMEOUT_SECS: u64 = 30;             // Backpressure: espera antes de descartar
#[cfg(feature = "activity-logging")]
const LOG_BUFFER_SIZE: usize = 1000;           // Buffer de logs em memória
const HEARTBEAT_INTERVAL_SECS: u64 = 30;       // Reduzir writes de heartbeat

// ⚡ Limpeza de clientes órfãos: um cliente que se conecta mas cujo admin
// nunca aparece fica ocupando uma vaga de conexão (das MAX_SIMULTANEOUS)
// pra sempre, à toa. Se o admin dele nunca esteve presente dentro dessa
// janela de tolerância, o servidor derruba a conexão pra liberar a vaga.
// IMPORTANTE: uma vez que o admin apareça (mesmo que só um instante), o
// cliente fica "validado" permanentemente e nunca mais é checado por essa
// rotina — isso evita derrubar clientes legítimos só porque o admin fechou
// o painel por um tempo depois de já ter estado online.
const ORPHAN_CLIENT_TIMEOUT_SECS: u64 = 300;   // 5 minutos de tolerância
const ORPHAN_SWEEP_INTERVAL_SECS: u64 = 30;    // checa a cada 30s

// ⚡ Respostas fixas do command_result. O payload é idêntico ao que
// serde_json::to_vec(CommandResult{...}) produziria, mas já vem pronto como
// &'static str: Bytes::from_static NÃO aloca (aponta pro .rodata) e pula a
// serialização por mensagem. O cliente admin lê success/message normalmente.
const COMMAND_RESULT_OK: &str =
    r#"{"type":"command_result","success":true,"message":"Comando enviado ao cliente"}"#;
// Mensagens (sem aspas) usadas na resposta protobuf do command_result —
// a serialização protobuf de String copia esses bytes uma única vez.
const COMMAND_RESULT_OK_MSG: &str = "Comando enviado ao cliente";
const COMMAND_RESULT_FAIL_MSG: &str = "Falha ao enviar comando ao cliente";
const COMMAND_RESULT_FAIL: &str =
    r#"{"type":"command_result","success":false,"message":"Falha: cliente não encontrado ou não autorizado"}"#;

// ⚡ Frames WSM do command_result PRÉ-COMPUTADOS (zero alocação por mensagem).
//
// O protobuf de uma resposta de sucesso/fracasso é sempre idêntico (type,
// success e message fixos). Em vez de montar a struct + alocar String + alocar
// Vec + serializar a cada mensagem, calculamos os bytes UMA VEZ no startup e
// guardamos como Bytes ref-countado: enviar é só incrementar um contador
// atômico (clone), igual ao caminho JSON com Bytes::from_static.
//
// Sem isto, a resposta WSM era a parte mais cara do hot path (profile: reply
// 1565µs vs 838µs do JSON) — três alocações por mensagem só pra dizer "ok".
static WSM_COMMAND_RESULT_OK: Lazy<hyper::body::Bytes> = Lazy::new(|| {
    hyper::body::Bytes::from(proto::encode_command_result(true, COMMAND_RESULT_OK_MSG))
});
static WSM_COMMAND_RESULT_FAIL: Lazy<hyper::body::Bytes> = Lazy::new(|| {
    hyper::body::Bytes::from(proto::encode_command_result(false, COMMAND_RESULT_FAIL_MSG))
});

// ============================================================================
// CONFIGURAÇÃO OTIMIZADA PARA POUCOS RECURSOS (0.1 CPU + 512MB RAM)
// ============================================================================
const MAX_CONNECTIONS: usize = 8000; //14000      // Conexões totais permitidas
const MAX_SIMULTANEOUS: usize = 3000; //6000        // Conexões ativas simultâneas
const NUM_SHARDS: usize = 6;                // Ponto de equilíbrio para poucos recursos
const BATCH_SIZE: usize = 64;               // Amortiza yields sem monopolizar o runtime

// ============================================================================
// CONTADORES LOCK-FREE
// ============================================================================

static TOTAL_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_CLIENTS: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_ADMINS: AtomicUsize = AtomicUsize::new(0);
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

// Semáforo para limitar conexões simultâneas (controle agressivo)
static CONNECTION_SEMAPHORE: Lazy<tokio::sync::Semaphore> =
    Lazy::new(|| tokio::sync::Semaphore::new(MAX_SIMULTANEOUS));

// ⚡ OTIMIZAÇÃO: Mapa global de admins para lookup O(1)
static GLOBAL_ADMIN_MAP: Lazy<Arc<RwLock<HashMap<String, mpsc::Sender<Message>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

// ⚡ OTIMIZAÇÃO: Canal para logging assíncrono
#[cfg(feature = "activity-logging")]
static LOG_CHANNEL: Lazy<(mpsc::UnboundedSender<String>, Arc<RwLock<Option<mpsc::UnboundedReceiver<String>>>>)> =
    Lazy::new(|| {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, Arc::new(RwLock::new(Some(rx))))
    });

// ⚡ KeepAlive Manager global (lazy - só cria quando necessário)
static KEEPALIVE_MANAGER: Lazy<Arc<keepalive::KeepAliveManager>> =
    Lazy::new(|| Arc::new(keepalive::KeepAliveManager::new(PORT)));

// ⚡ Timestamp RFC3339 em cache. Local::now().to_rfc3339() custa uma syscall
// + lookup de fuso + formatação + alocação de String por mensagem; o cliente
// recebe a mesma string, atualizada em background a cada 500ms (granularidade
// de meio segundo é irrelevante pra um file manager remoto). Leitura por
// mensagem = 1 atomic load (clone de Arc), zero alocação.
static CACHED_TIMESTAMP: Lazy<ArcSwap<String>> =
    Lazy::new(|| ArcSwap::from_pointee(String::new()));

/// Pega o timestamp em cache (clone de Arc, ~nanos).
fn cached_timestamp() -> Arc<String> {
    CACHED_TIMESTAMP.load_full()
}

/// Task de background: atualiza o timestamp a cada 500ms.
async fn timestamp_refresh_task() {
    loop {
        let ts = Local::now().to_rfc3339();
        CACHED_TIMESTAMP.store(Arc::new(ts));
        sleep(Duration::from_millis(500)).await;
    }
}

fn allocate_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

// Verifica se deve rejeitar novas conexões (75% da capacidade)
fn should_reject_connection() -> bool {
    TOTAL_CONNECTIONS.load(Ordering::Relaxed) > MAX_CONNECTIONS * 75 / 100
}

// ============================================================================
// ESTRUTURAS SIMPLES DE MENSAGENS
// ============================================================================

// Mensagens que o ADMIN envia para o SERVIDOR
// ⚡ Estrutura PLANA (não enum com #[serde(tag)]) por dois motivos:
//   1. Enum com tag internamente faz o serde bufferizar o objeto inteiro num
//      `Content` antes de despachar — é a forma mais lenta de despachar e,
//      pior, `Content` NÃO preserva RawValue (dá erro de parse em runtime,
//      não em compile). Estrutura plana faz UMA única passada no JSON.
//   2. `kind: &str` com #[serde(borrow)] é zero-copy: aponta direto pro
//      buffer de input, sem alocação de String.
// `message: Box<RawValue>` captura o JSON aninhado cru — sem desserializar
// a árvore (Value) — e é reemitido pro cliente byte a byte, sem serializar
// de novo.
#[derive(Debug, Deserialize)]
struct AdminCommandRaw<'a> {
    #[serde(rename = "type", borrow)]
    kind: &'a str,
    #[serde(rename = "clientId")]
    client_id: Option<u32>,
    message: Option<Box<RawValue>>,
}

// Comandos válidos (snake_case, como o cliente envia).
const CMD_SEND_TO_CLIENT: &str = "send_to_client";
const CMD_BROADCAST: &str = "broadcast_to_clients";
const CMD_LIST_CLIENTS: &str = "list_clients";
const CMD_LIST_ADMINS: &str = "list_admins";
const CMD_SERVER_STATUS: &str = "server_status";
const CMD_KICK: &str = "kick_client";
const CMD_GET_STATE: &str = "get_client_state";
const CMD_PING: &str = "ping";

// Mensagens que o CLIENTE envia para o SERVIDOR
#[derive(Debug, Deserialize)]
struct ClientConnectMessage {
    #[serde(rename = "type")]
    _msg_type: String,
    data: Option<String>,
    #[serde(rename = "adminId")]
    admin_id: Option<String>,
    #[serde(rename = "androidId")]
    android_id: Option<String>,
    wallpaper: Option<String>,
}

// ============================================================================
// ESTRUTURAS ULTRA-COMPACTAS (12 bytes) - OTIMIZADO PARA POUCOS RECURSOS
// ============================================================================

#[repr(C, packed)]  // packed para ZERO padding
struct CompactConnection {
    id: u32,                    // 4 bytes - ID da conexão
    admin_id_hash: u32,         // 4 bytes - Hash do admin_id
    last_activity: u16,         // 2 bytes - Segundos desde startup (65535s = 18h)
    flags: u8,                  // 1 byte - is_admin(1) | is_alive(1) | has_admin(1) | reserved(5)
    wallpaper_idx: u8,          // 1 byte - Índice no pool de wallpapers
}
// Tamanho total: 12 bytes por conexão!

impl CompactConnection {
    fn new(id: u32, admin_id_hash: u32, wallpaper_idx: u8, is_admin: bool) -> Self {
        let mut flags = 0b0000_0010; // is_alive
        if is_admin {
            flags |= 0b0000_0001;
        }
        if admin_id_hash != 0 {
            flags |= 0b0000_0100; // has_admin
        }

        Self {
            id,
            admin_id_hash,
            last_activity: 0,
            flags,
            wallpaper_idx,
        }
    }

    fn is_alive(&self) -> bool {
        (self.flags & 0b0000_0010) != 0
    }

    fn update_activity(&mut self, timestamp: u16) {
        self.last_activity = timestamp;
    }
}

// ============================================================================
// METADATA E SHARDS
// ============================================================================

struct StringPool {
    strings: Vec<String>,
    string_to_idx: HashMap<String, u8>,
}

impl StringPool {
    fn new() -> Self {
        Self {
            strings: Vec::with_capacity(256),
            string_to_idx: HashMap::with_capacity(256),
        }
    }

    fn get_or_insert(&mut self, s: &str) -> Option<u8> {
        self.string_to_idx.get(s).copied().or_else(|| {
            if self.strings.len() >= 255 {
                None
            } else {
                let idx = self.strings.len() as u8;
                self.strings.push(s.to_string());
                self.string_to_idx.insert(s.to_string(), idx);
                Some(idx)
            }
        })
    }

    fn get(&self, idx: u8) -> Option<&String> {
        self.strings.get(idx as usize)
    }
}

struct ClientMetadata {
    addr: SocketAddr,
    device_type: String,
    android_id: String,
    connected_at: Instant,
    admin_id: Option<String>,
    // ⚡ Hash do admin guardada no metadata: send_to_client faz UMA única
    // lookup (client_metadata) em vez de duas (metadata + CompactConnection).
    admin_id_hash: u32,
    sender: mpsc::Sender<Message>,  // ⚡ BOUNDED channel
    last_heartbeat_update: Instant, // ⚡ Para reduzir writes
    admin_seen: bool,               // ⚡ true assim que o admin dele aparecer alguma vez
}

struct AdminMetadata {
    addr: SocketAddr,
    connected_at: Instant,
    admin_id: String,
}

struct ConnectionShard {
    start_time: Instant,
    connections: Vec<CompactConnection>,
    // ⚡ OTIMIZAÇÃO: FxHashMap em vez de HashMap padrão (SipHash) para chaves u32.
    // As chaves aqui são IDs internos alocados pelo servidor (não são strings
    // controladas por um atacante), então não há motivo para pagar o custo do
    // SipHash "DoS-resistant" — FxHash é várias vezes mais rápido para inteiros
    // e essas tabelas são consultadas a cada mensagem roteada.
    connection_indices: FxHashMap<u32, usize>, // id -> índice (O(1) lookup!)
    client_metadata: FxHashMap<u32, ClientMetadata>,
    admin_metadata: FxHashMap<u32, AdminMetadata>,
    admin_clients: FxHashMap<u32, Vec<u32>>, // admin_hash -> lista de clientes
    wallpaper_pool: StringPool, // Pool de wallpapers compartilhado
}

impl ConnectionShard {
    fn new(start_time: Instant) -> Self {
        Self {
            start_time,
            connections: Vec::with_capacity(MAX_CONNECTIONS / NUM_SHARDS),
            connection_indices: FxHashMap::with_capacity_and_hasher(MAX_CONNECTIONS / NUM_SHARDS, Default::default()),
            client_metadata: FxHashMap::with_capacity_and_hasher(MAX_CONNECTIONS / NUM_SHARDS, Default::default()),
            admin_metadata: FxHashMap::with_capacity_and_hasher(10, Default::default()),
            admin_clients: FxHashMap::with_capacity_and_hasher(100, Default::default()),
            wallpaper_pool: StringPool::new(),
        }
    }

    fn current_timestamp(&self) -> u16 {
        // Retorna segundos desde startup (u16 = 65535s = ~18h)
        self.start_time.elapsed().as_secs().min(u16::MAX as u64) as u16
    }

    // ⚡ OTIMIZAÇÃO: Acesso O(1) usando índice
    fn get_connection_mut(&mut self, id: u32) -> Option<&mut CompactConnection> {
        if let Some(&index) = self.connection_indices.get(&id) {
            self.connections.get_mut(index)
        } else {
            None
        }
    }

    fn get_connection(&self, id: u32) -> Option<&CompactConnection> {
        if let Some(&index) = self.connection_indices.get(&id) {
            self.connections.get(index)
        } else {
            None
        }
    }

    // Adiciona conexão com índice
    fn add_connection(&mut self, conn: CompactConnection) {
        let id = conn.id;
        let index = self.connections.len();
        self.connections.push(conn);
        self.connection_indices.insert(id, index);
    }

    // Remove conexão e atualiza índices
    fn remove_connection(&mut self, id: u32) -> Option<CompactConnection> {
        if let Some(index) = self.connection_indices.remove(&id) {
            let removed = self.connections.swap_remove(index);

            // Se não foi o último, atualizar índice do elemento movido
            if index < self.connections.len() {
                let moved_id = self.connections[index].id;
                self.connection_indices.insert(moved_id, index);
            }

            Some(removed)
        } else {
            None
        }
    }
}

static CONN_SHARDS: Lazy<Vec<Arc<RwLock<ConnectionShard>>>> = Lazy::new(|| {
    let start_time = Instant::now();
    (0..NUM_SHARDS)
        .map(|_| Arc::new(RwLock::new(ConnectionShard::new(start_time))))
        .collect()
});

fn get_shard(id: u32) -> &'static Arc<RwLock<ConnectionShard>> {
    &CONN_SHARDS[id as usize % NUM_SHARDS]
}

fn hash_admin_id(admin_id: &str) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    admin_id.hash(&mut hasher);
    hasher.finish() as u32
}

// ⚡ sockudo-ws: usamos o WebSocketStream NÃO-split. Antes chamávamos
// ws.split() e isso criava, POR CONEXÃO, um task driver dedicado para
// escritas; cada SplitWriter::send() alocava um oneshot channel, acordava o
// driver, esperava ele escrever no socket e responder (round-trip entre
// tasks). Em um runtime single-thread saturado com centenas de conexões,
// isso significava várias ativações de task + alocações por mensagem.
//
// Com o stream não-split, UM task por conexão faz leitura E escrita:
// select! entre rx.recv() e stream.next(); o envio é só encode no buffer
// interno + write no socket, tudo na mesma task, sem oneshot e sem troca de
// task. O stream continua respondendo Pings automaticamente e continua
// honrando ping_interval/pong_timeout/idle_timeout (tudo dentro do poll).
type WsIo = hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>;
type WsStream = sockudo_ws::WebSocketStream<WsIo>;

use futures_util::{SinkExt as _, StreamExt as _};

// ============================================================================
// MAIN SERVER
// ============================================================================
//#[tokio::main(flavor = "multi_thread", worker_threads = 2)] // às vezes ajuda em picos raros
#[tokio::main(flavor = "current_thread")]  // ⚡ SINGLE THREAD para 0.1 CPU!
async fn main() {
    let start_time = Instant::now();

    // Logging detalhado é opt-in para não consumir CPU no caminho crítico.
    #[cfg(feature = "activity-logging")]
    tokio::spawn(log_writer_task());

    // ⚡ Reporter de profiling (feature `profile`)
    #[cfg(feature = "profile")]
    tokio::spawn(profile::reporter_task());

    // ⚡ Mantém o timestamp RFC3339 em cache (ver CACHED_TIMESTAMP)
    tokio::spawn(timestamp_refresh_task());

    // ⚡ Limpeza periódica de clientes órfãos (admin nunca conectou)
    tokio::spawn(orphan_cleanup_task());

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT))
        .await
        .expect("Falha ao bind na porta");

    println!("\n🚀 Remote File Manager Server V5 - SUPER OTIMIZADO");
    println!("📡 Escutando em ws://0.0.0.0:{}", PORT);
    println!("📊 Memória/conexão: {} bytes | Shards: {} | Max Simultâneas: {}",
             std::mem::size_of::<CompactConnection>(), NUM_SHARDS, MAX_SIMULTANEOUS);
    println!("⚙️  Config: Single-thread + Bounded channels | Activity logging: {}",
             if cfg!(feature = "activity-logging") { "ON" } else { "OFF" });
    println!("⚡ Otimizações: O(1) admin lookup + Reduced writes + Cooperative yielding");

    // ⚡ Iniciar KeepAlive (detecta automaticamente se é dev ou prod)
    let keepalive = Arc::clone(&KEEPALIVE_MANAGER);
    tokio::spawn(async move {
        sleep(Duration::from_secs(30)).await; // Aguarda servidor estabilizar
        keepalive.start().await;
    });

    println!("⚡ Aguardando conexões...\n");

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                log_critical(&format!("Erro ao aceitar conexão: {}", e));
                continue;
            }
        };

        // WebSocket usa muitos frames pequenos; evita atrasos do algoritmo de Nagle.
        if let Err(e) = stream.set_nodelay(true) {
            log_critical(&format!("Falha ao habilitar TCP_NODELAY para {}: {}", addr, e));
        }

        // Rejeita se servidor sobrecarregado (75% da capacidade)
        if should_reject_connection() {
            log_critical(&format!("Servidor sobrecarregado, rejeitando conexão de {}", addr));
            drop(stream); // Fecha imediatamente
            continue;
        }

        tokio::spawn(async move {
            let io = TokioIo::new(stream);

            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| handle_http_request(req, addr, start_time)),
                )
                .with_upgrades()
                .await
            {
                let err_str = e.to_string();
                if !err_str.contains("connection closed") && !err_str.contains("broken pipe") {
                    log_critical(&format!("Erro HTTP de {}: {}", addr, e));
                }
            }
        });
    }
}

async fn handle_http_request(
    req: Request<Incoming>,
    addr: SocketAddr,
    server_start: Instant,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {

    let path = req.uri().path();

    // ⚡ Endpoint de KeepAlive (apenas /health)
    if path == "/health" {
        let response = keepalive::get_health_response();
        let json = serde_json::to_string(&response).unwrap();

        return Ok(Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(json)))
            .unwrap());
    }

    // ⚡ sockudo-ws não faz o parsing HTTP/upgrade sozinho (isso continua
    // sendo trabalho do hyper) — ele só assume a partir do socket já
    // "upgraded". Fazemos manualmente o que o hyper_tungstenite fazia,
    // seguindo o exemplo Axum da própria documentação do sockudo-ws.
    let is_ws_upgrade = req
        .headers()
        .get(hyper::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
        && req
            .headers()
            .get(hyper::header::CONNECTION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_lowercase().contains("upgrade"))
            .unwrap_or(false);

    if is_ws_upgrade {
        let sec_key = req
            .headers()
            .get("sec-websocket-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let Some(sec_key) = sec_key else {
            return Ok(Response::builder()
                .status(400)
                .body(Full::new(Bytes::from("Missing Sec-WebSocket-Key")))
                .unwrap());
        };
        let accept_key = generate_accept_key(&sec_key);

        tokio::spawn(async move {
            match hyper::upgrade::on(req).await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    // ⚡ sockudo-ws 2.0.1: config ajustada pra rodar MUITAS conexões
                    // (MAX_CONNECTIONS = 8000) num ambiente com pouca RAM.
                    //   - max_payload_length: nosso protocolo é JSON pequeno (comandos,
                    //     listagem de arquivos). 2MB de teto é folga generosa e evita
                    //     que o default de 64MB seja usado pra estourar memória por
                    //     conexão (malícia ou bug do cliente).
                    //   - max_backpressure: se o consumidor (admin ou cliente) travar,
                    //     não deixamos a fila de escrita crescer além de 256KB por
                    //     conexão antes de derrubar — em 512MB de RAM totais isso
                    //     importa muito mais do que numa VM com dezenas de GB livres.
                    //   - ping_interval/pong_timeout/idle_timeout: no 2.0.1 esses
                    //     valores passaram a FECHAR a conexão de verdade quando o peer
                    //     some sem mandar Close (comum em app Android que perde rede
                    //     ou é morto pelo sistema) — antes disso a conexão ficava
                    //     "zumbi" presa no shard consumindo memória até o TCP dar
                    //     timeout do SO. 30s/10s casa com o HEARTBEAT_INTERVAL_SECS
                    //     que já usamos na lógica de heartbeat manual do app.
                    /*let ws_config = WsConfig::builder()
                        .max_payload_length(2 * 1024 * 1024) //2 * 1024 * 1024
                        .max_backpressure(256 * 1024)
                        .ping_interval(30)
                        .pong_timeout(10)
                        // Deve ser maior que ping_interval + pong_timeout; caso
                        // contrário o hard timeout vence antes do Pong.
                        .idle_timeout(120)
                        .close_timeout(5)
                        .build();*/
                    let ws_config = WsConfig::default();
                    let ws = WebSocketStream::server(io, ws_config);
                    if let Err(e) = handle_websocket(ws, addr, server_start).await {
                        eprintln!("Erro WebSocket de {}: {}", addr, e);
                    }
                }
                Err(e) => eprintln!("Erro ao fazer upgrade WebSocket de {}: {}", addr, e),
            }
        });

        Ok(Response::builder()
            .status(101)
            .header(hyper::header::UPGRADE, "websocket")
            .header(hyper::header::CONNECTION, "Upgrade")
            .header("Sec-WebSocket-Accept", accept_key)
            .body(Full::new(Bytes::new()))
            .unwrap())
    } else {
        // Página inicial simples
        let html = format!(
            r#"<!DOCTYPE html>
<html><head><title>File Manager Server V5</title></head>
<body style="font-family: Arial; margin: 50px;">
<h1>🚀 File Manager Server V5 - SUPER OTIMIZADO</h1>
<p style="color: green;">✅ Server is running</p>
<div style="background: #f0f0f0; padding: 15px; border-radius: 5px;">
<h3>📊 Server Status</h3>
<p><strong>WebSocket URL:</strong> ws://{}:{}</p>
<p><strong>Uptime:</strong> {}s</p>
<p><strong>Total Connections:</strong> {}</p>
<p><strong>Clients:</strong> {}</p>
<p><strong>Admins:</strong> {}</p>
<p><strong>KeepAlive:</strong> 🏓 Ativo (ping a cada 8 min)</p>
</div>
<div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin-top: 20px;">
<h3>📡 Endpoints</h3>
<p><strong>Health Check:</strong> <a href="/health">/health</a></p>
</div>
</body></html>"#,
            addr.ip(), PORT,
            server_start.elapsed().as_secs(),
            TOTAL_CONNECTIONS.load(Ordering::Relaxed),
            ACTIVE_CLIENTS.load(Ordering::Relaxed),
            ACTIVE_ADMINS.load(Ordering::Relaxed)
        );

        Ok(Response::new(Full::new(Bytes::from(html))))
    }
}

async fn handle_websocket(
    mut stream: WsStream,
    addr: SocketAddr,
    server_start: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    // Adquirir semáforo (controle de conexões simultâneas)
    let _permit = match CONNECTION_SEMAPHORE.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            // Servidor sobrecarregado, responde rápido com erro
            let _ = stream.send(Message::text(
                serde_json::json!({
                    "type": "error",
                    "message": "Servidor sobrecarregado, tente novamente em alguns segundos"
                }).to_string()
            )).await;
            log_critical("Conexão rejeitada: limite de conexões simultâneas atingido");
            return Ok(());
        }
    };

    // Aguardar identificação
    // ⚡ sockudo-ws: Message::Text carrega Bytes, não String — convertemos
    // com from_utf8_lossy (nunca falha, troca bytes inválidos por replacement
    // char em vez de derrubar a conexão).
    let init_msg = match tokio::time::timeout(IDENTIFICATION_TIMEOUT, stream.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => String::from_utf8_lossy(&text).into_owned(),
        _ => {
            let _ = stream.send(Message::text(
                serde_json::json!({"type": "error", "message": "Timeout: Identificação não recebida"}).to_string()
            )).await;
            return Ok(());
        }
    };

    // Verificar se é admin ou cliente
    let init_json: serde_json::Value = match serde_json::from_str(&init_msg) {
        Ok(j) => j,
        Err(_) => {
            return handle_client_connection(init_msg, stream, addr).await;
        }
    };

    if init_json["type"] == "admin_auth" {
        // Verificar senha do admin
        if init_json["password"].as_str() == Some(ADMIN_PASSWORD) {
            handle_admin_connection(init_msg, stream, addr, server_start).await
        } else {
            let _ = stream.send(Message::text(
                serde_json::json!({"type": "error", "message": "Senha incorreta"}).to_string()
            )).await;
            Ok(())
        }
    } else {
        handle_client_connection(init_msg, stream, addr).await
    }
}

// ============================================================================
// HANDLER DE ADMIN (SIMPLES)
// ============================================================================

async fn handle_admin_connection(
    init_msg: String,
    mut stream: WsStream,
    addr: SocketAddr,
    server_start: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct AdminAuth {
        #[serde(rename = "adminId")]
        admin_id: String,
        #[serde(rename = "password")]
        _password: String,
    }

    let auth: AdminAuth = serde_json::from_str(&init_msg)?;
    let admin_unique_id = auth.admin_id;
    let admin_id_hash = hash_admin_id(&admin_unique_id);

    // ⚡ BOUNDED channel para evitar OOM
    let (tx, mut rx) = mpsc::channel::<Message>(CHANNEL_BUFFER_SIZE);

    let admin_id = allocate_id();
    let shard = get_shard(admin_id);

    // ⚡ Registrar no mapa global ANTES do shard
    {
        let mut global_map = GLOBAL_ADMIN_MAP.write().await;
        global_map.insert(admin_unique_id.clone(), tx);
    }

    {
        let mut shard = shard.write().await;

        // Adicionar conexão compacta - ⚡ OTIMIZADO: com índice
        let conn = CompactConnection::new(admin_id, admin_id_hash, 0, true);
        shard.add_connection(conn);

        // Adicionar metadata
        shard.admin_metadata.insert(admin_id, AdminMetadata {
            addr,
            connected_at: Instant::now(),
            admin_id: admin_unique_id.clone(),
        });
    }

    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    ACTIVE_ADMINS.fetch_add(1, Ordering::Relaxed);

    // ⚡ Limpeza de órfãos: se já existem clientes esperando por este admin
    // (conectaram antes dele), valida todos de uma vez — nunca mais serão
    // checados pela rotina de limpeza de órfãos, mesmo que este admin saia
    // depois.
    mark_admin_seen_for_existing_clients(admin_id_hash).await;

    debug_log!("Admin {} ({}) conectado", admin_id, admin_unique_id);
    activity_log!("ADMIN_CONNECTED: {} ({})", admin_id, admin_unique_id);

    // Enviar welcome
    // ⚡ Serialização direta de struct + timestamp do cache: sem árvore
    // Value, sem chrono, sem alocação de String de timestamp.
    let ts = cached_timestamp();
    let welcome = AdminWelcomeMsg {
        message_type: "admin_welcome",
        admin_id,
        admin_unique_id: &admin_unique_id,
        timestamp: ts.as_str(),
    };
    let _ = stream.send(Message::text(serde_json::to_string(&welcome).unwrap_or_default())).await;

    // ⚡ Loop principal do admin — UM task por conexão, sem driver separado:
    // select! entre a fila de saída (notificações de clientes) e o socket.
    // O futuro de cada ramo só existe enquanto o outro não está em uso, então
    // o borrow checker aceita os dois &mut stream/rx.
    loop {
        // ⚡ profiling: CPU de UMA iteração inteira do select! (todos os
        // await points incluídos — é a conta que importa).
        #[cfg(feature = "profile")]
        let _loop_scope = profile::Scope::new(&profile::T_ADMIN_LOOP);

        tokio::select! {
            // Mensagens para enviar ao admin (notificações de clientes) - ENVIO IMEDIATO
            msg = rx.recv() => {
                match msg {
                    Some(message) => {
                        // ⚡ Escrita direta no socket: encode no buffer interno
                        // + write, sem oneshot channel e sem driver round-trip.
                        if stream.send(message).await.is_err() {
                            activity_log!("Admin {} desconectado (send failed)", admin_id);
                            break;
                        }
                    }
                    None => break,
                }
            }

            // Mensagens recebidas do admin
            msg = stream.next() => {
                #[cfg(feature = "profile")]
                let _read_scope = profile::Scope::new(&profile::T_READ);
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // ⚡ sockudo-ws: Bytes -> &str (mensagens de admin sempre
                        // devem ser JSON válido/UTF-8; se não forem, ignoramos
                        // em vez de derrubar a conexão)
                        if let Ok(text_str) = std::str::from_utf8(&text) {
                            handle_admin_message(admin_id, admin_id_hash, text_str, &mut stream, server_start).await;
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        // ⚡ Protocolo WSM: envelope protobuf + payload ZSTD.
                        // O admin (app React Native novo) fala este protocolo;
                        // o payload (comando pro cliente) é repassado CRU, sem
                        // descompressão. Ver src/proto.rs.
                        handle_admin_binary(admin_id, admin_id_hash, &bin, &mut stream, server_start).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // Atualizar heartbeat - ⚡ OTIMIZADO: O(1) lookup
                        let shard = get_shard(admin_id);
                        let mut shard = shard.write().await;
                        if let Some(conn) = shard.get_connection_mut(admin_id) {
                            conn.flags |= 0b0000_0010;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        // 🔴 Responde o Close handshake e termina LIMPAMENTE.
                        // Sem este branch o frame caía no catch-all abaixo e o
                        // socket era dropado sem responder — kernel punia com
                        // 60s de TIME-WAIT por socket, acumulando entre runs
                        // do benchmark até centenas de conexões falharem.
                        let _ = stream.send(Message::Close(None)).await;
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        }
    }

    cleanup_admin(admin_id).await;
    Ok(())
}

async fn handle_admin_message(
    admin_id: u32,
    admin_id_hash: u32,
    text: &str,
    stream: &mut WsStream,
    server_start: Instant,
) {
    // ⚡ profiling
    #[cfg(feature = "profile")]
    let _scope_total = profile::Scope::new(&profile::T_ADMIN_TOTAL);
    #[cfg(feature = "profile")]
    profile::N_ADMIN_MSG.fetch_add(1, Ordering::Relaxed);

    // Parse da mensagem do admin — UMA passada, tag emprestada, message cru.
    let cmd: AdminCommandRaw = {
        #[cfg(feature = "profile")]
        let _s = profile::Scope::new(&profile::T_PARSE);
        match serde_json::from_str(text) {
            Ok(c) => c,
            Err(_) => {
                let _ = stream.send(Message::text(
                    serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
                )).await;
                return;
            }
        }
    };

    activity_log!("ADMIN_COMMAND: {} - {:?}", admin_id, cmd);

    match cmd.kind {
        // Comando PRINCIPAL: enviar mensagem para cliente
        // ⚡ HOT PATH: o JSON aninhado já está cru (RawValue) — só copiamos
        // os bytes pro canal do cliente, sem desserializar Value e sem
        // serializar de novo. A resposta é uma string estática (zero
        // serialização, zero alocação: Bytes::from_static aponta pro .rodata).
        CMD_SEND_TO_CLIENT => {
            let (Some(client_id), Some(message)) = (cmd.client_id, cmd.message) else {
                let _ = stream.send(Message::text(
                    serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
                )).await;
                return;
            };
            let success = {
                #[cfg(feature = "profile")]
                let _s = profile::Scope::new(&profile::T_ROUTE);
                send_to_client(client_id, message.get(), admin_id, admin_id_hash).await
            };
            #[cfg(feature = "profile")]
            let _reply = profile::Scope::new(&profile::T_REPLY);
            let payload = if success { COMMAND_RESULT_OK } else { COMMAND_RESULT_FAIL };
            let _ = stream.send(Message::Text(Bytes::from_static(payload.as_bytes()))).await;
        },

        // Broadcast para todos os clientes do admin
        CMD_BROADCAST => {
            let Some(message) = cmd.message else {
                let _ = stream.send(Message::text(
                    serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
                )).await;
                return;
            };
            let count = broadcast_to_clients(message.get(), admin_id, admin_id_hash).await;
            let result = CommandResult {
                message_type: "command_result",
                success: true,
                message: "Broadcast enviado",
                client_count: Some(count),
            };
            send_json(stream, &result).await;
        },

        CMD_LIST_CLIENTS => send_client_list(admin_id, stream).await,
        CMD_LIST_ADMINS => send_admin_list(stream).await,
        CMD_SERVER_STATUS => send_server_status(server_start, stream).await,
        CMD_PING => {
            let _ = stream.send(Message::text(
                serde_json::json!({"type": "pong", "timestamp": Local::now().to_rfc3339()}).to_string()
            )).await;
        },

        // Kick client
        CMD_KICK => {
            let Some(client_id) = cmd.client_id else {
                let _ = stream.send(Message::text(
                    serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
                )).await;
                return;
            };
            let success = kick_client(client_id, admin_id).await;
            let _ = stream.send(Message::text(serde_json::json!({
                "type": "command_result",
                "success": success,
                "message": if success { "Cliente desconectado" } else { "Falha ao desconectar cliente" }
            }).to_string())).await;
        },

        // Get client state
        CMD_GET_STATE => {
            let Some(client_id) = cmd.client_id else {
                let _ = stream.send(Message::text(
                    serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
                )).await;
                return;
            };
            let state = get_client_info(client_id, admin_id).await;
            let _ = stream.send(Message::text(serde_json::json!({
                "type": "client_info",
                "clientId": client_id,
                "info": state
            }).to_string())).await;
        },

        _ => {
            let _ = stream.send(Message::text(
                serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
            )).await;
        },
    }
}

// ============================================================================
// HANDLER BINÁRIO PROTOBUF (WSM) — admin→server
// ============================================================================
//
// ⚡ Este é o coração do protocolo binário. O fluxo:
//   1. Strip do byte de FLAGS
//   2. Parse do envelope protobuf (µs — 3 campos, sem string parsing,
//      sem árvore JSON). Fall back pra JSON legado se não for protobuf.
//   3. Roteamento com base no `type` + `client_id` do envelope
//   4. O PAYLOAD é copiado byte a byte pro canal do cliente — **nunca
//      descomprimido, nunca desserializado**. O ZSTD foi aplicado na ponta
//      (app admin) e desfeito na outra ponta (cliente Android).
//
// A resposta `command_result` também é protobuf puro: uma struct estática
// serializada (poucos bytes), sem JSON.

async fn handle_admin_binary(
    admin_id: u32,
    admin_id_hash: u32,
    bin: &[u8],
    stream: &mut WsStream,
    server_start: Instant,
) {
    #[cfg(feature = "profile")]
    let _scope_total = profile::Scope::new(&profile::T_ADMIN_TOTAL);
    #[cfg(feature = "profile")]
    profile::N_ADMIN_MSG.fetch_add(1, Ordering::Relaxed);

    // Byte 0 == '{' → cliente novo mandando JSON dentro de um frame binário
    // (caso raro, mas seguro de aceitar).
    if proto::is_json_frame(bin) {
        if let Ok(text_str) = std::str::from_utf8(bin) {
            handle_admin_message(admin_id, admin_id_hash, text_str, stream, server_start).await;
        }
        return;
    }

    // [FLAGS][envelope]
    let (flags, env_bytes) = bin.split_first().unwrap_or((&0, &[]));
    let payload_compressed = *flags & proto::FLAG_ZSTD != 0;

    let env: proto::AdminEnvelope = {
        #[cfg(feature = "profile")]
        let _s = profile::Scope::new(&profile::T_PARSE);
        match proto::AdminEnvelope::decode(env_bytes) {
            Ok(e) => e,
            Err(_) => {
                // Envelope inválido → responde erro em JSON (fallback seguro)
                let _ = stream.send(Message::text(
                    serde_json::json!({"type": "error", "message": "Comando inválido"}).to_string()
                )).await;
                return;
            }
        }
    };

    match proto::AdminCommandType::try_from(env.r#type).unwrap_or(proto::AdminCommandType::AdminUnspecified) {
        // ⚡ HOT PATH: envia o payload cru (ZSTD) pro cliente.
        // Zero descompressão, zero JSON parse — só memcpy de bytes.
        proto::AdminCommandType::ActSendToClient => {
            let Some(client_id) = (env.client_id != 0).then_some(env.client_id) else {
                send_admin_err(stream, "Comando inválido").await;
                return;
            };
            if env.payload.is_empty() {
                send_admin_err(stream, "Comando inválido").await;
                return;
            }
            let success = {
                #[cfg(feature = "profile")]
                let _s = profile::Scope::new(&profile::T_ROUTE);
                send_to_client_binary(client_id, &env.payload, payload_compressed, admin_id, admin_id_hash).await
            };
            #[cfg(feature = "profile")]
            let _reply = profile::Scope::new(&profile::T_REPLY);
            // ⚡ Resposta pré-computada: clone de Bytes é só um atomic
            // increment (zero alocação), idêntico ao caminho JSON.
            let frame = if success {
                WSM_COMMAND_RESULT_OK.clone()
            } else {
                WSM_COMMAND_RESULT_FAIL.clone()
            };
            let _ = stream.send(Message::Binary(frame)).await;
        },

        // ⚡ Broadcast: payload ZSTD compartilhado entre TODOS os clientes
        // do admin. Uma única cópia Bytes ref-counted (antes: re-serialização
        // por cliente). Payload idêntico em todos os destinos → clone grátis.
        proto::AdminCommandType::ActBroadcastToClients => {
            if env.payload.is_empty() {
                send_admin_err(stream, "Comando inválido").await;
                return;
            }
            let count = broadcast_to_clients_binary(&env.payload, payload_compressed, admin_id, admin_id_hash).await;
            // ⚡ Resposta protobuf de broadcast. Executada 1x por broadcast
            // (não por mensagem), então pode montar a struct — o frame
            // pré-computado não serve aqui porque carrega o client_count.
            let result = proto::ServerToAdmin {
                r#type: proto::ServerAdminType::SatCommandResult as i32,
                success: true,
                message: "Broadcast enviado".into(),
                client_count: count as u32,
                ..Default::default()
            };
            let _ = stream.send(Message::Binary(Bytes::from(
                proto::encode_envelope(&result, false)
            ))).await;
        },

        // Controle: continua em JSON (server precisa ler) — reusa handler.
        proto::AdminCommandType::ActListClients => send_client_list(admin_id, stream).await,
        proto::AdminCommandType::ActListAdmins => send_admin_list(stream).await,
        proto::AdminCommandType::ActServerStatus => send_server_status(server_start, stream).await,
        proto::AdminCommandType::ActKickClient => {
            let client_id = env.client_id;
            if client_id == 0 {
                send_admin_err(stream, "Comando inválido").await;
                return;
            }
            let success = kick_client(client_id, admin_id).await;
            let _ = stream.send(Message::text(serde_json::json!({
                "type": "command_result",
                "success": success,
                "message": if success { "Cliente desconectado" } else { "Falha ao desconectar cliente" }
            }).to_string())).await;
        },
        proto::AdminCommandType::ActGetClientState => {
            let client_id = env.client_id;
            if client_id == 0 {
                send_admin_err(stream, "Comando inválido").await;
                return;
            }
            let state = get_client_info(client_id, admin_id).await;
            let _ = stream.send(Message::text(serde_json::json!({
                "type": "client_info",
                "clientId": client_id,
                "info": state
            }).to_string())).await;
        },
        proto::AdminCommandType::ActPing => {
            let _ = stream.send(Message::text(
                serde_json::json!({"type": "pong", "timestamp": cached_timestamp().as_str()}).to_string()
            )).await;
        },
        _ => {
            send_admin_err(stream, "Comando inválido").await;
        },
    }
}

#[inline]
async fn send_admin_err(stream: &mut WsStream, msg: &str) {
    let _ = stream.send(Message::text(
        serde_json::json!({"type": "error", "message": msg}).to_string()
    )).await;
}

// ⚡ Encaminhamento de mensagens do cliente → admin.
//
// TEXT  (cliente JSON legado): serializa o envelope client_message e envia.
// BINARY (cliente WSM novo):   o payload ZSTD vai CRU — só montamos o envelope
//   ServerToAdmin em torno dele e empurramos pro canal do admin. **Nunca
//   descomprimimos** no servidor: quem recebe (app admin) faz o ZSTD decode.
//
// Manter os dois caminhos permite uma migração incremental: clientes Android
// antigos continuam funcionando enquanto os novos já falam WSM.

#[inline]
async fn forward_client_text(client_id: u32, trimmed: &str, admin_unique_id: Option<&str>) {
    if trimmed.is_empty() {
        return;
    }
    activity_log!("MESSAGE_RECEIVED: {} - {}", client_id, trimmed);
    if let Some(admin_unique_id) = admin_unique_id {
        #[cfg(feature = "profile")]
        profile::N_CLIENT_MSG.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "profile")]
        let _fwd_scope = profile::Scope::new(&profile::T_CLIENT_FWD);

        let ts = cached_timestamp();
        let client_message = ClientForwardMessage {
            message_type: "client_message",
            client_id,
            message: trimmed,
            timestamp: ts.as_str(),
        };
        #[cfg(feature = "profile")]
        let _ta = profile::Scope::new(&profile::T_TO_ADMIN);
        send_to_admin_by_unique_id(admin_unique_id, &client_message).await;
    }
}

#[inline]
async fn forward_client_binary(
    client_id: u32,
    payload: &[u8],
    payload_compressed: bool,
    admin_unique_id: Option<&str>,
) {
    activity_log!("MESSAGE_RECEIVED(BIN): {} - {}B", client_id, payload.len());
    if let Some(admin_unique_id) = admin_unique_id {
        #[cfg(feature = "profile")]
        profile::N_CLIENT_MSG.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "profile")]
        let _fwd_scope = profile::Scope::new(&profile::T_CLIENT_FWD);

        // ⚡ Fast path: monta o envelope ServerToAdmin com UMA cópia do
        // payload (o caminho genérico são duas — ver proto::encode_server_*).
        // O ZSTD é transparente pro servidor: só a ponta admin descomprime.
        let frame = proto::encode_client_message(client_id, payload, payload_compressed);
        #[cfg(feature = "profile")]
        let _ta = profile::Scope::new(&profile::T_TO_ADMIN);
        send_to_admin_raw_binary(admin_unique_id, frame).await;
    }
}

async fn handle_client_connection(
    init_msg: String,
    mut stream: WsStream,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse da mensagem de identificação
    let connect_msg: ClientConnectMessage = match serde_json::from_str(&init_msg) {
        Ok(msg) => msg,
        Err(_) => {
            let _ = stream.send(Message::text(
                serde_json::json!({"type": "error", "message": "Mensagem de identificação inválida"}).to_string()
            )).await;
            return Ok(());
        }
    };

    let admin_id_opt = connect_msg.admin_id;
    let admin_id_hash = admin_id_opt.as_ref().map(|id| hash_admin_id(id)).unwrap_or(0);
    let device_type = connect_msg.data.unwrap_or_else(|| "unknown".to_string());
    let android_id = connect_msg.android_id.unwrap_or_else(|| "unknown".to_string());
    let wallpaper = connect_msg.wallpaper.unwrap_or_default();

    // ⚡ BOUNDED channel para evitar OOM
    let (tx, mut rx) = mpsc::channel::<Message>(CHANNEL_BUFFER_SIZE);

    // ⚡ Limpeza de órfãos: se o admin já está conectado neste exato momento,
    // o cliente já nasce "validado" e nunca mais entra na checagem de órfão,
    // mesmo que esse admin fique offline depois.
    let admin_seen_initial = match &admin_id_opt {
        Some(id) => GLOBAL_ADMIN_MAP.read().await.contains_key(id),
        None => true, // sem adminId associado, o conceito de "órfão" não se aplica
    };

    let client_id = allocate_id();
    let shard = get_shard(client_id);

    let _wallpaper_idx = {
        let mut shard = shard.write().await;

        // Adicionar wallpaper ao pool
        let idx = if !wallpaper.is_empty() {
            shard.wallpaper_pool.get_or_insert(&wallpaper).unwrap_or(0)
        } else {
            0
        };

        // Adicionar conexão compacta - ⚡ OTIMIZADO: com índice
        let conn = CompactConnection::new(client_id, admin_id_hash, idx, false);
        shard.add_connection(conn);

        // Adicionar metadata
        shard.client_metadata.insert(client_id, ClientMetadata {
            addr,
            device_type: device_type.clone(),
            android_id: android_id.clone(),
            connected_at: Instant::now(),
            admin_id: admin_id_opt.clone(),
            admin_id_hash,
            sender: tx,
            last_heartbeat_update: Instant::now(),
            admin_seen: admin_seen_initial,
        });

        // Adicionar ao mapeamento admin->clients
        if admin_id_hash != 0 {
            shard.admin_clients.entry(admin_id_hash).or_insert_with(Vec::new).push(client_id);
        }

        idx
    };

    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    ACTIVE_CLIENTS.fetch_add(1, Ordering::Relaxed);

    debug_log!("Cliente {} conectado (admin: {:?})", client_id, admin_id_opt);
    activity_log!("CLIENT_CONNECTED: {} (admin: {:?})", client_id, admin_id_opt);

    // Notificar admin sobre nova conexão
    // ⚡ Serialização direta (sem árvore json!()) + timestamp do cache.
    if let Some(admin_unique_id) = &admin_id_opt {
        let ts = cached_timestamp();
        let connection_msg = ClientConnectedMsg {
            message_type: "client_connected",
            client_id,
            address: &addr.ip().to_string(),
            android_id: &android_id,
            port: addr.port(),
            device_type: &device_type,
            timestamp: ts.as_str(),
        };
        let payload = serde_json::to_string(&connection_msg).unwrap_or_default();
        send_to_admin_raw(admin_unique_id, payload).await;
    }

    // Enviar welcome ao cliente
    let ts = cached_timestamp();
    let welcome = WelcomeMsg {
        message_type: "welcome",
        client_id,
        timestamp: ts.as_str(),
    };
    let _ = stream.send(Message::text(serde_json::to_string(&welcome).unwrap_or_default())).await;

    // Evita consultar o shard em toda mensagem só para controlar o throttling.
    let mut last_activity_update = Instant::now();

    // ⚡ Loop principal do cliente — UM task por conexão, sem driver separado.
    loop {
        tokio::select! {
            // Mensagens do admin para enviar ao cliente - ENVIO IMEDIATO
            msg = rx.recv() => {
                match msg {
                    Some(message) => {
                        // ⚡ Escrita direta no socket (sem oneshot/driver).
                        let failed = {
                            #[cfg(feature = "profile")]
                            let _s = profile::Scope::new(&profile::T_WS_SEND_CLIENT);
                            stream.send(message).await.is_err()
                        };
                        if failed {
                            break;
                        }
                    }
                    None => break,
                }
            }

            // Mensagens recebidas do cliente
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // ⚡ sockudo-ws: Bytes -> &str
                        let text_str = std::str::from_utf8(&text).unwrap_or("");
                        // ⚡ trim() UMA vez (era chamado duas vezes: no
                        // forward e no is_empty() abaixo). str::trim escapa
                        // leading/trailing whitespace — custa uma passada.
                        let trimmed = text_str.trim();
                        forward_client_text(client_id, trimmed, admin_id_opt.as_deref()).await;
                        if !trimmed.is_empty()
                            && last_activity_update.elapsed().as_secs() >= HEARTBEAT_INTERVAL_SECS
                        {
                            update_client_activity(client_id).await;
                            last_activity_update = Instant::now();
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        // ⚡ Protocolo WSM (client→server): envelope protobuf +
                        // payload ZSTD. O servidor só precisa do type do envelope;
                        // o payload (resposta pro admin) é repassado CRU — sem
                        // descompressão. Ver src/proto.rs.
                        if proto::is_json_frame(&bin) {
                            // JSON dentro de frame binário (compat): trata como texto.
                            if let Ok(text_str) = std::str::from_utf8(&bin) {
                                forward_client_text(client_id, text_str.trim(), admin_id_opt.as_deref()).await;
                            }
                            continue;
                        }
                        let (flags, env_bytes) = match bin.split_first() {
                            Some((f, rest)) => (*f, rest),
                            None => continue,
                        };
                        let compressed = flags & proto::FLAG_ZSTD != 0;
                        if let Ok(env) = proto::ClientToServer::decode(env_bytes) {
                            match proto::ClientServerType::try_from(env.r#type)
                                .unwrap_or(proto::ClientServerType::ClientServerUnspecified)
                            {
                                proto::ClientServerType::CstClientResponse => {
                                    if !env.payload.is_empty() {
                                        forward_client_binary(
                                            client_id,
                                            &env.payload,
                                            compressed,
                                            admin_id_opt.as_deref(),
                                        ).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // ⚡ OTIMIZAÇÃO: Atualizar heartbeat apenas se passou tempo suficiente
                        let shard = get_shard(client_id);
                        let now = Instant::now();

                        let should_update = {
                            let shard_read = shard.read().await;
                            if let Some(meta) = shard_read.client_metadata.get(&client_id) {
                                now.duration_since(meta.last_heartbeat_update).as_secs() >= HEARTBEAT_INTERVAL_SECS
                            } else {
                                false
                            }
                        };

                        if should_update {
                            let mut shard = shard.write().await;
                            if let Some(conn) = shard.get_connection_mut(client_id) {
                                conn.flags |= 0b0000_0010;
                            }
                            if let Some(meta) = shard.client_metadata.get_mut(&client_id) {
                                meta.last_heartbeat_update = now;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        // 🔴 Completa o close handshake (idêntico ao loop do
                        // admin) — sem isso o socket vira TIME-WAIT e acumula.
                        let _ = stream.send(Message::Close(None)).await;
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        }
    }

    cleanup_client(client_id).await;
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientForwardMessage<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    client_id: u32,
    message: &'a str,
    // ⚡ Emprestado do timestamp em cache — zero alocação por mensagem.
    timestamp: &'a str,
}

// ⚡ Mensagens do caminho de CONEXÃO (churn: centenas de connects/s no seu
// benchmark). Antes eram montadas com json!() (constrói árvore Value com
// várias alocações) e depois to_string() (serializa a árvore) — trabalho
// duplo. Serialização direta de struct pula a árvore inteira; o timestamp
// vem do cache (sem chrono por connect).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientConnectedMsg<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    client_id: u32,
    address: &'a str,
    android_id: &'a str,
    port: u16,
    device_type: &'a str,
    timestamp: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WelcomeMsg<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    client_id: u32,
    timestamp: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminWelcomeMsg<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    admin_id: u32,
    admin_unique_id: &'a str,
    timestamp: &'a str,
}

#[derive(Serialize)]
struct CommandResult<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    success: bool,
    message: &'a str,
    #[serde(rename = "clientCount", skip_serializing_if = "Option::is_none")]
    client_count: Option<usize>,
}

async fn send_json<T: Serialize + ?Sized>(stream: &mut WsStream, value: &T) {
    if let Ok(payload) = serde_json::to_vec(value) {
        #[cfg(feature = "profile")]
        let _s = profile::Scope::new(&profile::T_WS_SEND_ADMIN);
        let _ = stream.send(Message::Text(Bytes::from(payload))).await;
    }
}

async fn update_client_activity(client_id: u32) {
    let shard = get_shard(client_id);
    let mut shard = shard.write().await;
    let current_time = shard.current_timestamp();
    if let Some(conn) = shard.get_connection_mut(client_id) {
        conn.update_activity(current_time);
    }
    if let Some(meta) = shard.client_metadata.get_mut(&client_id) {
        meta.last_heartbeat_update = Instant::now();
    }
}

// ============================================================================
// FUNÇÕES DE ENVIO/ROUTING (SIMPLES)
// ============================================================================

/// ⚡ Envia com BACKPRESSURE de verdade: se o canal do cliente estiver cheio
/// (cliente lento, rede ruim), ESPERA espaço em vez de descartar a mensagem.
///
/// Antes usávamos `try_send` em todo lugar. Quando o canal de 32 mensagens
/// enchia (cliente em 3G não dava conta), o comando era DESCARTADO
/// silenciosamente — o admin recebia `success:false` e o cliente nunca via o
/// comando. Em produção isso é "admin perde o contato com o cliente" sem
/// explicação. O sintoma no benchmark era centenas de "quedas durante envio".
///
/// Agora: `send().await` com timeout. O admin que envia é freado até o
/// cliente drenar — backpressure correto de um RELAY. Se o cliente travar
/// mesmo (timeout), aí sim descartamos e reportamos falha.
async fn send_with_backpressure(
    sender: &mpsc::Sender<Message>,
    msg: Message,
) -> bool {
    match tokio::time::timeout(Duration::from_secs(SEND_TIMEOUT_SECS), sender.send(msg)).await {
        Ok(Ok(())) => true,
        // Timeout: cliente não drenou em SEND_TIMEOUT_SECS — provavelmente
        // travado/desconectado. Descarta esta e segue.
        Ok(Err(_)) | Err(_) => false,
    }
}

// Enviar comando do admin para cliente específico
// ⚡ `message` é o JSON cru (bytes) — não parseamos Value nem serializamos de
// novo; copiamos os bytes direto pro canal do cliente. Uma única lookup no
// metadata (o admin_id_hash vive no ClientMetadata agora).
//
// ℹ️ O backpressure (send_with_backpressure) é chamado DENTRO do escopo do
// read lock. Pode parecer mau, mas em tokio single-thread (este servidor) as
// tasks são COOPERATIVAS: nenhum await rouba o lock de outra task sem um
// ponto de suspensão explícito. Soltar o lock antes (clonando o Sender)
// mostrou-se LENTO na prática (+1 Arc clone + branch por mensagem) — revertido.
async fn send_to_client(
    client_id: u32,
    message: &str,
    _admin_id: u32,
    admin_hash: u32,
) -> bool {
    let shard = get_shard(client_id);
    let shard = shard.read().await;

    if let Some(meta) = shard.client_metadata.get(&client_id) {
        // Verificar se cliente pertence ao mesmo admin (hash guardada no metadata)
        if meta.admin_id.is_some() && meta.admin_id_hash == admin_hash {
            // ⚡ Uma alocação (memcpy do JSON cru); antes eram: parse Value +
            // re-serialização (várias alocações).
            let outbound = Message::Text(Bytes::copy_from_slice(message.as_bytes()));
            activity_log!("TO_CLIENT: Admin {} → Cliente {}", _admin_id, client_id);
            return send_with_backpressure(&meta.sender, outbound).await;
        }
    }

    activity_log!("TO_CLIENT_FAILED: Admin {} não tem permissão para Cliente {}", _admin_id, client_id);
    false
}

// ⚡ Versão binária (WSM) do send_to_client.
// O payload JÁ está comprimido (ZSTD) na ponta admin — o servidor **nunca**
// descomprime. Só monta o envelope ServerToClient (bytes do payload são
// MOVED, zero cópia extra) e joga no canal do cliente.
//
// Comparado ao caminho JSON: pula RawValue-capture (já são bytes), pula
// serialização do envelope (protobuf é mais barato que JSON) e entrega a
// carga comprimida — menos bytes no socket = menos syscalls/write.
async fn send_to_client_binary(
    client_id: u32,
    payload: &[u8],
    payload_compressed: bool,
    _admin_id: u32,
    admin_hash: u32,
) -> bool {
    // ℹ️ Lock mantido durante o backpressure — em single-thread tokio isso não
    // gera contenção (tasks cooperativas). Clone extra do Sender provou-se
    // mais lento; revertido. Ver send_to_client.
    let shard = get_shard(client_id);
    let shard = shard.read().await;

    if let Some(meta) = shard.client_metadata.get(&client_id) {
        if meta.admin_id.is_some() && meta.admin_id_hash == admin_hash {
            // ⚡ Fast path: UMA cópia do payload (não duas). O envelope
            // ServerToClient é escrito direto, sem struct intermediária —
            // ver proto::encode_server_command.
            let frame = proto::encode_server_command(client_id, payload, payload_compressed);
            activity_log!("TO_CLIENT(BIN): Admin {} → Cliente {} ({}B)", _admin_id, client_id, frame.len());
            return send_with_backpressure(&meta.sender, Message::Binary(Bytes::from(frame))).await;
        }
    }

    activity_log!("TO_CLIENT_FAILED: Admin {} não tem permissão para Cliente {}", _admin_id, client_id);
    false
}

// ⚡ Broadcast binário (WSM): o payload ZSTD é **compartilhado** entre todos
// os clientes do admin. O envelope é serializado UMA vez e o `Bytes` final é
// ref-counted — cada cliente recebe um clone do mesmo Arc (copia só o ponteiro,
// não os bytes). Antes: serialização + cópia por cliente.
async fn broadcast_to_clients_binary(
    payload: &[u8],
    payload_compressed: bool,
    _admin_id: u32,
    admin_hash: u32,
) -> usize {
    let mut count = 0;

    // ⚡ Serializa o envelope UMA SÓ VEZ, com UMA cópia do payload. O Bytes
    // resultante é ref-countado: clones (`.clone()`) incrementam um contador
    // atômico em vez de copiar os bytes — broadcast se torna O(1) em bytes
    // copiados por cliente.
    let shared: Bytes = Bytes::from(proto::encode_server_broadcast(payload, payload_compressed));
    let outbound = Message::Binary(shared);

    for shard_arc in CONN_SHARDS.iter() {
            let senders: Vec<mpsc::Sender<Message>> = {
                let shard = shard_arc.read().await;
                match shard.admin_clients.get(&admin_hash) {
                    Some(client_ids) => client_ids
                        .iter()
                        .filter_map(|id| shard.client_metadata.get(id).map(|m| m.sender.clone()))
                        .collect(),
                    None => Vec::new(),
                }
            };

            for (i, sender) in senders.iter().enumerate() {
                if sender.try_send(outbound.clone()).is_ok() {
                    count += 1;
                }
                if (i + 1) % BATCH_SIZE == 0 {
                    tokio::task::yield_now().await;
                }
            }

            tokio::task::yield_now().await;
    }

    activity_log!("BROADCAST(BIN): Admin {} → {} clientes ({}B)", _admin_id, count, shared.len());
    count
}

// ⚡ Broadcast com yield para não bloquear runtime
async fn broadcast_to_clients(
    message: &str,
    _admin_id: u32,
    admin_hash: u32,
) -> usize {
    let mut count = 0;
    // ⚡ JSON cru compartilhado: UMA cópia por broadcast (Bytes ref-counted),
    // em vez de serializar o Value uma vez e clonar.
    let outbound = Message::Text(Bytes::copy_from_slice(message.as_bytes()));

    // ⚡ Procurar clientes em todos os shards com yield periódico
    for shard_arc in CONN_SHARDS.iter() {
            // ⚡ OTIMIZADO: coleta os senders (clone barato, é um Arc por baixo)
            // e libera o RwLock ANTES de fazer os envios/yields. Antes o lock de
            // leitura do shard ficava preso durante os `yield_now().await`,
            // bloqueando qualquer escrita concorrente (novas conexões, cleanup)
            // naquele shard até o broadcast inteiro terminar.
            let senders: Vec<mpsc::Sender<Message>> = {
                let shard = shard_arc.read().await;
                match shard.admin_clients.get(&admin_hash) {
                    Some(client_ids) => client_ids
                        .iter()
                        .filter_map(|id| shard.client_metadata.get(id).map(|m| m.sender.clone()))
                        .collect(),
                    None => Vec::new(),
                }
            };

            for (i, sender) in senders.iter().enumerate() {
                // ⚡ Usar try_send para não bloquear em clientes lentos
                if sender.try_send(outbound.clone()).is_ok() {
                    count += 1;
                }

                // Yield por lote para reduzir trocas de task sem perder fairness.
                if (i + 1) % BATCH_SIZE == 0 {
                    tokio::task::yield_now().await;
                }
            }

            // ⚡ Yield entre shards
            tokio::task::yield_now().await;
    }

    activity_log!("BROADCAST: Admin {} → {} clientes", _admin_id, count);
    count
}

// Kick client (envia mensagem de disconnect)
// ⚡ Limpeza de órfãos: marca como "validados" todos os clientes que já
// estavam conectados esperando por este admin (admin_clients já é indexado
// por admin_hash, então isso é O(clientes desse admin), não O(n) total).
async fn mark_admin_seen_for_existing_clients(admin_id_hash: u32) {
    for shard_arc in CONN_SHARDS.iter() {
        let client_ids: Vec<u32> = {
            let shard = shard_arc.read().await;
            match shard.admin_clients.get(&admin_id_hash) {
                Some(ids) => ids.clone(),
                None => continue,
            }
        };
        if client_ids.is_empty() {
            continue;
        }
        let mut shard = shard_arc.write().await;
        for client_id in client_ids {
            if let Some(meta) = shard.client_metadata.get_mut(&client_id) {
                meta.admin_seen = true;
            }
        }
    }
}

// ⚡ Task periódica: derruba clientes cujo admin nunca apareceu dentro da
// janela de tolerância, liberando a vaga de conexão. Roda em background,
// independente de qualquer comando de admin.
async fn orphan_cleanup_task() {
    loop {
        sleep(Duration::from_secs(ORPHAN_SWEEP_INTERVAL_SECS)).await;

        let mut expired = 0u32;
        for shard_arc in CONN_SHARDS.iter() {
            // ⚡ Coleta os candidatos com o lock de leitura, envia a
            // notificação de desconexão só depois de soltar o lock.
            let to_disconnect: Vec<(u32, mpsc::Sender<Message>)> = {
                let shard = shard_arc.read().await;
                shard.client_metadata.iter()
                    .filter(|(_, meta)| {
                        !meta.admin_seen
                            && meta.admin_id.is_some()
                            && meta.connected_at.elapsed().as_secs() >= ORPHAN_CLIENT_TIMEOUT_SECS
                    })
                    .map(|(id, meta)| (*id, meta.sender.clone()))
                    .collect()
            };

            for (_client_id, sender) in to_disconnect {
                let msg = serde_json::json!({
                    "type": "disconnect",
                    "reason": "Admin não conectado (timeout de conexão órfã)"
                });
                let _ = sender.send(Message::text(msg.to_string())).await;
                expired += 1;
                activity_log!(
                    "ORPHAN_CLEANUP: Cliente {} desconectado (admin nunca conectou em {}s)",
                    _client_id, ORPHAN_CLIENT_TIMEOUT_SECS
                );
            }
        }

        if expired > 0 {
            println!("⚡ Limpeza de órfãos: {} cliente(s) desconectado(s) por falta de admin", expired);
        }
    }
}

async fn kick_client(client_id: u32, admin_id: u32) -> bool {
    // Verificar permissão primeiro
    let shard = get_shard(client_id);
    let shard = shard.read().await;

    // ⚡ OTIMIZADO: O(1) via índice em vez de escanear o Vec de conexões
    let admin_hash = {
        let admin_shard = get_shard(admin_id);
        let admin_shard = admin_shard.read().await;
        admin_shard.get_connection(admin_id).map(|c| c.admin_id_hash)
    };

    if let Some(meta) = shard.client_metadata.get(&client_id) {
        if meta.admin_id.is_some() {
            // ⚡ OTIMIZADO: reaproveita o hash já calculado na CompactConnection
            let client_admin_hash = shard.get_connection(client_id).map(|c| c.admin_id_hash);
            if client_admin_hash.is_some() && client_admin_hash == admin_hash {
                // Enviar mensagem de disconnect
                let kick_msg = serde_json::json!({
                    "type": "disconnect",
                    "reason": "Kicked by admin"
                });
                let _ = meta.sender.send(Message::text(kick_msg.to_string())).await;
                return true;
            }
        }
    }

    false
}

// ⚡ Versão RAW: o payload JÁ está serializado (String) — pula a serialização.
// Usada no caminho de conexão (notificação client_connected), onde o JSON é
// montado uma vez e mandado direto.
//
// 🔴 O Sender é clonado e o lock liberado antes do envio — segurar o read lock
// do GLOBAL_ADMIN_MAP através do backpressure (até SEND_TIMEOUT_SECS) bloqueia
// login/logout de todos os admins. Ver send_to_admin_by_unique_id.
async fn send_to_admin_raw(admin_unique_id: &str, payload: String) -> bool {
    let sender = {
        let global_map = GLOBAL_ADMIN_MAP.read().await;
        global_map.get(admin_unique_id).cloned()
    };

    let Some(sender) = sender else {
        activity_log!("TO_ADMIN_FAILED: Admin {} não encontrado", admin_unique_id);
        return false;
    };

    let outbound = Message::Text(Bytes::from(payload));
    let ok = send_with_backpressure(&sender, outbound).await;
    activity_log!("TO_ADMIN: {} {}", admin_unique_id, if ok { "✅" } else { "⏱️ (timeout/backpressure)" });
    ok
}

// ⚡ Variante binária (WSM) do send_to_admin: o frame protobuf JÁ está montado
// (payload ZSTD dentro). Empurra direto pro canal — zero serialização.
async fn send_to_admin_raw_binary(admin_unique_id: &str, frame: Vec<u8>) -> bool {
    let sender = {
        let global_map = GLOBAL_ADMIN_MAP.read().await;
        global_map.get(admin_unique_id).cloned()
    };

    let Some(sender) = sender else {
        activity_log!("TO_ADMIN_FAILED: Admin {} não encontrado", admin_unique_id);
        return false;
    };

    let ok = send_with_backpressure(&sender, Message::Binary(Bytes::from(frame))).await;
    activity_log!("TO_ADMIN(BIN): {} {}", admin_unique_id, if ok { "✅" } else { "⏱️ (timeout/backpressure)" });
    ok
}
async fn send_to_admin_by_unique_id<T: Serialize + ?Sized>(admin_unique_id: &str, message: &T) -> bool {
    // ⚡ Lookup O(1) no mapa global.
    //
    // 🔴 O Sender é CLONADO e o lock LIBERADO antes do envio. O write lock do
    // GLOBAL_ADMIN_MAP é tomado em TODO login/logout de admin; se segurássemos
    // o read lock através do send_with_backpressure().await, um único admin
    // lento (espera de até SEND_TIMEOUT_SECS) bloquearia TODOS os outros
    // admins de logar/deslogar. O clone é um Arc increment (~nanos).
    let sender = {
        let global_map = GLOBAL_ADMIN_MAP.read().await;
        global_map.get(admin_unique_id).cloned()
    };

    let Some(sender) = sender else {
        activity_log!("TO_ADMIN_FAILED: Admin {} não encontrado", admin_unique_id);
        return false;
    };

    let payload = match serde_json::to_vec(message) {
        Ok(payload) => Message::Text(Bytes::from(payload)),
        Err(_) => return false,
    };

    // 🔴 BACKPRESSURE (era try_send). Este é o único caminho PONTO-A-PONTO
    // que ainda descartava silenciosamente: quando o canal do admin
    // (256) enchesse — app admin em background, rede lenta — a resposta
    // do cliente sumia e o admin nunca ficava sabendo. Os outros 4
    // caminhos ponto-a-ponto (send_to_client, send_to_client_binary,
    // send_to_admin_raw, send_to_admin_raw_binary) já usam
    // send_with_backpressure; só faltava este.
    //
    // Trade-off honesto: agora a TASK DO CLIENTE espera (até
    // SEND_TIMEOUT_SECS) o admin drenar, em vez de seguir mandando e
    // perdendo tudo. Isso é backpressure de relay correto — o cliente
    // desacelera quando o destino não dá conta, como já acontece no
    // sentido inverso. Broadcast continua com try_send (lá é intencional:
    // um cliente travado não pode atrasar o delivery pra centenas de outros).
    let ok = send_with_backpressure(&sender, payload).await;
    if ok {
        activity_log!("TO_ADMIN: {} ✅", admin_unique_id);
    } else {
        activity_log!("TO_ADMIN_SLOW: {} (timeout/backpressure)", admin_unique_id);
    }
    ok
}

// ============================================================================
// FUNÇÕES DE LISTAGEM
// ============================================================================

// ⚡ Entries de listagem: dados coletados DENTRO do lock (strings CLONADAS —
// poucos bytes cada), serializados FORA dele. Antes, o json!() (que monta uma
// árvore serde Value com ~10 alocações por cliente) rodava DENTRO do read
// lock do shard — com 3000 clientes isso são dezenas de milhares de alocações
// segurando o lock, bloqueando connects/disconnects/cleanup do shard por
// dezenas de ms. Agora só os clones pequenos ficam no lock; a serialização
// (a parte cara) roda solta.
#[derive(Serialize)]
struct ClientListEntry {
    id: u32,
    address: String,
    #[serde(rename = "androidId")]
    android_id: String,
    port: u16,
    #[serde(rename = "deviceType")]
    device_type: String,
    uptime: u64,
    #[serde(rename = "isAlive")]
    is_alive: bool,
    #[serde(rename = "adminId", skip_serializing_if = "Option::is_none")]
    admin_id: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    wallpaper: String,
    #[serde(rename = "lastActivity")]
    last_activity: u16,
}

#[derive(Serialize)]
struct ClientListResponse<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    clients: &'a [ClientListEntry],
    total: usize,
    timestamp: &'a str,
}

async fn send_client_list(admin_id: u32, stream: &mut WsStream) {
    let admin_shard = get_shard(admin_id);
    let admin_unique_id = {
        let shard = admin_shard.read().await;
        shard.admin_metadata.get(&admin_id).map(|a| a.admin_id.clone())
    };

    let mut clients: Vec<ClientListEntry> = Vec::new();

    // ⚡ Coleta os dados CRUDOS (structs pequenos) enquanto o lock está
    // segurado, e serializa DEPOIS de soltar. Antes, o json!() (que monta uma
    // árvore serde Value com ~8 alocações por cliente) rodava DENTRO do
    // read lock do shard — com 3000 clientes isso são dezenas de milhares de
    // alocações segurando o lock, bloqueando connects/disconnects/cleanup
    // do shard inteiro por dezenas de ms.
    for shard_arc in CONN_SHARDS.iter() {
        let shard = shard_arc.read().await;
        for (client_id, meta) in shard.client_metadata.iter() {
            // Mostrar apenas clientes sem adminId ou do mesmo adminId
            let should_show = meta.admin_id.is_none() ||
                             meta.admin_id.as_ref() == admin_unique_id.as_ref();

            if should_show {
                // ⚡ OTIMIZADO: O(1) lookup
                if let Some(conn) = shard.get_connection(*client_id) {
                    let uptime = Instant::now().duration_since(meta.connected_at).as_secs();
                    // Copiar valores da estrutura packed para evitar problemas de alinhamento
                    let wallpaper_idx = conn.wallpaper_idx;
                    let last_activity = conn.last_activity;
                    let is_alive = conn.is_alive();

                    let wallpaper = shard.wallpaper_pool.get(wallpaper_idx)
                        .map(|s| s.as_str())
                        .unwrap_or("");

                    clients.push(ClientListEntry {
                        id: *client_id,
                        address: meta.addr.ip().to_string(),
                        port: meta.addr.port(),
                        android_id: meta.android_id.clone(),
                        device_type: meta.device_type.clone(),
                        uptime,
                        is_alive,
                        admin_id: meta.admin_id.clone(),
                        wallpaper: wallpaper.to_string(),
                        last_activity,
                    });
                }
            }
        }
    }

    // ⚡ Serialização FORA do lock: direto pro buffer (uma passada, sem árvore
    // serde Value). Antes eram duas passadas + milhares de alocações (json!()
    // + to_string()). O timestamp vem do cache (sem chrono).
    let ts = cached_timestamp();
    let response = ClientListResponse {
        message_type: "client_list",
        clients: &clients,
        total: clients.len(),
        timestamp: ts.as_str(),
    };
    let payload = serde_json::to_string(&response).unwrap_or_default();

    let _ = stream.send(Message::Text(Bytes::from(payload))).await;
}

async fn send_admin_list(stream: &mut WsStream) {
    let mut admins = Vec::new();

    for shard_arc in CONN_SHARDS.iter() {
        let shard = shard_arc.read().await;
        for (id, meta) in shard.admin_metadata.iter() {
            // ⚡ OTIMIZADO: O(1) lookup
            if let Some(conn) = shard.get_connection(*id) {
                let uptime = Instant::now().duration_since(meta.connected_at).as_secs();
                let is_alive = conn.is_alive();

                admins.push(serde_json::json!({
                    "id": id,
                    "adminUniqueId": meta.admin_id,
                    "address": meta.addr.ip().to_string(),
                    "port": meta.addr.port(),
                    "uptime": uptime,
                    "isAlive": is_alive
                }));
            }
        }
    }

    let response = serde_json::json!({
        "type": "admin_list",
        "admins": admins,
        "total": admins.len(),
        "timestamp": Local::now().to_rfc3339()
    });

    let _ = stream.send(Message::text(response.to_string())).await;
}

async fn send_server_status(server_start: Instant, stream: &mut WsStream) {
    let uptime = server_start.elapsed().as_secs();
    let total = TOTAL_CONNECTIONS.load(Ordering::Relaxed);
    let clients = ACTIVE_CLIENTS.load(Ordering::Relaxed);
    let admins = ACTIVE_ADMINS.load(Ordering::Relaxed);

    // Calcular uso de memória
    let mem_per_conn = std::mem::size_of::<CompactConnection>();
    let estimated_mem = (total * mem_per_conn) / 1_000_000; // MB

    let response = serde_json::json!({
        "type": "server_status",
        "version": "v4-simple-optimized",
        "uptime": uptime,
        "connections": {
            "total": total,
            "clients": clients,
            "admins": admins,
            "maxCapacity": MAX_CONNECTIONS
        },
        "memory": {
            "perConnectionBytes": mem_per_conn,
            "estimatedTotalMB": estimated_mem
        },
        "optimizations": {
            "shards": NUM_SHARDS,
            "batchSize": BATCH_SIZE,
            "lockFreeCounters": true
        },
        "timestamp": Local::now().to_rfc3339()
    });

    let _ = stream.send(Message::text(response.to_string())).await;
}

async fn get_client_info(client_id: u32, admin_id: u32) -> Option<serde_json::Value> {
    let shard = get_shard(client_id);
    let shard = shard.read().await;

    // Verificar permissão - ⚡ OTIMIZADO: O(1) lookup
    let admin_hash = {
        let admin_shard = get_shard(admin_id);
        let admin_shard = admin_shard.read().await;
        admin_shard.get_connection(admin_id)
            .map(|c| c.admin_id_hash)
    };

    if let Some(meta) = shard.client_metadata.get(&client_id) {
        if meta.admin_id.is_some() {
            // ⚡ OTIMIZADO: reaproveita o hash já calculado na CompactConnection
            let client_admin_hash = shard.get_connection(client_id).map(|c| c.admin_id_hash);
            if client_admin_hash.is_some() && client_admin_hash == admin_hash {
                // ⚡ OTIMIZADO: O(1) lookup
                if let Some(conn) = shard.get_connection(client_id) {
                    // Copiar valores da estrutura packed
                    let wallpaper_idx = conn.wallpaper_idx;
                    let last_activity = conn.last_activity;
                    let is_alive = conn.is_alive();

                    let wallpaper = shard.wallpaper_pool.get(wallpaper_idx)
                        .map(|s| s.as_str())
                        .unwrap_or("");

                    return Some(serde_json::json!({
                        "id": client_id,
                        "address": meta.addr.ip().to_string(),
                        "port": meta.addr.port(),
                        "androidId": meta.android_id,
                        "deviceType": meta.device_type,
                        "connectedAt": meta.connected_at.elapsed().as_secs(),
                        "lastActivity": last_activity,
                        "isAlive": is_alive,
                        "adminId": meta.admin_id,
                        "wallpaper": wallpaper
                    }));
                }
            }
        }
    }

    None
}

// ============================================================================
// CLEANUP
// ============================================================================

async fn cleanup_client(client_id: u32) {
    let shard = get_shard(client_id);

    // Obter adminId antes de remover
    let client_admin_id = {
        let shard = shard.read().await;
        shard.client_metadata.get(&client_id).and_then(|c| c.admin_id.clone())
    };

    let mut shard = shard.write().await;

    // ⚡ OTIMIZADO: Remove com atualização de índices
    shard.remove_connection(client_id);
    shard.client_metadata.remove(&client_id);

    // Remover do mapeamento admin->clients
    for clients in shard.admin_clients.values_mut() {
        clients.retain(|&id| id != client_id);
    }

    drop(shard);

    TOTAL_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    ACTIVE_CLIENTS.fetch_sub(1, Ordering::Relaxed);

    debug_log!("Cliente {} desconectado", client_id);
    activity_log!("CLIENT_DISCONNECTED: {}", client_id);

    // Notificar admin sobre desconexão
    // ⚡ JSON montado direto no buffer (string fixa + clientId numérico);
    // sem árvore Value e sem chrono por disconnect.
    if let Some(admin_unique_id) = client_admin_id {
        let ts = cached_timestamp();
        let payload = format!(
            r#"{{"type":"client_disconnected","clientId":{},"timestamp":"{}"}}"#,
            client_id, ts
        );
        send_to_admin_raw(&admin_unique_id, payload).await;
    }
}

async fn cleanup_admin(admin_id: u32) {
    let shard = get_shard(admin_id);

    // ⚡ Obter admin_unique_id antes de remover
    let admin_unique_id = {
        let shard = shard.read().await;
        shard.admin_metadata.get(&admin_id).map(|m| m.admin_id.clone())
    };

    // ⚡ Remover do mapa global
    if let Some(unique_id) = &admin_unique_id {
        let mut global_map = GLOBAL_ADMIN_MAP.write().await;
        global_map.remove(unique_id);
    }

    let mut shard = shard.write().await;

    // ⚡ OTIMIZADO: Remove com atualização de índices
    shard.remove_connection(admin_id);
    shard.admin_metadata.remove(&admin_id);

    TOTAL_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    ACTIVE_ADMINS.fetch_sub(1, Ordering::Relaxed);

    debug_log!("Admin {} desconectado", admin_id);
    activity_log!("ADMIN_DISCONNECTED: {}", admin_id);
}

// ============================================================================
// UTILIDADES
// ============================================================================

// ⚡ OTIMIZAÇÃO: Logging assíncrono - envia para canal em vez de escrever direto
#[cfg(feature = "activity-logging")]
fn log_activity_async(args: std::fmt::Arguments<'_>) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let log_msg = format!("[{}] {}", timestamp, args);

    // Enviar para canal assíncrono (não bloqueia)
    let _ = LOG_CHANNEL.0.send(log_msg);
}

// Log síncrono legado (para casos onde precisamos garantir escrita imediata)
fn log_activity(msg: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(LOG_FILE) {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
        let _ = writeln!(file, "[{}] {}", timestamp, msg);
    }
}

// Log apenas eventos críticos (erros, sobrecarga, timeouts)
fn log_critical(msg: &str) {
    if msg.contains("ERROR") || msg.contains("sobrecarregado") || msg.contains("rejeitando")
        || msg.contains("TIMEOUT") || msg.contains("FULL") {
        eprintln!("[CRITICAL] {}", msg);
        log_activity(&format!("CRITICAL: {}", msg));
    }
}

// ⚡ Task de background para escrever logs em batch
#[cfg(feature = "activity-logging")]
async fn log_writer_task() {
    let mut rx = {
        let mut guard = LOG_CHANNEL.1.write().await;
        guard.take().expect("Log receiver já foi consumido")
    };

    let mut buffer = Vec::with_capacity(LOG_BUFFER_SIZE);
    let mut last_flush = Instant::now();

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(log_msg) => {
                        buffer.push(log_msg);

                        // Flush se buffer está cheio ou passou tempo suficiente
                        let should_flush = buffer.len() >= LOG_BUFFER_SIZE
                            || last_flush.elapsed() >= Duration::from_secs(5);

                        if should_flush {
                            flush_logs(&mut buffer).await;
                            last_flush = Instant::now();
                        }
                    }
                    None => {
                        // Canal fechado - flush final e sair
                        flush_logs(&mut buffer).await;
                        break;
                    }
                }
            }

            // Flush periódico a cada 5 segundos
            _ = sleep(Duration::from_secs(5)) => {
                if !buffer.is_empty() {
                    flush_logs(&mut buffer).await;
                    last_flush = Instant::now();
                }
            }
        }
    }
}

#[cfg(feature = "activity-logging")]
async fn flush_logs(buffer: &mut Vec<String>) {
    if buffer.is_empty() {
        return;
    }

    // Escrever todos os logs em batch
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(LOG_FILE) {
        for log_msg in buffer.drain(..) {
            let _ = writeln!(file, "{}", log_msg);
        }
    }
}
