//! Gerador de carga WebSocket 100% Rust — sem dependência de Python.
//!
//! Dois modos de uso:
//!
//! 1) Local, subindo o servidor sozinho (mede CPU real via /proc):
//!    load-client --url ws://127.0.0.1:8080 --server-bin ./websocket-server \
//!                --admins 200 --clients-per-admin 1 --duration 15
//!
//! 2) Contra um servidor que já está rodando em outro lugar (produção,
//!    staging, etc.) — sem --server-bin, não tenta subir nada nem medir CPU
//!    remoto (não daria, é outra máquina), só reporta throughput/latência
//!    do lado do cliente:
//!    load-client --url wss://meuservidor.exemplo.com --admins 50 \
//!                --clients-per-admin 4 --duration 30
//!
//! Suporta ws:// e wss:// (TLS via rustls, raiz de confiança embutida via
//! webpki-roots — não depende de ca-certificates instalado na máquina).

use clap::Parser;
use futures_util::{SinkExt as _, StreamExt as _};
use sockudo_ws::{handshake::client_handshake, Config, Message, Role, WebSocketStream};
use std::io;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

// ============================================================================
// CLI
// ============================================================================

#[derive(Parser, Debug, Clone)]
#[command(
    name = "load-client",
    about = "Gerador de carga WebSocket em Rust puro, com orquestração opcional de systemd-run"
)]
struct Args {
    /// URL do WebSocket alvo. Aceita ws:// e wss://.
    /// Ex: ws://127.0.0.1:8080  ou  wss://meuservidor.exemplo.com/
    #[arg(long)]
    url: String,

    /// Número de admins simulados
    #[arg(long, default_value_t = 50)]
    admins: usize,

    /// Clientes por admin
    #[arg(long, default_value_t = 4)]
    clients_per_admin: usize,

    /// Duração do teste em segundos
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Caminho do binário do servidor. Se informado, este programa sobe o
    /// servidor sozinho via `sudo systemd-run --scope` com os limites de
    /// CPU/memória abaixo, mede o CPU real dele durante o teste (via
    /// /proc/PID/stat) e reporta custo em µs/mensagem. Se omitido, assume
    /// que --url já aponta pra um servidor rodando em outro lugar.
    #[arg(long)]
    server_bin: Option<String>,

    /// Cota de CPU pro systemd-run (só usado com --server-bin)
    #[arg(long, default_value = "10%")]
    cpu_quota: String,

    /// Limite de memória pro systemd-run (só usado com --server-bin)
    #[arg(long, default_value = "512M")]
    mem_max: String,

    /// Quantos admins sobem por "lote" antes de ceder o runtime (yield).
    /// Evita estourar a fila de accept() do kernel ao escalar pra milhares
    /// de conexões de uma vez.
    #[arg(long, default_value_t = 20)]
    ramp_batch: usize,

    /// ⚡ Falar o protocolo binário WSM (envelope protobuf + payload ZSTD)
    /// em vez de JSON no hot path de send_to_client. Compara os dois.
    #[arg(long, default_value_t = false)]
    wsm: bool,

    /// ⚡ Tamanho (em bytes) da mensagem de carga. Padrão 0 = mensagem mínima
    /// (`{"type":"cmd","i":N}`), igual ao comportamento antigo.
    ///
    /// Com um tamanho maior (ex: 1024) o payload é preenchido com letras
    /// variadas até atingir aproximadamente este tamanho. É a forma de ver o
    /// poder da compressão ZSTD do WSM: mensagens grandes comprimem muito,
    /// as minúsculas não (ZSTD as incha).
    #[arg(long, default_value_t = 0)]
    msg_size: usize,
}

// ============================================================================
// Transporte: TCP puro ou TLS, escolhido em runtime a partir do esquema da URL
// ============================================================================

enum MaybeTls {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

type WsStream = WebSocketStream<MaybeTls>;

#[derive(Clone)]
struct Target {
    host: String,
    port: u16,
    path: String,
    tls: bool,
    tls_connector: Option<tokio_rustls::TlsConnector>,
}

fn parse_target(url_str: &str) -> Target {
    let parsed = url::Url::parse(url_str).expect("URL inválida (esperado ws://host:porta/path ou wss://...)");
    let tls = match parsed.scheme() {
        "ws" => false,
        "wss" => true,
        other => panic!("esquema '{other}' não suportado, use ws:// ou wss://"),
    };
    let host = parsed
        .host_str()
        .expect("URL sem host (ex: ws://127.0.0.1:8080)")
        .to_string();
    let port = parsed.port_or_known_default().unwrap_or(if tls { 443 } else { 80 });
    let path = if parsed.path().is_empty() { "/".to_string() } else { parsed.path().to_string() };

    let tls_connector = if tls {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Some(tokio_rustls::TlsConnector::from(Arc::new(config)))
    } else {
        None
    };

    Target { host, port, path, tls, tls_connector }
}

// ⚡ Antes: TcpStream::connect(...).unwrap() derrubava o processo inteiro
// (panic=abort) na primeira falha de conexão. Agora retorna Option; quem
// chama decide (contar falha e seguir em frente).
async fn connect_ws(target: &Target) -> Option<WsStream> {
    let tcp = TcpStream::connect((target.host.as_str(), target.port)).await.ok()?;
    let _ = tcp.set_nodelay(true);

    let mut transport = if target.tls {
        let connector = target.tls_connector.as_ref()?;
        let domain = rustls::pki_types::ServerName::try_from(target.host.clone()).ok()?;
        let tls_stream = connector.connect(domain, tcp).await.ok()?;
        MaybeTls::Tls(Box::new(tls_stream))
    } else {
        MaybeTls::Plain(tcp)
    };

    client_handshake(&mut transport, &target.host, &target.path, None).await.ok()?;
    Some(WebSocketStream::from_raw(transport, Role::Client, Config::default()))
}

fn text_str(t: &bytes::Bytes) -> &str {
    std::str::from_utf8(t).unwrap_or("")
}

// ⚡ Gera o payload de carga do teste.
//
// `msg_size == 0` → mensagem mínima (comportamento antigo).
// `msg_size > 0`  → a mensagem é preenchida com LETRAS VARIADAS até atingir
//                   aproximadamente este tamanho, em bytes.
//
// Por que letras variadas e não um bloco repetido: um payload todo igual
// (ex: 1024x 'a') é o melhor caso irreal do ZSTD — o compressor decora um
// único símbolo. Conteúdo pseudo-aleatório mas DETERMINÍSTICO (semente =
// índice da mensagem) aproxima a entropia de um payload real (file listing,
// trecho de texto) sem custar caro nem variar entre runs — o benchmark é
// reproduzível e a comparação JSON-vs-WSM é justa.
fn build_payload(msg_size: usize, i: usize) -> String {
    if msg_size == 0 {
        return format!(r#"{{"type":"cmd","i":{i}}}"#);
    }
    // Prefixo fixo + campo "data" preenchido até msg_size.
    let prefix = format!(r#"{{"type":"cmd","i":{i},"data":""#);
    let suffix = r#""}"#;
    let need = msg_size.saturating_sub(prefix.len() + suffix.len());
    if need == 0 {
        return format!("{prefix}{suffix}");
    }

    // ⚡ Conteúdo SEMI-ESTRUTURADO, não ruído puro. É o que um payload real
    // parece (paths, nomes de arquivo, chaves JSON): vocabulário pequeno
    // repetido várias vezes. Ruído aleatório é o PIOR caso do ZSTD — 1024B de
    // letras aleatórias só vão pra 63%, a compressão mal paga o overhead.
    // Payloads realistas vão pra 20-25%, e é aí que o WSM brilha.
    //
    // Determinístico (semente = índice): reproduzível, e a comparação
    // JSON-vs-WSM é justa — os dois enviam EXATAMENTE os mesmos bytes
    // descomprimidos, só muda o encapsulamento.
    let mut state: u64 = (i as u64).wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(0x1);
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    // Vocabulário estilo file listing (o payload mais comum deste app).
    const WORDS: [&str; 16] = [
        "DCIM", "Camera", "IMG_001", ".jpg", "storage", "emulated", "download",
        "screenshot", "_2026", "audio", "music", "documents", "pdf", "app",
        "cache", "thumb",
    ];
    const SEPS: [char; 3] = ['/', '_', '-'];

    let mut data = String::with_capacity(need);
    while data.len() < need {
        let w = WORDS[next() as usize % WORDS.len()];
        let remaining = need - data.len();
        if w.len() <= remaining {
            data.push_str(w);
        } else {
            data.push_str(&w[..remaining]);
            break;
        }
        if data.len() < need {
            data.push(SEPS[next() as usize % SEPS.len()]);
        }
    }
    format!("{prefix}{data}{suffix}")
}

// ⚡ Serializa o envelope AdminEnvelope {type=SEND_TO_CLIENT(1), client_id, payload}.
// Wire format protobuf direto — poucos bytes, sem crate extra de runtime.
// [FLAGS=0x40][tag1 varint][tag2 varint cid][tag3 len+payload]
fn wsm_admin_envelope(client_id: u32, payload: &[u8], flag: u8) -> Vec<u8> {
    fn varint(n: u64, out: &mut Vec<u8>) {
        let mut n = n;
        loop {
            let b = (n & 0x7F) as u8;
            n >>= 7;
            if n == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }
    let mut env = Vec::with_capacity(16 + payload.len());
    // field 1, wire 0 (varint): type = 1 (ACT_SEND_TO_CLIENT)
    varint((1 << 3) | 0, &mut env);
    varint(1, &mut env);
    // field 2, wire 0 (varint): client_id
    if client_id != 0 {
        varint((2 << 3) | 0, &mut env);
        varint(client_id as u64, &mut env);
    }
    // field 3, wire 2 (length-delimited): payload ZSTD
    varint((3 << 3) | 2, &mut env);
    varint(payload.len() as u64, &mut env);
    env.extend_from_slice(payload);

    let mut frame = Vec::with_capacity(env.len() + 1);
    frame.push(flag); // FLAG_ZSTD (0x40) ou 0 (payload cru)
    frame.extend_from_slice(&env);
    frame
}

// ============================================================================
// Carga: mesma lógica de antes (admin + N clientes, pipeline de send_to_client)
// ============================================================================

#[derive(Default)]
struct Stats {
    msgs_ok: AtomicU64,
    connect_failures: AtomicU64,
    send_failures: AtomicU64,
}

async fn admin_session(
    target: Target,
    idx: usize,
    cpd: usize,
    stop: Instant,
    stats: Arc<Stats>,
    server_alive: Option<Arc<std::sync::atomic::AtomicBool>>,
    wsm: bool,
    msg_size: usize,
) {
    let mut ws = match connect_ws(&target).await {
        Some(w) => w,
        None => {
            stats.connect_failures.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    let auth = format!(r#"{{"type":"admin_auth","adminId":"admin{}","password":"admin123"}}"#, idx);
    if ws.send(Message::text(auth)).await.is_err() {
        stats.connect_failures.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let _ = ws.next().await; // welcome

    let mut client_ids = Vec::new();
    for c in 0..cpd {
        let mut cw = match connect_ws(&target).await {
            Some(w) => w,
            None => {
                stats.connect_failures.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let ident = format!(
            r#"{{"type":"identification","data":"android","adminId":"admin{}","androidId":"dev{}-{}","wallpaper":"x"}}"#,
            idx, idx, c
        );
        if cw.send(Message::text(ident)).await.is_err() {
            stats.connect_failures.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if let Some(Ok(Message::Text(t))) = cw.next().await {
            let s = text_str(&t);
            if let Some(rest) = s.split("\"clientId\":").nth(1) {
                if let Some(num) = rest.split(',').next() {
                    if let Ok(cid) = num.trim().parse::<u32>() {
                        client_ids.push(cid);
                    }
                }
            }
        }
        tokio::spawn(async move {
            let mut cw = cw;
            // Drena tudo até o servidor fechar (Close frame → next() retorna None).
            while let Some(Ok(_)) = cw.next().await {}
            // 🔴 Faltava mandar o Close deste lado. O socket do cliente era
            // dropado no chão (RST) → 60s de TIME-WAIT por socket. Com 100+
            // clients por run, acumulava e a run seguinte falhava.
            let _ = cw.send(Message::Close(None)).await;
        });
    }
    if client_ids.is_empty() {
        return;
    }

    let (mut rx, mut tx) = ws.split();
    let stats2 = stats.clone();
    tokio::spawn(async move {
        // ⚡ CORREÇÃO: esta tarefa e o loop de envio (abaixo) compartilham o
        // MESMO `stop`, mas rodam em tasks separadas — sem coordenação, as
        // duas podem cruzar a linha de chegada quase ao mesmo tempo, e se
        // esta aqui sair primeiro e dropar `rx`, o `tx.send()` do loop de
        // envio (ainda em voo) quebra por causa disso, não por causa do
        // servidor. Isso reproduz sozinho o "N quedas de N admins" bem no
        // fim do teste. Dando uma folga aqui, quem sempre sai primeiro é o
        // loop de envio (ele para exatamente em `stop`), e esta tarefa só
        // fecha depois de qualquer envio em voo já ter terminado.
        let stop_reader = stop + Duration::from_millis(500);
        while Instant::now() < stop_reader {
            match tokio::time::timeout(Duration::from_millis(1000), rx.next()).await {
                // ⚡ JSON legado: command_result é texto.
                Ok(Some(Ok(Message::Text(t)))) => {
                    if text_str(&t).starts_with("{\"type\":\"command_result\"") {
                        stats2.msgs_ok.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // ⚡ WSM: command_result é binário. [FLAGS][type=1 (varint tag 1)]
                // tipo 1 == SAT_COMMAND_RESULT — basta checar o byte do tipo.
                Ok(Some(Ok(Message::Binary(b)))) => {
                    // frame = [flags][0x08 <type>] — tag do campo 1 é 0x08
                    if b.len() >= 2 && b[0] & 0x80 == 0 && b[1] == 0x08 && b.get(2) == Some(&1) {
                        stats2.msgs_ok.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(Some(Ok(_))) => {} // Ping/Pong/Close: ignora
                Ok(Some(Err(_))) | Ok(None) => return,
                Err(_) => continue, // timeout de 1s: continua até o stop_reader
            }
        }
    });

    // ⚡ PRÉ-MONTAGEM dos frames (uma vez por admin, fora do loop quente).
    //
    // Por que fora do loop: o objetivo do benchmark é medir o SERVIDOR, não o
    // cliente. Comprimir ZSTD + formatar JSON a cada mensagem consome CPU do
    // load-client, reduz as msgs/s que ele consegue gerar, e pior — poluindo
    // a métrica: a CPU fixa do servidor (tasks de background, sweep de órfãos)
    // passa a ser dividida por MENOS mensagens, inflando artificialmente o
    // "CPU/msg". Pré-montando, ambos os protocolos geram carga na mesma
    // velocidade e a comparação é justa.
    //
    // Em --wsm, cada admin precisa de um frame por clientId (o client_id vai
    // no envelope). São poucos clientes por admin, então o custo é pequeno.
    // O payload ZSTD é computado UMA VEZ e compartilhado entre todos eles.
    let _cid0 = client_ids[0];
    let inner = build_payload(msg_size, 0);
    let raw = inner.as_bytes();
    const ZSTD_MIN: usize = 64;
    // Payload ZSTD compartilhado (uma compressão por admin, não por mensagem).
    let (shared_payload, compressed_flag): (bytes::Bytes, u8) = if wsm && raw.len() >= ZSTD_MIN {
        match zstd::encode_all(raw, 1) {
            Ok(z) if z.len() < raw.len() => (bytes::Bytes::from(z), 0x40),
            _ => (bytes::Bytes::copy_from_slice(raw), 0),
        }
    } else {
        (bytes::Bytes::copy_from_slice(raw), 0)
    };
    // Frame por clientId (WSM) ou frame JSON único (legado).
    // ⚡ AMBOS os modos constroem um frame por cliente — simetria é essencial
    // pro benchmark. Antes o JSON mandava tudo pra um único cliente (cid0)
    // enquanto o WSM distribuía entre todos; o servidor fazia trabalho
    // diferente por protocolo e a comparação não valia nada.
    let frames: Vec<Message> = if wsm {
        client_ids
            .iter()
            .map(|&cid| {
                let env = wsm_admin_envelope(cid, &shared_payload, compressed_flag);
                Message::Binary(bytes::Bytes::from(env))
            })
            .collect()
    } else {
        client_ids
            .iter()
            .map(|&cid| {
                let payload = format!(
                    r#"{{"type":"send_to_client","clientId":{},"message":{}}}"#,
                    cid, inner
                );
                Message::text(payload)
            })
            .collect()
    };
    if frames.is_empty() {
        return;
    }
    // Reporta o tamanho no fio UMA VEZ (só o primeiro admin, não os 500).
    // Antes printava por sessão → 500 linhas idênticas entupindo o terminal.
    if msg_size > 0 && idx == 0 {
        let frame_len = match &frames[0] {
            Message::Binary(b) => b.len(),
            Message::Text(t) => t.len(),
            _ => 0,
        };
        eprintln!(
            "[payload] {}B → frame {}B ({}) — msgs/s de pico sem custo de cliente",
            raw.len(),
            frame_len,
            if compressed_flag != 0 { "ZSTD" } else { "sem compressão" }
        );
    }

    let mut i: usize = 0;
    let mut pending = 0usize;
    let mut last_yield = Instant::now();
    while Instant::now() < stop {
        // ⚡ Se o vigia detectou que o servidor morreu, para de tentar
        // enviar imediatamente em vez de gerar centenas de "quedas durante
        // envio" uma por uma até o timeout natural do loop.
        if let Some(alive) = &server_alive {
            if !alive.load(Ordering::Relaxed) {
                stats.send_failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // ⚡ Envia o frame pré-montado. Bytes é ref-countado: o clone no
        // send() só incrementa um contador atômico — zero memcpy por mensagem.
        let frame = &frames[i % frames.len()];
        i += 1;
        if tx.send(frame.clone()).await.is_err() {
            // 🔴 ANTES: `return` — a sessão do admin MORRIA no primeiro erro
            // e parava de enviar pelo resto do teste. Com 500 admins, isso
            // significava 89% da carga evaporando e o "msgs/s" medindo só os
            // sobreviventes. Número de capacidade totalmente falso.
            //
            // Agora: reconecta (como o app real faz) e CONTIVA enviando. Se a
            // reconexão falhar 3 vezes seguidas ou o tempo acabar, desiste.
            stats.send_failures.fetch_add(1, Ordering::Relaxed);
            let mut reconnected = false;
            for _attempt in 0..3 {
                if Instant::now() >= stop { break; }
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some(mut new_ws) = connect_ws(&target).await {
                    // Reautentica (admin) e reidentifica clientes.
                    let auth = format!(
                        r#"{{"type":"admin_auth","adminId":"admin{idx}","password":"admin123"}}"#
                    );
                    if new_ws.send(Message::text(auth)).await.is_err() { continue; }
                    let _ = new_ws.next().await; // welcome/auth-ok
                    // Reabre a conexão de leitura de respostas (rx antigo morreu
                    // junto com o tx): splita de novo e reinicia o leitor.
                    let (new_rx, new_tx) = new_ws.split();
                    tx = new_tx;
                    let stats3 = stats.clone();
                    // ⚡ Mesma folga do leitor original (ver comentário lá) —
                    // esse aqui também não pode dropar `new_rx` antes do
                    // loop de envio principal parar de tentar usar `tx`.
                    let stop2 = stop + Duration::from_millis(500);
                    tokio::spawn(async move {
                        let mut new_rx = new_rx;
                        while Instant::now() < stop2 {
                            match tokio::time::timeout(Duration::from_millis(1000), new_rx.next()).await {
                                Ok(Some(Ok(Message::Text(t)))) => {
                                    if text_str(&t).starts_with("{\"type\":\"command_result\"") {
                                        stats3.msgs_ok.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Ok(Some(Ok(Message::Binary(b)))) => {
                                    if b.len() >= 2 && b[0] & 0x80 == 0 && b[1] == 0x08 && b.get(2) == Some(&1) {
                                        stats3.msgs_ok.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Ok(Some(Ok(_))) => {}
                                Ok(Some(Err(_))) | Ok(None) => return,
                                Err(_) => continue,
                            }
                        }
                    });
                    reconnected = true;
                    break;
                }
            }
            if !reconnected { return; }
            // Reconectou: reenvia o frame que falhou e segue.
            if tx.send(frame.clone()).await.is_err() { return; }
        }
        pending += 1;
        if pending >= 64 {
            pending = 0;
            tokio::task::yield_now().await;
        }
        if last_yield.elapsed() >= Duration::from_millis(2) {
            last_yield = Instant::now();
            tokio::task::yield_now().await;
        }
    }

    // 🔴 Encerramento LIMPO: manda o Close frame do WebSocket e espera a resposta.
    //
    // Sem isso, o socket é dropado no chão (RST). O kernel põe cada um desses
    // em TIME-WAIT por 60s. Na próxima rodada do benchmark, centenas de
    // conexões novas colidem com esses sockets zumbis e FALHAM — era a causa
    // das "quedas durante envio" e da throughput caindo entre runs
    // (14.9k → 12.6k → 7.0k msgs/s conforme os TIME-WAITs se acumulavam).
    let _ = tx.send(Message::Close(None)).await;
    // Dá meio segundo pro handshake de close completar antes do drop final.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

// ============================================================================
// Orquestração do servidor (equivalente Rust do antigo run_rust2.py)
// ============================================================================

// ⚡ /proc/pid/stat inteiro numa leitura só (utime+stime pra CPU, starttime
// pra verificar identidade do processo — ver `is_same_process` abaixo).
struct ProcStat {
    cpu_ticks: u64,
    starttime: u64,
}

fn read_proc_stat(pid: u32) -> Option<ProcStat> {
    let data = std::fs::read(format!("/proc/{pid}/stat")).ok()?;
    let rp = data.iter().rposition(|&b| b == b')')?;
    let rest = &data[rp + 2..];
    let fields: Vec<&[u8]> = rest.split(|&b| b == b' ').collect();
    // campos do /proc/pid/stat (1-indexado): utime=14, stime=15, starttime=22.
    // Depois de descartar "pid (comm) ", o campo 3 (state) vira índice 0 —
    // então utime=índice 11, stime=índice 12, starttime=índice 19.
    let utime: u64 = std::str::from_utf8(fields.get(11)?).ok()?.parse().ok()?;
    let stime: u64 = std::str::from_utf8(fields.get(12)?).ok()?.parse().ok()?;
    let starttime: u64 = std::str::from_utf8(fields.get(19)?).ok()?.parse().ok()?;
    Some(ProcStat { cpu_ticks: utime + stime, starttime })
}

// ⚡ PIDs são reaproveitados pelo kernel. Se o processo original morreu (ex:
// OOM-kill) e outro processo qualquer nasceu com o MESMO PID antes de lermos
// o CPU final, leríamos o CPU do processo ERRADO — dando números sem
// sentido (foi exatamente o "0.01s de CPU pra 33 mil mensagens" que
// apareceu no teste anterior). `starttime` (campo 22) é o horário de início
// do processo em ticks desde o boot — é o jeito padrão do próprio Linux/ps/
// systemd de diferenciar "mesmo PID, processo diferente".
fn is_same_process(pid: u32, expected_starttime: u64) -> bool {
    match read_proc_stat(pid) {
        Some(s) => s.starttime == expected_starttime,
        None => false, // processo não existe mais
    }
}

// Pergunta ao systemd por que a scope terminou (ex: "oom-kill", "success",
// "signal"). Autoritativo — é o próprio kernel/cgroup que reporta isso pro
// systemd quando o OOM killer age dentro do cgroup da scope.
async fn systemd_scope_result(unit_name: &str) -> Option<String> {
    // ⚠️ stdin null: mesmo motivo do spawn_server — sudo aninhado pedindo
    // senha não pode pendurar o load-client. Ver comentário lá.
    let out = Command::new("sudo")
        .args(["systemctl", "show", &format!("{unit_name}.scope"), "-p", "Result", "--value"])
        .stdin(Stdio::null())
        .output()
        .await
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

// ⚡ Vigia em background: checa a cada 500ms se o servidor ainda é o MESMO
// processo que subimos. Assim que detecta que morreu/mudou, avisa via
// `alive` (os admin_session param de parar de tentar enviar) em vez de
// deixar o teste rodar até o fim gerando centenas de falhas uma por uma.
fn spawn_watchdog(pid: u32, starttime: u64) -> Arc<std::sync::atomic::AtomicBool> {
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let alive2 = alive.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if !is_same_process(pid, starttime) {
                alive2.store(false, Ordering::Relaxed);
                return;
            }
        }
    });
    alive
}

/// Acha o PID do servidor que subimos, não um qualquer.
///
/// ⚠️ Correção de bug: `pgrep -f websocket-server` pega QUALQUER processo com
/// esse nome — incluindo um servidor de uma rodada ANTERIOR que sobreviveu
/// (a mensagem "persistiu apesar de tudo — mate manualmente" do shutdown
/// significa exatamente isso). O load-client então testaria contra o processo
/// ERRADO, ou pior, reportava CPU de um servidor que ia ser morto no final.
///
/// Por isso comparamos o cgroup: o nosso servidor está dentro de
/// `/system.slice/bench-ws-<nossa-pid>.scope`, e qualquer outro
/// `websocket-server` está em outro lugar. Só serve o nosso.
fn find_server_pid(server_bin: &str, our_scope: &str, exclude: &[u32]) -> Option<u32> {
    let name = std::path::Path::new(server_bin)
        .file_name()?
        .to_str()?
        .to_string();
    let out = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(&name)
        .output()
        .ok()?;
    // Se nenhum candidato está na nossa scope, a info de cgroup pode estar
    // indisponível (container, systemd ausente). Nesse caso caímos pro match
    // por exclusão (fallback) — melhor arriscar o PID órfão do que pânico.
    let mut fallback: Option<u32> = None;
    for line in String::from_utf8_lossy(&out.stdout).split_whitespace() {
        let pid: u32 = match line.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        if exclude.contains(&pid) {
            continue;
        }
        let comm = std::fs::read_to_string(format!("/proc/{pid}/comm")).unwrap_or_default();
        let comm = comm.trim();
        if comm == "sudo" || comm.starts_with("systemd-run") {
            continue;
        }
        // Preferência forte: servidor dentro da NOSSA scope systemd-run.
        let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
            .unwrap_or_default();
        if cgroup.contains(our_scope) {
            return Some(pid);
        }
        // Candidato "sem cgroup conhecido" — vale como fallback se nada melhor
        // aparecer (servidor local fora de systemd-run, por exemplo).
        fallback = Some(pid);
    }
    fallback
}

struct ServerHandle {
    wrapper: Child,
    unit_name: String,
    server_pid: u32,
    starttime: u64,
}

async fn spawn_server(
    server_bin: &str,
    cpu_quota: &str,
    mem_max: &str,
    host: &str,
    port: u16,
) -> ServerHandle {
    // 🔴 CORREÇÃO DO BUG "CPU 0.28µs/msg + 395 quedas + PID repetido".
    //
    // Se a porta já está aceitando conexões ANTES de subirmos nada, existe um
    // servidor ZUMBI de uma rodada anterior vivo lá (shutdown falhou, SIGKILL
    // ignorado, etc.). Sintomas vistos no campo: PID repetido entre runs,
    // CPU do servidor ~0 (o zumbi está parado/throttled) e milhares de "quedas
    // durante envio".
    //
    // Sem esta checagem o novo servidor tenta o bind, FALHA ("Address already
    // in use"), morre silenciosamente — e o health check da porta lá embaixo
    // conecta no ZUMBI e报告a "pronto". O teste inteiro então roda contra o
    // processo ERRADO, e a CPU medida é lixo.
    //
    // Detectamos aqui e abortamos com as instruções exatas de limpeza.
    if tokio::net::TcpStream::connect((host, port)).await.is_ok() {
        eprintln!(
            "⛔ PORTA {host}:{port} JÁ ESTÁ OCUPADA por um servidor de uma rodada \
             anterior (zumbi). O novo servidor não conseguiria fazer bind e o \
             teste mediria o processo errado — é exatamente o bug do \
             'CPU 0.28 us/msg' e do PID repetido entre runs.\n\
             \nLimpe antes de rodar:\n\
             \n  sudo pkill -9 -f '{server_bin}'\n\
             \n  sudo systemctl stop 'bench-ws-*.scope'\n\
             \nDepois rode o bench de novo."
        );
        std::process::exit(2);
    }

    let unit_name = format!("bench-ws-{}", std::process::id());
    // ⚠️ stdin NULL de propósito: se este sudo aninhado (chamado de dentro de
    // outro sudo) decidir pedir senha — cache do sudo expirou, NOPASSWD sumiu,
    // etc. — ele lê do stdin. Herdando o stdin do terminal/pipe, ele fica
    // esperando uma senha que nunca chega e o load-client trava PRA SEMPRE
    // (motivo pelo qual antes era preciso `pkill -9 load-client` antes de um
    // segundo teste). Com stdin null, sudo sem senha falha IMEDIATAMENTE em
    // vez de pendurar.
    let mut wrapper = Command::new("sudo")
        .args([
            "systemd-run",
            "--scope",
            "-p",
            &format!("CPUQuota={cpu_quota}"),
            "-p",
            &format!("MemoryMax={mem_max}"),
            "--unit",
            &unit_name,
            "--collect",
            server_bin,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("falha ao executar sudo systemd-run (precisa de sudo sem senha configurado, ou rode como root)");

    let wrapper_pid = wrapper.id().unwrap_or(0);
    let scope_name = format!("bench-ws-{}", std::process::id());
    let deadline = Instant::now() + Duration::from_secs(15);
    let (server_pid, starttime) = loop {
        if let Some(pid) = find_server_pid(server_bin, &scope_name, &[std::process::id(), wrapper_pid]) {
            if let Some(stat) = read_proc_stat(pid) {
                break (pid, stat.starttime);
            }
        }
        if Instant::now() > deadline {
            let _ = wrapper.kill().await;
            panic!("não achei o PID do servidor em 15s — ele subiu mesmo? Veja `journalctl -u {unit_name}`");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    // 🔴 ÚLTIMA CORREÇÃO DA FALHA "500 falhas de conexão em 0.04s".
    //
    // Achar o PID NÃO significa que o servidor está pronto. Entre o kernel
    // criar o processo e o Rust terminar o bind()+listen() no tokio, rola
    // inicialização (runtime, config, logs). O PID existe nessa janela, mas
    // a porta REJEITA conexões — connect() retorna ECONNREFUSED instantâneo.
    //
    // O sintoma era exatamente o que você viu: "msgs=0 elapsed=0.04s, 500
    // falhas" e o banner do servidor ("Escutando em ws://0.0.0.0:8080")
    // aparecia no log DEPOIS do teste já ter falhado. O load-client largava
    // as 500 conexões na janela exata em que a porta ainda não existia.
    //
    // Agora: TCP connect real (mesmo host:porta do teste) até aceitar.
    // Critério de parada é o SOCKET aceitar conexões, não o processo existir.
    //
    // ⚠️ ARMADILHA do "servidor zumbi": se um servidor de rodada anterior
    // ainda segura a porta, o NOVO servidor morre no bind ("Address already
    // in use") — mas o connect() abaixo SUCEDE porque o ZUMBI atende. O teste
    // roda contra o processo errado e a CPU medida é lixo (vimos 0.28µs/msg,
    // impossível). Por isso a checagem de porta-ocupada lá em cima; e aqui,
    // desconfiamos se o nosso processo sumiu.
    let port_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match tokio::net::TcpStream::connect((host, port)).await {
            Ok(_) => break, // porta aceitando — servidor pronto de verdade
            Err(e) => {
                // O nosso servidor morreu no meio do caminho? Checagem extra:
                // sem essa, um bind-falho fica invisível até o deadline de 30s.
                if read_proc_stat(server_pid).is_none() {
                    let _ = wrapper.kill().await;
                    panic!(
                        "servidor (pid={server_pid}) MORREU antes de aceitar conexões em \
                         {host}:{port}. Causa mais provável: a porta já estava em uso \
                         (bind falhou — 'Address already in use'), ou o binário travou no \
                         startup. Veja os logs acima.\n\
                         \nSe há um servidor zumbi de uma rodada anterior:\n\
                         \n  sudo pkill -9 -f '{server_bin}'\n\
                         \n  sudo systemctl stop 'bench-ws-*.scope'"
                    );
                }
                if Instant::now() > port_deadline {
                    let _ = wrapper.kill().await;
                    panic!(
                        "servidor (pid={server_pid}) não aceitou conexões em {host}:{port} \
                         após 30s: {e}. O bind falhou? Porta em uso por outro processo? \
                         Tente: sudo systemctl stop '{}.scope' ou pkill -9 -f '{server_bin}'",
                        unit_name
                    );
                }
                // Ainda não está pronto — dá tempo de terminar o bind e tenta
                // de novo. Polling agressivo (100ms) porque a janela é curta.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    ServerHandle { wrapper, unit_name, server_pid, starttime }
}

impl ServerHandle {
    /// ⚡ Encerra o servidor de forma que NUNCA pendura o load-client.
    ///
    /// Duas falhas corrigidas aqui:
    ///
    /// 1. **sudo pode pedir senha** — sudo aninhado (chamado de dentro de outro
    ///    sudo) pode perder o cache e pedir senha. Com stdin herdado ele espera
    ///    pra sempre. Agora: `stdin(Stdio::null())` + timeout duro em volta do
    ///    `systemctl stop`. Ou o sudo responde rápido, ou desistimos e vamos
    ///    pra via brutal (kill), mas em NENHUM caso ficamos presos.
    ///
    /// 2. **confiança cega no systemctl** — antes o código só checava o código
    ///    de saída do `systemctl stop`. Isso NÃO garante que o processo morreu:
    ///    o systemd pode ter demorado, a scope pode ter ignorado o sinal, ou o
    ///    sudo pode ter falhado (senha) e o erro era ignorado (`let _ =`).
    ///    Agora POLLAMOS /proc/PID/stat até o processo sumir de verdade (ou
    ///    deadline), e se ainda estiver vivo, damos SIGKILL no wrapper e no PID.
    async fn shutdown(mut self) {
        // Via 1: systemctl stop (educada), com stdin null e timeout de 10s.
        let stop_ok = tokio::time::timeout(
            Duration::from_secs(10),
            Command::new("sudo")
                .args(["systemctl", "stop", &format!("{}.scope", self.unit_name)])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        )
        .await
        .map(|r| r.map(|s| s.success()).unwrap_or(false))
        .unwrap_or(false); // timeout → trata como falha e escala pra kill

        // Via 2: confirmar de VERDADE que o processo morreu. Não confiamos no
        // código de saída do systemctl — olhamos o /proc. is_same_process
        // checa a identidade (PID + starttime), então um PID reaproveitado por
        // outro processo não nos engana.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut died = false;
        while Instant::now() < deadline {
            if !is_same_process(self.server_pid, self.starttime) {
                died = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Via 3: ainda vivo? Mata na marra. Importante: matar o wrapper (sudo)
        // NÃO mata a scope — systemd-run --scope só é o pai, o servidor continua
        // vivo dentro do cgroup. Por isso o SIGKILL vai DIRETO no PID do
        // servidor. Sem isso o servidor zumbi sobrevive, segura a porta e
        // corrompe a próxima rodada (o bug do "CPU 0.28µs/msg").
        if !died {
            eprintln!(
                "⚠️  servidor pid={} ainda vivo após systemctl stop (ok={stop_ok}); forçando SIGKILL",
                self.server_pid
            );
            // Primeiro o servidor em si (é o que segura a porta):
            let _ = Command::new("kill")
                .arg("-9")
                .arg(self.server_pid.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            // Depois o wrapper, pra recolher a scope inteira:
            let _ = self.wrapper.kill().await;
            // Última chance: se o kill direto não funcionou (sem permissão?),
            // sudo kill -9 é a via final.
            if is_same_process(self.server_pid, self.starttime) {
                let _ = Command::new("sudo")
                    .args(["kill", "-9", &self.server_pid.to_string()])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }

        // Em qualquer caso, recolhe o wrapper pra não deixar zumbi.
        let _ = self.wrapper.wait().await;

        // Verificação final: processo realmente se foi?
        if !is_same_process(self.server_pid, self.starttime) {
            died = true;
        }
        if !died {
            eprintln!(
                "❌ servidor pid={} persistiu apesar de tudo — mate manualmente: \
                 sudo systemctl stop {}.scope (ou kill -9 {})",
                self.server_pid, self.unit_name, self.server_pid
            );
        }
    }
}

// ============================================================================
// main
// ============================================================================

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // ⚡ rustls 0.23: precisa instalar um provedor de criptografia explicitamente
    // antes de qualquer handshake TLS, mesmo com a feature "ring" habilitada no
    // Cargo.toml. Sem isso, toda conexão wss:// falha em runtime.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Args::parse();
    let target = parse_target(&args.url);

    println!(
        "[config] url={} (host={} porta={} tls={}) admins={} clientes/admin={} duracao={}s wsm={} msg_size={}B",
        args.url, target.host, target.port, target.tls, args.admins, args.clients_per_admin, args.duration, args.wsm, args.msg_size
    );

    // ⚡ Só sobe/mede o servidor se --server-bin foi passado. Contra um
    // servidor remoto (produção), não tem processo local pra amostrar CPU.
    let server_handle = if let Some(bin) = &args.server_bin {
        println!("[run] subindo servidor local via systemd-run (CPUQuota={} MemoryMax={})...", args.cpu_quota, args.mem_max);
        let h = spawn_server(bin, &args.cpu_quota, &args.mem_max, &target.host, target.port).await;
        println!("[run] servidor pid={} (wrapper pid={:?}) — porta {} pronta", h.server_pid, h.wrapper.id(), target.port);
        Some(h)
    } else {
        println!("[run] sem --server-bin: assumindo que {} já está rodando em outro lugar", args.url);
        None
    };

    // ⚡ Vigia: só existe quando subimos o servidor nós mesmos. Ele monitora
    // a IDENTIDADE do processo (não só se "algum PID" existe), então detecta
    // tanto "morreu" quanto "morreu e o PID foi reaproveitado por outro
    // processo" — os dois casos em que confiar no CPU final seria enganoso.
    let server_alive = server_handle
        .as_ref()
        .map(|h| spawn_watchdog(h.server_pid, h.starttime));

    let cpu0 = server_handle.as_ref().and_then(|h| read_proc_stat(h.server_pid)).map(|s| s.cpu_ticks);
    let t0 = Instant::now();

    let stats = Arc::new(Stats::default());
    let stop = Instant::now() + Duration::from_secs(args.duration);
    let mut handles = Vec::new();
    for a in 0..args.admins {
        handles.push(tokio::spawn(admin_session(
            target.clone(),
            a,
            args.clients_per_admin,
            stop,
            stats.clone(),
            server_alive.clone(),
            args.wsm,
            args.msg_size,
        )));
        if a % args.ramp_batch == args.ramp_batch - 1 {
            tokio::task::yield_now().await;
        }
    }
    for h in handles {
        let _ = h.await;
    }

    let elapsed = t0.elapsed().as_secs_f64();

    let total = stats.msgs_ok.load(Ordering::Relaxed);
    let conn_fail = stats.connect_failures.load(Ordering::Relaxed);
    let send_fail = stats.send_failures.load(Ordering::Relaxed);
    let mps = total as f64 / elapsed;

    println!("\n=== RESULTADO ===");
    println!("msgs={total} elapsed={elapsed:.2}s -> {mps:.0} msgs/s");
    if conn_fail > 0 || send_fail > 0 {
        eprintln!(
            "⚠️  {conn_fail} falhas de conexão + {send_fail} quedas durante envio (de {} admins x {} clientes esperados)",
            args.admins, args.clients_per_admin
        );
        eprintln!("    Se esse número for alto (e o servidor NÃO morreu, ver abaixo), rode `ulimit -n 65536` antes deste comando.");
    }

    // ⚡ Checagem final e AUTORITATIVA de identidade — independente do vigia
    // já ter percebido ou não durante o teste. Só confiamos nas métricas de
    // CPU se o processo no fim é comprovadamente o MESMO que start amos.
    let server_ok = match &server_handle {
        Some(h) => is_same_process(h.server_pid, h.starttime),
        None => false, // sem --server-bin, nunca teve CPU local pra medir mesmo
    };

    if let Some(h) = &server_handle {
        if !server_ok {
            eprintln!("\n🔴 O servidor morreu (ou o PID foi reaproveitado por outro processo) durante o teste.");
            eprintln!("   As métricas de CPU abaixo NÃO seriam confiáveis, então foram omitidas de propósito.");
            eprintln!("   msgs/s acima ainda reflete o que foi processado de verdade antes da queda.");
            if let Some(result) = systemd_scope_result(&h.unit_name).await {
                eprintln!("   systemd Result da scope '{}': {result}", h.unit_name);
                if result.to_lowercase().contains("oom") {
                    eprintln!(
                        "   ⚠️  OOM-KILL confirmado: o servidor excedeu MemoryMax={} e o kernel matou o processo.",
                        args.mem_max
                    );
                }
            } else {
                eprintln!("   (não consegui consultar o systemd pra saber a causa — rode `journalctl -u {}.scope` manualmente)", h.unit_name);
            }
        } else if let Some(c0) = cpu0 {
            if let Some(c1) = read_proc_stat(h.server_pid).map(|s| s.cpu_ticks) {
                let clk = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
                let clk = if clk > 0 { clk as f64 } else { 100.0 };
                let cpu_secs = (c1.saturating_sub(c0)) as f64 / clk;
                let cpu_pct = cpu_secs / elapsed * 100.0;
                println!("CPU do servidor:       {cpu_secs:.2}s ({cpu_pct:.0}% de 1 core)");
                if total > 0 && cpu_secs > 0.0 {
                    let cpu_per_msg_us = cpu_secs / total as f64 * 1e6;
                    println!("CPU por mensagem:      {cpu_per_msg_us:.2} us/msg   <== CHAVE");
                    let predicted = 0.1 / (cpu_per_msg_us * 1e-6);
                    println!("=> throughput previsto @0.1 CPU: {predicted:.0} msgs/s");
                } else {
                    println!("(CPU medido ficou em ~0 — teste rodou rápido/pouca carga demais pra medir; aumente --duration ou --admins)");
                }
            }
        }
    }

    if let Some(h) = server_handle {
        println!("[run] encerrando servidor...");
        h.shutdown().await;
    }
}
