//! ⚡ Protocolo binário WSM (WebSocket Manager): envelope Protobuf + payload ZSTD.
//!
//! ## Por que isso existe
//!
//! O servidor é um **roteador**, não um interpretador de comandos. Para
//! encaminhar `send_to_client` ele só precisa de duas informações: **o tipo**
//! da mensagem e **o clientId** do destino. O conteúdo do comando — o JSON que
//! o app admin montou e que o cliente Android vai executar — é opaco pro
//! servidor.
//!
//! Aproveitamos isso: o payload viaja como **bytes crus comprimidos com
//! ZSTD** dentro de um envelope Protobuf. O servidor:
//!   1. Faz parse do **envelope** (µs — poucos campos, sem string parsing)
//!   2. Copia o **payload** byte a byte pro socket do destino
//!   3. **Nunca** descomprime, nunca desserializa, nunca aloca árvore JSON
//!
//! A compressão/decompressão ZSTD acontece **nas pontas** (admin app e cliente
//! Android), onde tem CPU sobrando. O servidor, que é o gargalo (CPUQuota=10%),
//! não paga nada.
//!
//! ## Formato no fio (WebSocket binary frame)
//!
//! ```text
//! [1 byte: FLAGS][envelope protobuf serializado]
//! ```
//!
//! FLAGS:
//! - bit 7 (`0x80`): **JSON legado**. Mensagem inteira é texto JSON (backward
//!   compat com clientes antigos). O servidor detecta pelo byte 0 == `'{'`.
//! - bit 6 (`0x40`): **payload comprimido com ZSTD**.
//! - bits 0-5: reservado (0).
//!
//! ## Quando o servidor PRECISA desserializar
//!
//! Mensagens de **controle** (autenticação, identificação, listas, kick,
//! status) continuam em **JSON puro** — o servidor precisa ler seus campos.
//! Comprimir essas mensagens só adicionaria custo de descompressão sem ganho
//! (elas são pequenas). É exatamente o trade-off: ZSTD só onde o servidor
//! **não** precisa descomprimir.

// Tipos protobuf gerados pelo prost-build a partir de proto/wsm.proto.
// Incluídos diretamente neste módulo (wsm.rs é @generated, não versionado).
// O trait `prost::Message` (decode/encode) é importado em main.rs, onde os
// tipos são usados de fato.
include!(concat!(env!("OUT_DIR"), "/wsm.rs"));

/// Bit de flag: mensagem JSON legada (não-protobuf).
pub const FLAG_JSON: u8 = 0x80;
/// Bit de flag: payload (campo `bytes` do envelope) está sob ZSTD.
pub const FLAG_ZSTD: u8 = 0x40;

/// Threshold: payloads menores que isto **não são comprimidos**. ZSTD level 1
/// tem overhead fixo (~centenas de ns + header) que supera o ganho em payloads
/// pequenos. Comandos típicos (screenshot, location, mic) cabem abaixo disso;
/// file lists e screenshots comprimidos ficam bem acima.
pub const ZSTD_MIN_COMPRESS: usize = 64;
/// Level 1: prioriza velocidade (a diferença pro level 9 é <5% em ratio mas
/// ~3x em CPU). As pontas têm CPU sobrando, mas não vamos desperdiçar.
pub const ZSTD_LEVEL: i32 = 1;

/// Verdadeiro se o frame começa com `{` → mensagem JSON legada (sem flags).
/// É a forma do servidor decidir o caminho sem ambiguidade: nenhum envelope
/// protobuf válido começa com 0x7B (primeiro byte seria uma tag de campo
/// varint/length-delimited, nunca `'{'`).
#[inline]
pub fn is_json_frame(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes[0] == b'{'
}

/// Serializa um envelope protobuf prefixando o byte de FLAGS.
/// `compressed` indica se o payload (campo 3) já veio sob ZSTD.
#[inline]
pub fn encode_envelope(msg: &impl prost::Message, compressed: bool) -> Vec<u8> {
    // Pré-aloca o tamanho do envelope + 1 byte de flags. protobuf::encode()
    // faz exatamente uma passada e só aloca uma vez (buffer cresce por
    // realocação amortizada, mas com a estimativa correta raramente realoca).
    let len = msg.encoded_len();
    let mut out = Vec::with_capacity(len + 1);
    out.push(if compressed { FLAG_ZSTD } else { 0 });
    // encode_vec não está disponível em todas as versões; usamos o encode()
    // direto no buffer, que é o caminho mais rápido (zero-copy pro Vec).
    msg.encode(&mut out).expect("envelope protobuf válido");
    out
}

// ---------------------------------------------------------------------------
// ⚡ FAST PATH: ServerToClient::ClientCommand com UMA ÚNICA cópia do payload
// ---------------------------------------------------------------------------

/// Escreve um varint no buffer (Little-Endian Base-128).
#[inline]
fn push_varint(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let byte = (n & 0x7F) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Monta um frame `[FLAGS][envelope ServerToClient{CLIENT_COMMAND, client_id, payload}]`
/// com **uma única cópia** dos bytes do payload.
///
/// Por que isto existe separado do `encode_envelope` genérico: o caminho genérico
/// recebe uma struct `ServerToClient` cujo campo `payload: Vec<u8>` JÁ é uma
/// cópia dos bytes, e o `prost::Message::encode` os copia DE NOVO pro buffer
/// de saída. São duas cópias do payload por mensagem. Em payloads pequenos é
/// imperceptível, mas em 4KB+ o custo do memcpy duplicado domina o CPU/msg do
/// servidor (~159µs vs ~118µs do caminho JSON nesta carga).
///
/// Aqui escrevemos o cabeçalho (type + client_id + tag + len) direto e
/// estendemos o buffer com o payload de UMA vez — mesma contagem de cópias que
/// o caminho JSON (`Bytes::copy_from_slice`), só que com um cabeçalho muito
/// menor e bytes potencialmente comprimidos.
#[inline]
pub fn encode_server_command(client_id: u32, payload: &[u8], compressed: bool) -> Vec<u8> {
    // Cabeçalho: 1 (flags) + 2 (field1 type=2) + até 5 (field2 client_id) +
    // 1 (field3 tag) + até 5 (varint len) = no máximo 14 bytes antes do payload.
    let mut out = Vec::with_capacity(14 + payload.len());
    out.push(if compressed { FLAG_ZSTD } else { 0 });
    // field 1, wire 0 (varint): type = CLIENT_COMMAND (2)
    push_varint(&mut out, (1 << 3) | 0);
    push_varint(&mut out, ServerClientType::SctClientCommand as u64);
    // field 2, wire 0 (varint): client_id (proto3 omite se 0)
    if client_id != 0 {
        push_varint(&mut out, (2 << 3) | 0);
        push_varint(&mut out, client_id as u64);
    }
    // field 3, wire 2 (length-delimited): payload
    push_varint(&mut out, (3 << 3) | 2);
    push_varint(&mut out, payload.len() as u64);
    // ⚡ UMA cópia do payload — extend_from_slice é um memcpy único.
    out.extend_from_slice(payload);
    out
}

/// Fast path para broadcast (ServerToClient sem client_id — vai pra todos os
/// clientes do admin). Mesma lógica de cópia única do `encode_server_command`.
#[inline]
pub fn encode_server_broadcast(payload: &[u8], compressed: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(10 + payload.len());
    out.push(if compressed { FLAG_ZSTD } else { 0 });
    // field 1: type = CLIENT_COMMAND (2)
    push_varint(&mut out, (1 << 3) | 0);
    push_varint(&mut out, ServerClientType::SctClientCommand as u64);
    // field 3: payload
    push_varint(&mut out, (3 << 3) | 2);
    push_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    out
}

/// Fast path pra resposta command_result (ServerToAdmin). Uma alocação no
/// startup, zero por mensagem. Como success e message são fixos, os bytes do
/// frame são sempre os mesmos — quem chama guarda o resultado num `Bytes`
/// ref-countado e só clona (contador atômico) por mensagem.
#[inline]
pub fn encode_command_result(success: bool, message: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + message.len());
    out.push(0); // flags: resposta de controle, sem payload, sem ZSTD
    // field 1: type = COMMAND_RESULT (SatCommandResult = 1)
    push_varint(&mut out, (1 << 3) | 0);
    push_varint(&mut out, ServerAdminType::SatCommandResult as u64);
    // field 4: success (bool — proto3 omite se false)
    if success {
        push_varint(&mut out, (4 << 3) | 0);
        push_varint(&mut out, 1);
    }
    // field 5: message (string)
    if !message.is_empty() {
        push_varint(&mut out, (5 << 3) | 2);
        push_varint(&mut out, message.len() as u64);
        out.extend_from_slice(message.as_bytes());
    }
    out
}

/// Fast path pra resposta client→admin (ServerToAdmin::CLIENT_MESSAGE).
/// Uma cópia do payload; o ZSTD é repassado cru pra ponta admin descomprimir.
#[inline]
pub fn encode_client_message(client_id: u32, payload: &[u8], compressed: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(14 + payload.len());
    out.push(if compressed { FLAG_ZSTD } else { 0 });
    // field 1: type = CLIENT_MESSAGE (SatClientMessage = 2)
    push_varint(&mut out, (1 << 3) | 0);
    push_varint(&mut out, ServerAdminType::SatClientMessage as u64);
    // field 2: client_id
    if client_id != 0 {
        push_varint(&mut out, (2 << 3) | 0);
        push_varint(&mut out, client_id as u64);
    }
    // field 3: payload
    push_varint(&mut out, (3 << 3) | 2);
    push_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------------
// Helpers de payload ZSTD — usados pelas PONTAS (server não chama no hot path
// de roteamento; o server só usa zstd quando um cliente legado envia JSON e
// precisamos empacotar pra um admin novo, ou vice-versa).
// ---------------------------------------------------------------------------

/// Comprime só se valer a pena (>= ZSTD_MIN_COMPRESS). Caso contrário,
/// devolve os bytes originais — o envelope marca a flag apropriadamente.
#[inline]
pub fn maybe_compress(data: &[u8]) -> (Vec<u8>, bool) {
    if data.len() >= ZSTD_MIN_COMPRESS {
        match zstd::encode_all(data, ZSTD_LEVEL) {
            Ok(c) if c.len() < data.len() => return (c, true),
            _ => {}
        }
    }
    (data.to_vec(), false)
}

/// Descomprime se a flag ZSTD estiver ligada, senão devolve cru.
#[inline]
pub fn maybe_decompress(data: &[u8], compressed: bool) -> Vec<u8> {
    if compressed {
        zstd::decode_all(data).unwrap_or_else(|_| data.to_vec())
    } else {
        data.to_vec()
    }
}
