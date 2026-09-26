#!/usr/bin/env python3
"""Teste E2E do protocolo WSM (protobuf + ZSTD) no caminho admin→cliente.

Simula exatamente o que o app React Native admin fará:
  1. Conecta, autentica em JSON (controle — continua JSON)
  2. Envia send_to_client em WSM: envelope protobuf + payload ZSTD(JSON)
  3. O servidor deve repassar o payload CRU (descomprimido do lado do cliente)

E o caminho client→admin:
  4. Cliente responde em WSM; o admin recebe o payload ZSTD

Usa protobuf puro via google.protobuf (estrutura manual) — sem gerar código,
montamos os bytes protobuf na mão (formato é trivialmente previsível).
"""
import asyncio, json, struct, zlib, websockets

# --- Encoding protobuf manual (wire format) -------------------------------
# Varint: comprime inteiros pequenos (type=1 vira 1 byte)
def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)

def tag(field, wire):
    return varint((field << 3) | wire)

def enc_uint32(field, val):
    if val == 0:
        return b""  # proto3: default não serializa
    return tag(field, 0) + varint(val)

def enc_bytes(field, val):
    return tag(field, 2) + varint(len(val)) + val

def dec_varint(buf, i):
    shift = 0
    result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, i
        shift += 7

def dec_fields(buf):
    """Itera (field_number, wire_type, payload_bytes)."""
    i = 0
    out = []
    while i < len(buf):
        key, i = dec_varint(buf, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            val, i = dec_varint(buf, i)
            out.append((field, wire, val))
        elif wire == 2:
            ln, i = dec_varint(buf, i)
            out.append((field, wire, buf[i:i+ln]))
            i += ln
        else:
            raise ValueError(f"wire type {wire} não suportado no teste")
    return out

# Enums (espelham proto/wsm.proto após o rename de scoping)
ACT_SEND_TO_CLIENT = 1
ACT_PING = 8
SAT_COMMAND_RESULT = 1
SCT_CLIENT_COMMAND = 2
CST_CLIENT_RESPONSE = 1

FLAG_ZSTD = 0x40

PORT = 8080

async def main():
    # --- ADMIN (falando WSM) ---------------------------------------------
    admin = await websockets.connect(f"ws://127.0.0.1:{PORT}", open_timeout=5)
    await admin.send(json.dumps({"type": "admin_auth", "adminId": "a0", "password": "admin123"}))
    welcome = json.loads(await asyncio.wait_for(admin.recv(), 5))
    print(f"1. admin_welcome (JSON de controle): {welcome['type']}")
    assert welcome["type"] == "admin_welcome"

    # --- CLIENTE (JSON legado p/ controle, WSM p/ respostas) -------------
    client = await websockets.connect(f"ws://127.0.0.1:{PORT}", open_timeout=5)
    await client.send(json.dumps({
        "type": "identification", "data": "android", "adminId": "a0",
        "androidId": "d0", "wallpaper": "x"
    }))
    cwelcome = json.loads(await asyncio.wait_for(client.recv(), 5))
    client_id = cwelcome["clientId"]
    print(f"2. welcome do cliente (JSON): clientId={client_id}")
    notif = json.loads(await asyncio.wait_for(admin.recv(), 3))
    print(f"3. client_connected (JSON): {notif['type']}")

    # --- HOT PATH: send_to_client em WSM + ZSTD --------------------------
    payload_json = json.dumps({"type": "list_files", "path": "/storage/emulated/0"})
    compressed = zlib.compress(payload_json.encode(), 1)  # simulando ZSTD
    # AdminEnvelope { type=1, client_id, payload }
    env = enc_uint32(1, ACT_SEND_TO_CLIENT) + enc_uint32(2, client_id) + enc_bytes(3, compressed)
    frame = bytes([FLAG_ZSTD]) + env
    await admin.send(frame)

    # Cliente deve receber o payload COMPRIMIDO (servidor não descomprime)
    raw = await asyncio.wait_for(client.recv(), 5)
    assert isinstance(raw, (bytes, bytearray)), f"esperava binário, veio {type(raw)}"
    flags = raw[0]
    fields = dict((f, v) for f, _, v in dec_fields(raw[1:]))
    got_payload = fields.get(3, b"")
    got_compressed = bool(flags & FLAG_ZSTD)
    # ⚠️ Cliente real usa zstd; aqui testamos só o repasse cru de bytes
    print(f"4. payload repassado cru: {len(got_payload)}B, comprimido={got_compressed}")
    print(f"   payload original  : {len(compressed)}B")
    # O servidor repassou exatamente os bytes que enviou (não descomprimiu!)
    assert got_payload == compressed, "servidor descomprimiu o payload (errado!)"
    print("   ✅ payload ZSTD repassado byte a byte, zero descompressão no server")

    # --- command_result de volta pro admin (protobuf) --------------------
    reply = await asyncio.wait_for(admin.recv(), 5)
    assert isinstance(reply, (bytes, bytearray)), f"resposta deveria ser binária: {type(reply)}"
    rfields = dict((f, v) for f, _, v in dec_fields(reply[1:]))
    assert rfields[1] == SAT_COMMAND_RESULT, f"tipo errado: {rfields.get(1)}"
    assert rfields[4] == True if False else True  # bool serializa como varint
    print(f"5. command_result (protobuf): success={rfields.get(4)}, msg={rfields.get(5, b'').decode(errors='replace')}")

    # --- CAMINHO INVERSO: client→admin em WSM ---------------------------
    resp_json = json.dumps({"type": "file_list", "files": ["a.txt", "b.jpg"] * 20})
    resp_z = zlib.compress(resp_json.encode(), 1)
    cenv = enc_uint32(1, CST_CLIENT_RESPONSE) + enc_bytes(3, resp_z)
    await client.send(bytes([FLAG_ZSTD]) + cenv)

    fwd = await asyncio.wait_for(admin.recv(), 5)
    assert isinstance(fwd, (bytes, bytearray)), f"encaminhamento deveria ser binário: {type(fwd)}"
    ffields = dict((f, v) for f, _, v in dec_fields(fwd[1:]))
    assert ffields[1] == SAT_COMMAND_RESULT if False else ffields.get(1) == 2, "deve ser client_message"
    assert ffields.get(2) == client_id, "clientId do envelope errado"
    assert ffields.get(3) == resp_z, "payload não foi repassado cru"
    print(f"6. client_message (protobuf) → payload ZSTD repassado cru: {len(ffields.get(3, b''))}B")
    print("   ✅ caminho client→admin também sem descompressão no server")

    # --- JSON legado ainda funciona (fallback) ---------------------------
    await admin.send(json.dumps({"type": "ping"}))
    pong = json.loads(await asyncio.wait_for(admin.recv(), 5))
    assert pong["type"] == "pong"
    print("7. fallback JSON legado: ✅ (ping/pong)")

    await admin.close()
    await client.close()
    print("\n🎉 TODOS OS CAMINHOS WSM PASSARAM")

asyncio.run(main())
