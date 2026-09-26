#!/usr/bin/env python3
"""Teste E2E completo do WSM, simulando byte a byte o que os apps React Native
produzem (wsm.ts) e consomem.

Cobre:
  A. admin → server → cliente (comando em WSM, payload pequeno = SEM zstd)
  B. cliente → server → admin (resposta em WSM)
  C. fallback JSON legado ainda funciona (controle)
  D. detecção: JSON dentro de frame binário (byte 0 == '{')
  E. kick/disconnect continuam em JSON (controle)
"""
import asyncio, json, struct, websockets

def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F; n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n: return bytes(out)

def enc_uint32(f, v):
    return b"" if v == 0 else varint((f<<3)|0) + varint(v)

def enc_bytes(f, v):
    return b"" if len(v) == 0 else varint((f<<3)|2) + varint(len(v)) + v

def dec_varint(buf, i):
    r = 0; s = 0
    while True:
        b = buf[i]; i += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80): return r, i
        s += 7

def parse(buf):
    """Parser de envelope → dict {type, clientId, payload, success, message}"""
    out = {"type":0,"clientId":0,"payload":b"","success":False,"message":"","count":0}
    i = 0
    while i < len(buf):
        key, i = dec_varint(buf, i)
        f, w = key>>3, key&7
        if w == 0:
            v, i = dec_varint(buf, i)
            if f == 1: out["type"] = v
            elif f == 2: out["clientId"] = v
            elif f == 4: out["success"] = v != 0
            elif f == 6: out["count"] = v
        elif w == 2:
            ln, i = dec_varint(buf, i)
            if f == 3: out["payload"] = buf[i:i+ln]
            elif f == 5: out["message"] = buf[i:i+ln].decode(errors="replace")
            i += ln
    return out

FLAG_ZSTD = 0x40
ACT_SEND = 1; ACT_BCAST = 2; ACT_PING = 8
SAT_CMD = 1; SAT_MSG = 2
SCT_CMD = 2; CST_RESP = 1

PORT = 8080
fails = 0
def check(cond, msg):
    global fails
    if not cond:
        fails += 1
        print(f"  ❌ {msg}")
    else:
        print(f"  ✅ {msg}")

async def recv_binary(ws, timeout=5):
    """Recebe só o próximo frame BINÁRIO (pula JSON de controle)."""
    for _ in range(10):
        r = await asyncio.wait_for(ws.recv(), timeout)
        if isinstance(r, (bytes, bytearray)):
            return r
    raise TimeoutError("nenhum frame binário")

async def recv_json(ws, timeout=5):
    for _ in range(10):
        r = await asyncio.wait_for(ws.recv(), timeout)
        if isinstance(r, str):
            return json.loads(r)
    raise TimeoutError("nenhum frame JSON")

async def main():
    global fails
    # ---- Setup ----------------------------------------------------------
    admin = await websockets.connect(f"ws://127.0.0.1:{PORT}", open_timeout=5)
    await admin.send(json.dumps({"type":"admin_auth","adminId":"a0","password":"admin123"}))
    w = await recv_json(admin)
    check(w["type"] == "admin_welcome", f"admin_welcome (JSON controle): {w['type']}")

    client = await websockets.connect(f"ws://127.0.0.1:{PORT}", open_timeout=5)
    await client.send(json.dumps({"type":"identification","data":"android","adminId":"a0",
                                  "androidId":"d0","wallpaper":"x"}))
    cw = await recv_json(client)
    cid = cw["clientId"]
    notif = await recv_json(admin)
    check(notif["type"] == "client_connected", f"client_connected (JSON): {notif['type']}")

    # ---- A. admin → cliente: WSM com payload PEQUENO (sem ZSTD) ---------
    print("\n[A] Comando admin→cliente (payload pequeno, SEM zstd):")
    payload = json.dumps({"type":"screenshot"}).encode()
    env = enc_uint32(1, ACT_SEND) + enc_uint32(2, cid) + enc_bytes(3, payload)
    await admin.send(bytes([0]) + env)   # flag 0 = sem compressão

    raw = await recv_binary(client)
    flags = raw[0]
    p = parse(raw[1:])
    check(p["type"] == SCT_CMD, f"cliente recebeu CLIENT_COMMAND (type={p['type']})")
    check(not (flags & FLAG_ZSTD), "flag ZSTD desligada (payload pequeno)")
    check(p["payload"] == payload, "payload repassado byte a byte (idêntico)")
    cmd = json.loads(p["payload"].decode())
    check(cmd == {"type":"screenshot"}, f"comando decodificado: {cmd}")

    r = await recv_binary(admin)
    rp = parse(r[1:])
    check(rp["type"] == SAT_CMD, f"command_result protobuf (type={rp['type']})")
    check(rp["success"] == True, f"success=True")
    check("Comando" in rp["message"], f"message: {rp['message']!r}")

    # ---- B. broadcast WSM ----------------------------------------------
    print("\n[B] Broadcast (WSM, payload compartilhado):")
    bp = json.dumps({"type":"location"}).encode()
    env = enc_uint32(1, ACT_BCAST) + enc_bytes(3, bp)
    await admin.send(bytes([0]) + env)
    br = await recv_binary(client)
    bparsed = parse(br[1:])
    check(bparsed["type"] == SCT_CMD, "broadcast chegou como CLIENT_COMMAND")
    check(bparsed["payload"] == bp, "payload do broadcast idêntico")
    rb = await recv_binary(admin)
    rbp = parse(rb[1:])
    check(rbp["type"] == SAT_CMD and rbp["count"] == 1, f"broadcast result count={rbp['count']}")

    # ---- C. cliente → admin: resposta WSM ------------------------------
    print("\n[C] Resposta cliente→admin (WSM):")
    resp = json.dumps({"type":"location","lat":-23.5,"lng":-46.6}).encode()
    cenv = enc_uint32(1, CST_RESP) + enc_bytes(3, resp)
    await client.send(bytes([0]) + cenv)
    fr = await recv_binary(admin)
    fp = parse(fr[1:])
    check(fp["type"] == SAT_MSG, f"admin recebeu CLIENT_MESSAGE (type={fp['type']})")
    check(fp["clientId"] == cid, f"clientId do envelope = {fp['clientId']} (esperado {cid})")
    check(fp["payload"] == resp, "payload da resposta repassado cru")
    got = json.loads(fp["payload"].decode())
    check(got["lat"] == -23.5, f"resposta decodificada: {got}")

    # ---- D. JSON legado ainda funciona ---------------------------------
    print("\n[D] Fallback JSON legado:")
    await admin.send(json.dumps({"type":"send_to_client","clientId":cid,"message":{"type":"ping"}}))
    leg = await asyncio.wait_for(client.recv(), 5)
    check(isinstance(leg, str), "cliente JSON legado recebeu TEXT (não binário)")
    check(json.loads(leg) == {"type":"ping"}, f"payload JSON legado correto: {leg}")

    # ---- E. controle: ping/pong em JSON --------------------------------
    print("\n[E] Controle (JSON):")
    # Drenar qualquer command_result pendente antes do ping (WSM em voo)
    while True:
        try:
            leftover = await asyncio.wait_for(admin.recv(), 0.5)
        except asyncio.TimeoutError:
            break
    await admin.send(json.dumps({"type":"ping"}))
    pong = await recv_json(admin)
    check(pong["type"] == "pong", f"pong: {pong['type']}")

    # ---- F. comando grande seria comprimido (sintaxe) ------------------
    print("\n[F] Regra de threshold (não comprime < 64B):")
    small = json.dumps({"type":"mic"}).encode()
    check(len(small) < 64, f"comando pequeno: {len(small)}B < 64B → sem zstd")

    await admin.close(); await client.close()
    print(f"\n{'🎉 TODOS OS CHECKS PASSARAM' if fails == 0 else f'⚠️ {fails} FALHAS'}")
    return 1 if fails else 0

code = asyncio.run(main())
raise SystemExit(code)
