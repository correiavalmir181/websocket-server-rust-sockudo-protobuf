import asyncio, json, websockets

async def main():
    # Admin le DEVAGAR (1 msg a cada 80ms = 12.5/s) — canal de 256 vai encher
    a = await websockets.connect('ws://127.0.0.1:8080', open_timeout=5, max_size=None)
    await a.send(json.dumps({'type':'admin_auth','adminId':'slow','password':'admin123'}))
    await a.recv()

    c = await websockets.connect('ws://127.0.0.1:8080', open_timeout=5, max_size=None)
    await c.send(json.dumps({'type':'identification','data':'android','adminId':'slow','androidId':'d','wallpaper':'x'}))
    await c.recv()
    await asyncio.sleep(0.3)
    await a.recv()  # client_connected

    # Cliente inunda 400 mensagens (> 256 = capacidade do canal do admin)
    N = 400
    for i in range(N):
        await c.send(json.dumps({'type':'r','d':f'm{i}'}))

    # Admin lê DEVAGAR enquanto o cliente termina de enviar
    received = []
    for i in range(N):
        try:
            m = await asyncio.wait_for(a.recv(), 45)
            if '"client_message"' in m:
                received.append(m)
        except asyncio.TimeoutError:
            break
        await asyncio.sleep(0.08)  # leitura lenta proposital

    # Drena o resto
    try:
        while True:
            m = await asyncio.wait_for(a.recv(), 0.5)
            if '"client_message"' in m:
                received.append(m)
    except asyncio.TimeoutError:
        pass

    print(f'enviadas={N}  entregues={len(received)}  PERDIDAS={N-len(received)}')
    if len(received) == N:
        print('RESULTADO: zero perdas — backpressure funcionou (cliente freou em vez de descartar)')
    else:
        print('RESULTADO: houve perda silenciosa')

asyncio.run(main())
