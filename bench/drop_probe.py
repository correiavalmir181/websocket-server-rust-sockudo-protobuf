import asyncio, json, websockets

async def main():
    a = await websockets.connect('ws://127.0.0.1:8080', open_timeout=5, max_size=None)
    await a.send(json.dumps({'type':'admin_auth','adminId':'drop','password':'admin123'}))
    await a.recv()
    c = await websockets.connect('ws://127.0.0.1:8080', open_timeout=5, max_size=None)
    await c.send(json.dumps({'type':'identification','data':'android','adminId':'drop','androidId':'d','wallpaper':'x'}))
    await c.recv()
    await asyncio.sleep(0.3)
    await a.recv()

    # Payloads GRANDES (8KB): buffer TCP do kernel nao absorve, canal de 256 enche
    N = 400
    big = 'x' * 8000
    for i in range(N):
        await c.send(json.dumps({'type':'r','d':f'{i}{big}'}))
    await asyncio.sleep(2.0)

    received = []
    try:
        while True:
            m = await asyncio.wait_for(a.recv(), 0.5)
            received.append(m)
    except asyncio.TimeoutError:
        pass
    payload = [m for m in received if '"client_message"' in m]
    print(f'enviadas={N} (8KB cada)  recebidas={len(payload)}  PERDIDAS={N-len(payload)}')

asyncio.run(main())
