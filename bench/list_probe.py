import asyncio, json, time, websockets

async def main():
    a = await websockets.connect('ws://127.0.0.1:8080', open_timeout=5)
    await a.send(json.dumps({'type':'admin_auth','adminId':'lst','password':'admin123'}))
    await a.recv()

    # Cria N clientes
    N = 500
    clients = []
    for i in range(N):
        c = await websockets.connect('ws://127.0.0.1:8080', open_timeout=5)
        await c.send(json.dumps({'type':'identification','data':'android','adminId':'lst','androidId':f'd{i}','wallpaper':'w'}))
        await c.recv()
        clients.append(c)
    await asyncio.sleep(0.5)
    # drena notificações client_connected do admin
    try:
        while True: await asyncio.wait_for(a.recv(), 0.3)
    except asyncio.TimeoutError: pass

    # Mede list_clients 3x
    for trial in range(3):
        t0 = time.perf_counter()
        await a.send(json.dumps({'type':'list_clients'}))
        resp = await asyncio.wait_for(a.recv(), 30)
        dt = (time.perf_counter() - t0) * 1000
        n = len(json.loads(resp)['clients'])
        print(f'trial {trial+1}: {n} clientes em {dt:.1f} ms')

    for c in clients: await c.close()
    await a.close()

asyncio.run(main())
