"""
Mede o custo de CPU do SERVIDOR por mensagem, sem depender de cgroup.

Por que isso funciona: com CPUQuota=10% (0.1 CPU), o throughput máximo é
exatamente  throughput = 0.1 / cpu_por_msg.  Então reduzir cpu_por_msg é a
única forma de subir o número do benchmark. Medindo cpu_por_msg localmente
(sem throttling) dá pra comparar versões com precisão e rapidez.

Uso (a partir da raiz do projeto):
  python3 bench/bench_cpumsg.py <binario_do_server> [n_admins] [clientes_por_admin] [duracao_s]

Exemplo:
  python3 bench/bench_cpumsg.py target/release/websocket-server 60 4 10
"""
import asyncio, json, sys, os, time, subprocess
import socket as sock_mod
import websockets

WINDOW = 24            # mensagens em vôo por admin (saturar, não medir latência)
CONNECT_TIMEOUT = 30
BIND = "127.0.0.1"
PORT = 8080
URI = f"ws://{BIND}:{PORT}"


def read_cpu_ticks(pid):
    # /proc/<pid>/stat: campos 14 (utime) e 15 (stime), em clock ticks
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            data = f.read()
        rparen = data.rindex(b")")
        fields = data[rparen + 2:].split()
        return int(fields[11]) + int(fields[12])
    except Exception:
        return None


def clk():
    return os.sysconf("SC_CLK_TCK")


async def drain(ws):
    try:
        async for _ in ws:
            pass
    except Exception:
        pass


async def admin_reader(ws, q):
    try:
        async for raw in ws:
            if json.loads(raw).get("type") == "command_result":
                q.put_nowait(1)
    except Exception:
        pass


async def admin_loop(ws, q, clients, stop_at, st):
    """Pipeline: dispara rajadas de MAX_PENDING e drena respostas por evento."""
    MAX_PENDING = 100
    n = 0
    pending = 0
    while time.perf_counter() < stop_at:
        # dispara até encher a janela
        while pending < MAX_PENDING and time.perf_counter() < stop_at:
            cid = clients[n % len(clients)]
            n += 1
            try:
                await ws.send(json.dumps({"type": "send_to_client", "clientId": cid,
                                          "message": {"type": "cmd", "i": n}}))
            except Exception:
                return
            pending += 1
        # bloqueia até uma resposta chegar (event-driven, sem polling)
        try:
            await asyncio.wait_for(q.get(), 2.0)
            pending -= 1
            st[0] += 1
        except asyncio.TimeoutError:
            st[1] += 1
            pending = max(0, pending - 1)
        # drena o que já chegou
        while q.qsize() > 0:
            q.get_nowait()
            pending -= 1
            st[0] += 1


async def run_load(n_admins, cpd, duration, start_gate, ready_evt, pid, out):
    """Conecta tudo, libera a carga e mede a janela de CPU."""
    sessions = []
    conns = []
    for a in range(n_admins):
        ws = await asyncio.wait_for(websockets.connect(URI, open_timeout=CONNECT_TIMEOUT), CONNECT_TIMEOUT)
        await ws.send(json.dumps({"type": "admin_auth", "adminId": f"admin{a}", "password": "admin123"}))
        await ws.recv()
        q = asyncio.Queue()
        clients = []
        for c in range(cpd):
            cw = await asyncio.wait_for(websockets.connect(URI, open_timeout=CONNECT_TIMEOUT), CONNECT_TIMEOUT)
            await cw.send(json.dumps({"type": "identification", "data": "android",
                                      "adminId": f"admin{a}", "androidId": f"dev{a}-{c}", "wallpaper": "x"}))
            welcome = json.loads(await cw.recv())
            clients.append(welcome["clientId"])
            conns.append(cw)
            asyncio.create_task(drain(cw))
        asyncio.create_task(admin_reader(ws, q))
        sessions.append((ws, q, clients))
        conns.append(ws)
    ready_evt.set()
    await start_gate.wait()

    # === inÃ­cio da janela de carga ===
    t0 = time.perf_counter()
    cpu0 = read_cpu_ticks(pid)
    stop_at = t0 + duration
    tasks = []
    stats = []
    for (ws, q, clients) in sessions:
        st = [0, 0]
        stats.append(st)
        tasks.append(asyncio.create_task(admin_loop(ws, q, clients, stop_at, st)))
    await asyncio.sleep(duration)
    cpu1 = read_cpu_ticks(pid)
    t1 = time.perf_counter()
    for t in tasks:
        t.cancel()
    for c in conns:
        try:
            await c.close()
        except Exception:
            pass
    out["stats"] = stats
    out["cpu0"] = cpu0
    out["cpu1"] = cpu1
    out["t0"] = t0
    out["t1"] = t1


def main():
    server_bin = sys.argv[1] if len(sys.argv) > 1 else "target/release/websocket-server"
    n_admins = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    cpd = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    duration = float(sys.argv[4]) if len(sys.argv) > 4 else 10.0

    if os.path.exists("bench/server.log"):
        os.remove("bench/server.log")
    logf = open("bench/server.log", "w")
    proc = subprocess.Popen([server_bin], stdout=logf, stderr=logf)
    print(f"[harness] server pid={proc.pid} bin={server_bin} (admins={n_admins} cpd={cpd} dur={duration}s)")

    for _ in range(100):
        try:
            s = sock_mod.create_connection((BIND, PORT), timeout=0.3)
            s.close()
            break
        except Exception:
            time.sleep(0.1)

    ready_evt = asyncio.Event()
    start_gate = asyncio.Event()

    async def main_async():
        out = {}
        driver = asyncio.create_task(run_load(n_admins, cpd, duration, start_gate, ready_evt, proc.pid, out))
        await ready_evt.wait()
        t_setup = time.perf_counter()
        start_gate.set()
        await driver
        return out, t_setup

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    out, t_setup = loop.run_until_complete(main_async())

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except Exception:
        proc.kill()

    total_ok = sum(s[0] for s in out["stats"])
    total_to = sum(s[1] for s in out["stats"])
    wall = out["t1"] - out["t0"]
    cpu0, cpu1 = out["cpu0"], out["cpu1"]
    cpu_secs = ((cpu1 - cpu0) / clk()) if (cpu0 and cpu1) else 0.0

    mps = total_ok / wall if wall > 0 else 0
    cpu_per_msg = (cpu_secs / total_ok * 1e6) if total_ok else float("nan")
    print(f"\n=== RESULTADO ===")
    print(f"setup (conexões):            {t_setup:.1f}s")
    print(f"mensagens respondidas:       {total_ok} (timeouts: {total_to})")
    print(f"janela de carga:             {wall:.2f}s")
    print(f"throughput (1 core):         {mps:,.0f} msgs/s")
    print(f"CPU do server na janela:     {cpu_secs:.2f}s ({cpu_secs/wall*100:.0f}% de 1 core)")
    print(f"CPU por mensagem:            {cpu_per_msg:.2f} us/msg   <== CHAVE")
    if total_ok and cpu_per_msg == cpu_per_msg:
        print(f"=> throughput previsto @0.1 CPU: {0.1/(cpu_per_msg*1e-6):,.0f} msgs/s")


if __name__ == "__main__":
    main()
