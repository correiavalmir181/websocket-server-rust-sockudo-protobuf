#!/usr/bin/env python3
"""Versão do run_rust.py que testa o protocolo WSM (protobuf + ZSTD).

Mesma medição (CPU do server por mensagem via /proc), mas o load-client fala
o protocolo binário no hot path em vez de JSON. Uso:

    sudo python3 bench/run_wsm.py target/release/websocket-server 60 4 12

⚡ CORRIGIDO: antes este script subia o servidor com subprocess.Popen puro,
SEM systemd-run/CPUQuota/MemoryMax — ou seja, testava com CPU livre (a
janela de CPU medida batia ~100% de 1 core inteiro). Isso invalidava
qualquer comparação de msgs/s contra o run_rust2.py / load-client (que
sempre aplicaram CPUQuota=10%/MemoryMax=512M de verdade via systemd-run).
Precisa de sudo agora por causa do systemd-run.
"""
import subprocess, sys, os, time, threading, queue as Queue

CPU_QUOTA = os.environ.get("WSM_CPU_QUOTA", "25%")
MEM_MAX = os.environ.get("WSM_MEM_MAX", "128M")

def read_cpu_ticks(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().split()
    return int(parts[13]) + int(parts[14])

def find_server_pid(server_bin, exclude):
    name = os.path.basename(server_bin)
    out = subprocess.run(["pgrep", "-f", name], capture_output=True, text=True).stdout
    for line in out.split():
        pid = int(line)
        if pid in exclude:
            continue
        try:
            with open(f"/proc/{pid}/comm") as f:
                comm = f.read().strip()
        except FileNotFoundError:
            continue
        if comm in ("sudo", "systemd-run"):
            continue
        return pid
    return None

def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(1)
    server_bin = sys.argv[1]
    admins = sys.argv[2] if len(sys.argv) > 2 else "60"
    cpd    = sys.argv[3] if len(sys.argv) > 3 else "4"
    dur    = sys.argv[4] if len(sys.argv) > 4 else "12"
    msgsz  = sys.argv[5] if len(sys.argv) > 5 else "0"

    unit_name = f"bench-wsm-{os.getpid()}"
    # ⚡ Mesma restrição que o run_rust2.py/load-client sempre aplicaram —
    # sem isso, a comparação de msgs/s entre protocolos não vale nada.
    # stdin fechado: sudo aninhado pedindo senha não pode pendurar o teste.
    wrapper = subprocess.Popen(
        ["sudo", "systemd-run", "--scope",
         "-p", f"CPUQuota={CPU_QUOTA}",
         "-p", f"MemoryMax={MEM_MAX}",
         "--unit", unit_name, "--collect",
         server_bin],
        stdin=subprocess.DEVNULL,
    )

    pid = None
    deadline = time.time() + 15
    while time.time() < deadline:
        pid = find_server_pid(server_bin, exclude={os.getpid(), wrapper.pid})
        if pid is not None:
            try:
                read_cpu_ticks(pid)
                break
            except FileNotFoundError:
                pid = None
        time.sleep(0.2)
    if pid is None:
        wrapper.terminate()
        print(f"❌ não achei o PID do servidor em 15s — veja `journalctl -u {unit_name}`")
        sys.exit(1)

    print(f"[wsm] server pid={pid} load: {admins} admins x {cpd} clientes (WSM, CPUQuota={CPU_QUOTA} MemoryMax={MEM_MAX})")

    stop = threading.Event()
    q = Queue.Queue()
    def sampler():
        samples = []
        clk = 0
        while not stop.is_set():
            try:
                samples.append(read_cpu_ticks(pid))
            except FileNotFoundError:
                break
            clk += 1
            time.sleep(0.1)
        q.put((samples, clk))
    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    t0 = time.perf_counter()
    cpu0 = read_cpu_ticks(pid)
    msgs = 0
    el = float(dur)
    r = subprocess.run(
        ["bench/load-client/target/release/load-client",
         "--url", "ws://127.0.0.1:8080", "--wsm",
         "--admins", admins, "--clients-per-admin", cpd, "--duration", dur,
         "--msg-size", msgsz],
        capture_output=True, text=True, timeout=int(dur) + 60,
    )
    # Servidor pode ter morrido no meio (ex: OOM) — checar antes de ler CPU final.
    server_died = find_server_pid(server_bin, exclude={os.getpid(), wrapper.pid}) != pid
    cpu1 = read_cpu_ticks(pid) if not server_died else cpu0
    t1 = time.perf_counter()
    stop.set(); th.join(2)

    # ⚠️ stdin DEVNULL + timeout: sudo aninhado pode pedir senha (cache
    # expirado) e pendurar o teste pra sempre. Fechando o stdin ele falha
    # imediato em vez de esperar; o timeout garante que nunca ficamos presos.
    try:
        subprocess.run(["sudo", "systemctl", "stop", f"{unit_name}.scope"],
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        timeout=10)
    except subprocess.TimeoutExpired:
        print(f"⚠️  systemctl stop demorou >10s; forçando kill do wrapper")
    wrapper.terminate()
    try: wrapper.wait(timeout=3)
    except Exception: wrapper.kill()

    # parse "msgs=NNN elapsed=SS -> X msgs/s" e também as linhas de falha,
    # que o load-client já imprime (--wsm reaproveita os mesmos contadores
    # connect_failures/send_failures do modo JSON) mas este script não
    # repassava antes.
    falhas_linhas = []
    incompletos = False
    for line in (r.stdout.splitlines() + r.stderr.splitlines()):
        if "msgs=" in line and "elapsed" in line:
            msgs = int(line.split("msgs=")[1].split()[0])
            el = float(line.split("elapsed=")[1].split("s")[0])
        if "enviados ao socket:" in line:
            falhas_linhas.append(line.strip())
            try:
                incompletos = int(line.rsplit("sem confirmação: ", 1)[1]) > 0 or int(line.split("recusados pelo servidor: ", 1)[1].split(";", 1)[0]) > 0
            except (IndexError, ValueError):
                incompletos = True
        elif "falhas de conexão" in line or "quedas durante envio" in line or "erros de escrita/reconexão" in line:
            falhas_linhas.append(line.strip())
            incompletos = True

    cpu_s = (cpu1 - cpu0) / os.sysconf("SC_CLK_TCK")
    print(f"\n=== RESULTADO WSM (protobuf + ZSTD) ===")
    print(f"load-client:                 msgs={msgs} elapsed={el:.2f}s -> {msgs/el:,.0f} msgs/s" if el else "sem dados")
    for linha in falhas_linhas:
        print(linha)
    if incompletos:
        print("   Houve envios sem confirmação, recusas ou desconexões; msgs/s não demonstra entrega no cliente.")
    if server_died:
        print("🔴 O servidor morreu/mudou de identidade durante o teste — CPU abaixo NÃO é confiável.")
        r2 = subprocess.run(["sudo", "systemctl", "show", f"{unit_name}.scope", "-p", "Result", "--value"],
                             capture_output=True, text=True)
        result = r2.stdout.strip()
        if result:
            print(f"   systemd Result da scope: {result}")
            if "oom" in result.lower():
                print(f"   ⚠️  OOM-KILL confirmado: excedeu MemoryMax={MEM_MAX}.")
    else:
        print(f"CPU do server:               {cpu_s:.2f}s")
        if msgs and cpu_s > 0:
            cpu_per_msg_us = cpu_s * 1e6 / msgs
            print(f"CPU por mensagem:            {cpu_per_msg_us:.2f} us/msg   <== CHAVE")
            # ⚡ Mesma previsão que o load-client (versão sem protobuf) já
            # mostra — faltava aqui, adicionada a pedido.
            predicted = 0.1 / (cpu_per_msg_us * 1e-6)
            print(f"=> throughput previsto @0.1 CPU: {predicted:,.0f} msgs/s")

if __name__ == "__main__":
    main()

