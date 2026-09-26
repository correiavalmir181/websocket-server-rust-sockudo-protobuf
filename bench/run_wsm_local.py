#!/usr/bin/env python3
"""Versão do run_wsm.py SEM sudo — mesma medição do run_rust.py (server
subido direto, sem cgroup) mas falando WSM no hot path. Serve pra comparar
os dois protocolos na mesma máquina sem precisar de root.

    python3 bench/run_wsm_local.py target/release/websocket-server 60 4 12 [msg_size]
"""
import subprocess, sys, os, time, threading, queue as Queue

def read_cpu_ticks(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            parts = f.read().split()
        return int(parts[13]) + int(parts[14])
    except FileNotFoundError:
        return None

def find_server_pid(server_bin, exclude):
    name = os.path.basename(server_bin)
    try:
        out = subprocess.run(["pgrep", "-f", name], capture_output=True, text=True, timeout=5)
    except Exception:
        return None
    for line in out.stdout.split():
        pid = int(line)
        if pid not in exclude:
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

    logf = open("bench/server_wsm.log", "w")
    proc = subprocess.Popen([server_bin], stdout=logf, stderr=logf,
                            stdin=subprocess.DEVNULL)
    time.sleep(1.5)
    pid = proc.pid
    if read_cpu_ticks(pid) is None:
        pid = find_server_pid(server_bin, {os.getpid(), proc.pid}) or proc.pid
    print(f"[wsm-local] server pid={pid} load: {admins} admins x {cpd} clientes (msg {msgsz}B)")

    stop = threading.Event()
    q = Queue.Queue()
    def sampler():
        while not stop.is_set():
            t = read_cpu_ticks(pid)
            if t is not None: q.put(t)
            time.sleep(0.1)
    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    cpu0 = read_cpu_ticks(pid)
    t0 = time.perf_counter()
    r = subprocess.run(
        ["bench/load-client/target/release/load-client",
         "--url", "ws://127.0.0.1:8080", "--wsm",
         "--admins", admins, "--clients-per-admin", cpd, "--duration", dur,
         "--msg-size", msgsz],
        capture_output=True, text=True, timeout=int(dur) + 60,
    )
    cpu1 = read_cpu_ticks(pid)
    t1 = time.perf_counter()
    stop.set(); th.join(2)

    proc.terminate()
    try: proc.wait(timeout=3)
    except Exception: proc.kill()

    msgs, el = 0, 0.0
    for line in (r.stdout.splitlines() + r.stderr.splitlines()):
        if "msgs=" in line and "elapsed" in line:
            msgs = int(line.split("msgs=")[1].split()[0])
            el = float(line.split("elapsed=")[1].split("s")[0])

    cpu_s = (cpu1 - cpu0) / os.sysconf("SC_CLK_TCK")
    print(f"\n=== RESULTADO WSM (protobuf+ZSTD, sem cgroup) ===")
    if el and msgs:
        print(f"load-client:                 msgs={msgs} elapsed={el:.2f}s -> {msgs/el:,.0f} msgs/s")
    else:
        print("sem dados (cliente não completou nenhuma mensagem)")
    print(f"CPU do server:               {cpu_s:.2f}s")
    if msgs and cpu_s > 0:
        print(f"CPU por mensagem:            {cpu_s*1e6/msgs:.2f} us/msg   <== CHAVE")

if __name__ == "__main__":
    main()
