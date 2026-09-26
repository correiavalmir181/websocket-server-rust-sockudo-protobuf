#!/usr/bin/env python3
"""
Wrapper: roda o server + o load-client (Rust, saturador de verdade) e mede
CPU do server por mensagem.

Uso: bench/run_rust.sh <server_bin> <admins> <clientes_por_admin> <duracao_s>

Exemplo: bench/run_rust.sh target/release/websocket-server 60 4 12
"""
import subprocess, sys, os, time, threading, queue as Queue

def read_cpu_ticks(pid):
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            data = f.read()
        rp = data.rindex(b")")
        fields = data[rp + 2:].split()
        return int(fields[11]) + int(fields[12])
    except Exception:
        return None

def sampler(pid, q, stop):
    clk = os.sysconf("SC_CLK_TCK")
    samples = []
    while not stop.is_set():
        v = read_cpu_ticks(pid)
        if v is not None:
            samples.append((time.perf_counter(), v))
        time.sleep(0.05)
    q.put((samples, clk))

def main():
    server_bin = sys.argv[1]
    admins = sys.argv[2]
    cpd = sys.argv[3]
    duration = sys.argv[4]
    msgsz = sys.argv[5] if len(sys.argv) > 5 else "0"

    logf = open("bench/server.log", "w")
    proc = subprocess.Popen([server_bin], stdout=logf, stderr=logf)
    print(f"[run] server pid={proc.pid} load: {admins} admins x {cpd} clientes")
    time.sleep(1.5)
    # No sandbox o PID pode ser remapeado; lê o PID real via /proc/<pid>/task
    pid = proc.pid
    if read_cpu_ticks(pid) is None:
        # procura o processo pelo nome do binário
        try:
            out = subprocess.run(["pgrep", "-f", os.path.basename(server_bin)],
                                 capture_output=True, text=True, timeout=5).stdout
            for line in out.split():
                p = int(line)
                if p != os.getpid():
                    pid = p
                    break
        except Exception:
            pass
    print(f"[run] pid usado pra CPU: {pid}")

    q = Queue.Queue()
    stop = threading.Event()
    th = threading.Thread(target=sampler, args=(proc.pid, q, stop), daemon=True)
    th.start()

    t0 = time.perf_counter()
    cpu0 = read_cpu_ticks(pid)
    r = subprocess.run(
        ["bench/load-client/target/release/load-client",
         "--url", "ws://127.0.0.1:8080",
         "--admins", admins, "--clients-per-admin", cpd, "--duration", duration,
         "--msg-size", msgsz],
        capture_output=True, text=True, timeout=int(duration) + 60,
    )
    cpu1 = read_cpu_ticks(pid)
    t1 = time.perf_counter()
    stop.set(); th.join(2)
    samples, clk = q.get()

    proc.terminate()
    try: proc.wait(timeout=3)
    except Exception: proc.kill()

    out = r.stdout.strip()
    # msgs=123 elapsed=12.00s -> 1000 msgs/s
    msgs = 0; mps = 0.0
    for tok in out.split():
        if tok.startswith("msgs="):
            msgs = int(tok[5:].split(",")[0])
        if tok.startswith("->"):
            try: mps = float(tok[2:].split()[0].replace(",", ""))
            except Exception: pass

    wall = t1 - t0
    cpu_secs = ((cpu1 - cpu0) / clk) if (cpu0 is not None and cpu1 is not None) else 0.0
    cpu_per_msg = (cpu_secs / msgs * 1e6) if msgs else float("nan")

    print(f"\n=== RESULTADO (load-client Rust) ===")
    print(f"load-client:                 {out}")
    print(f"janela (client):             {wall:.2f}s")
    print(f"CPU do server:               {cpu_secs:.2f}s ({cpu_secs/wall*100:.0f}% de 1 core)")
    print(f"CPU por mensagem:            {cpu_per_msg:.2f} us/msg   <== CHAVE")
    if msgs and cpu_per_msg == cpu_per_msg and cpu_per_msg > 0:
        print(f"=> throughput previsto @0.1 CPU: {0.1/(cpu_per_msg*1e-6):,.0f} msgs/s")

if __name__ == "__main__":
    main()
