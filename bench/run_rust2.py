#!/usr/bin/env python3
"""
Wrapper: roda o server (limitado por systemd-run CPUQuota=10%, MemoryMax=512M)
+ o load-client (Rust, saturador de verdade) e mede CPU do server por mensagem.

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


def find_server_pid(server_bin, exclude_pids):
    """Localiza o PID real do server (systemd-run + sudo criam wrappers)."""
    name = os.path.basename(server_bin)
    try:
        out = subprocess.run(
            ["pgrep", "-f", name],
            capture_output=True, text=True, timeout=5,
        ).stdout
        for line in out.split():
            p = int(line)
            if p in exclude_pids:
                continue
            # ignora o próprio sudo/systemd-run
            try:
                with open(f"/proc/{p}/comm", "r") as f:
                    comm = f.read().strip()
            except Exception:
                continue
            if comm == "sudo" or comm.startswith("systemd-run"):
                continue
            return p
    except Exception:
        pass
    return None


def sampler(pid_getter, q, stop):
    clk = os.sysconf("SC_CLK_TCK")
    samples = []
    while not stop.is_set():
        pid = pid_getter()
        v = read_cpu_ticks(pid) if pid else None
        if v is not None:
            samples.append((time.perf_counter(), v))
        time.sleep(0.05)
    q.put((samples, clk))


def main():
    server_bin = sys.argv[1]
    admins = sys.argv[2]
    cpd = sys.argv[3]
    duration = sys.argv[4]

    logf = open("bench/server.log", "w")

    # --- Sobe o server com limite de CPU/memória via systemd-run ---
    cmd = [
        "sudo", "systemd-run", "--scope",
        "-p", "CPUQuota=10%",
        "-p", "MemoryMax=512M",
        "--unit", f"bench-ws-{os.getpid()}",
        "--collect",
        server_bin,
    ]
    print(f"[run] cmd: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=logf, stderr=logf)
    print(f"[run] wrapper pid={proc.pid} load: {admins} admins x {cpd} clientes")

    # --- Espera o server real aparecer ---
    server_pid = None
    t_wait = time.time() + 15
    while time.time() < t_wait:
        server_pid = find_server_pid(server_bin, exclude_pids={os.getpid(), proc.pid})
        if server_pid is not None and read_cpu_ticks(server_pid) is not None:
            break
        time.sleep(0.2)

    if server_pid is None:
        print("[erro] não achei o PID do server. Abortando.", file=sys.stderr)
        proc.terminate()
        try: proc.wait(timeout=3)
        except Exception: proc.kill()
        sys.exit(1)

    print(f"[run] server pid={server_pid} (wrapper={proc.pid})")

    # --- Sampler de CPU ---
    current_pid = {"v": server_pid}
    q = Queue.Queue()
    stop = threading.Event()

    def pid_getter():
        # revalida (por se systemd reexecutar)
        p = current_pid["v"]
        if read_cpu_ticks(p) is not None:
            return p
        p2 = find_server_pid(server_bin, exclude_pids={os.getpid(), proc.pid})
        if p2:
            current_pid["v"] = p2
        return p2

    th = threading.Thread(target=sampler, args=(pid_getter, q, stop), daemon=True)
    th.start()

    t0 = time.perf_counter()
    cpu0 = read_cpu_ticks(current_pid["v"])
    r = subprocess.run(
        ["bench/load-client/target/release/load-client", admins, cpd, duration],
        capture_output=True, text=True, timeout=int(duration) + 60,
    )
    cpu1 = read_cpu_ticks(current_pid["v"])
    t1 = time.perf_counter()
    stop.set()
    th.join(2)
    try:
        samples, clk = q.get(timeout=2)
    except Queue.Empty:
        samples, clk = [], os.sysconf("SC_CLK_TCK")

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except Exception:
        proc.kill()

    out = r.stdout.strip()

    # msgs=123 elapsed=12.00s -> 1000 msgs/s
    msgs = 0
    mps = 0.0
    for tok in out.split():
        if tok.startswith("msgs="):
            msgs = int(tok[5:].split(",")[0])
        if tok.startswith("->"):
            try:
                mps = float(tok[2:].split()[0].replace(",", ""))
            except Exception:
                pass

    wall = t1 - t0
    cpu_secs = ((cpu1 - cpu0) / clk) if (cpu0 is not None and cpu1 is not None) else 0.0
    cpu_per_msg = (cpu_secs / msgs * 1e6) if msgs else float("nan")

    print(f"\n=== RESULTADO (load-client Rust, CPUQuota=10%, MemMax=512M) ===")
    print(f"load-client:                 {out}")
    print(f"janela (client):             {wall:.2f}s")
    print(f"CPU do server:               {cpu_secs:.2f}s ({cpu_secs/wall*100:.0f}% de 1 core)")
    print(f"CPU por mensagem:            {cpu_per_msg:.2f} us/msg   <== CHAVE")
    if msgs and cpu_per_msg == cpu_per_msg and cpu_per_msg > 0:
        print(f"=> throughput previsto @0.1 CPU: {0.1/(cpu_per_msg*1e-6):,.0f} msgs/s")


if __name__ == "__main__":
    main()
