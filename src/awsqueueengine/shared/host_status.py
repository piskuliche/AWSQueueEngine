# Host/process status helpers
from concurrent.futures import ThreadPoolExecutor, as_completed
from .ssh_utils import ssh_run
from pathlib import Path

def check_host_for_tag(host):
    cmd = r"ps -eo pid,cmd | grep -F 'MANAGER_TAG=' | grep -v grep || true"
    rc, out, err = ssh_run(host, cmd)
    if rc != 0 and out == "" and err:
        return {"host": host, "reachable": False, "pid": None, "tag": None, "raw": err}
    if out:
        lines = [l for l in out.splitlines() if l.strip()]
        parts = lines[0].split(None, 1)
        pid = parts[0]
        cmdline = parts[1] if len(parts) > 1 else ""
        tag = None
        for token in cmdline.split():
            if token.startswith("MANAGER_TAG="):
                tag = token.split("=", 1)[1]
                break
        return {"host": host, "reachable": True, "pid": pid, "tag": tag, "raw": cmdline}
    rc2, out2, err2 = ssh_run(host, "ls -t ~/manager_jobs/*.pid 2>/dev/null | head -n1 || true")
    if rc2 == 0 and out2:
        pidfile = out2.splitlines()[0].strip()
        rc3, out3, err3 = ssh_run(host, f"cat {pidfile} || true")
        pidval = out3.strip() if rc3 == 0 else None
        tag = None
        try:
            tag = Path(pidfile).name.split(".")[0]
        except Exception:
            tag = None
        if pidval:
            rc4, out4, err4 = ssh_run(host, f"ps -p {pidval} -o pid= || true")
            if out4.strip():
                return {"host": host, "reachable": True, "pid": pidval, "tag": tag, "raw": f"pidfile:{pidfile}"}
        return {"host": host, "reachable": True, "pid": None, "tag": tag, "raw": f"stale-pidfile:{pidfile}"}
    return {"host": host, "reachable": True, "pid": None, "tag": None, "raw": ""}

def status_all(hosts):
    results = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(check_host_for_tag, h): h for h in hosts}
        for fut in as_completed(futures):
            results.append(fut.result())
    return sorted(results, key=lambda r: r["host"])
