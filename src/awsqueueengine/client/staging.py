"""Client-side staging: local sizing and `awsqe-client where` host probing."""
from ..shared.config import HOSTS
from ..shared.worker_staging import choose_scratch_on_host, sizeof_local_path_bytes

__all__ = ["sizeof_local_path_bytes", "where_is_next_submit"]


def where_is_next_submit():
    for host in HOSTS:
        needed_bytes = 1 * 1024 ** 3  # 1 GiB probe
        mount, result = choose_scratch_on_host(host, needed_bytes)
        if mount is not None:
            free_bytes = result
            print(f"[OK] host={host} mount={mount} free={free_bytes} bytes")
        else:
            error = result
            print(f"[FAIL] host={host} error={error}")
