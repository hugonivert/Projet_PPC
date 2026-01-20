# drought.py
# Drought logic (used by env) + optional CLI signal sender.
#
# Usage:
#   python3 drought.py <env_pid>
#   python3 drought.py --pidfile env.pid

import argparse
import os
import random
import signal
import sys
import threading


# Drought parameters
DROUGHT_MIN_TICKS = 10
DROUGHT_MAX_TICKS = 20
TICK_SECONDS = 1.0
DROUGHT_RANDOM_MAX_DELAY_TICKS = 100  # random timer between 1..100 ticks

drought_until_tick = None
toggle_drought_requested = False
random_drought_requested = False
drought_timer = None


def cancel_drought_timer() -> None:
    global drought_timer
    if drought_timer is not None:
        drought_timer.cancel()
        drought_timer = None


def schedule_drought_timer() -> None:
    global drought_timer
    delay_ticks = random.randint(1, DROUGHT_RANDOM_MAX_DELAY_TICKS)
    delay = delay_ticks * TICK_SECONDS
    drought_timer = threading.Timer(delay, request_random_drought)
    drought_timer.daemon = True
    drought_timer.start()


def request_random_drought() -> None:
    global random_drought_requested
    random_drought_requested = True


def set_drought(
    shared_mem,
    mutex,
    mq,
    st: list[int],
    on: bool,
    reason: str,
    *,
    apply_state,
    send_evt,
    ev_drought: str,
    idx_pred: int,
    idx_prey: int,
    idx_grass: int,
    idx_drought: int,
    idx_tick: int,
) -> None:
    global drought_until_tick

    if on:
        if st[idx_drought] == 1:
            return
        cancel_drought_timer()
        duration = random.randint(DROUGHT_MIN_TICKS, DROUGHT_MAX_TICKS)
        st[idx_drought] = 1
        drought_until_tick = st[idx_tick] + duration
        apply_state(shared_mem, mutex, st[idx_pred], st[idx_prey], st[idx_grass], st[idx_drought], st[idx_tick])
        send_evt(mq, ev_drought)
        print(f"[env] drought ON ({reason}, {duration} ticks)")
    else:
        if st[idx_drought] == 0:
            return
        st[idx_drought] = 0
        drought_until_tick = None
        apply_state(shared_mem, mutex, st[idx_pred], st[idx_prey], st[idx_grass], st[idx_drought], st[idx_tick])
        send_evt(mq, ev_drought)
        print(f"[env] drought OFF ({reason})")
        schedule_drought_timer()


def on_drought_signal(signum, frame):
    global toggle_drought_requested
    toggle_drought_requested = True


def read_pidfile(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        return int(content)
    except Exception as exc:
        raise RuntimeError(f"Cannot read pidfile '{path}': {exc}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Toggle drought in env via SIGUSR1.")
    parser.add_argument("pid", nargs="?", type=int, help="PID of env process")
    parser.add_argument("--pidfile", default=None, help="Path to pidfile (default: none)")
    args = parser.parse_args()

    if args.pidfile:
        pid = read_pidfile(args.pidfile)
    elif args.pid is not None:
        pid = args.pid
    else:
        print("error: provide <env_pid> or --pidfile", file=sys.stderr)
        return 2

    try:
        os.kill(pid, signal.SIGUSR1)
    except Exception as exc:
        print(f"error: failed to signal pid {pid}: {exc}", file=sys.stderr)
        return 1

    print(f"[drought] sent SIGUSR1 to pid={pid}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
