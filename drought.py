# drought.py
# External drought signaler for env (SIGUSR1).
#
# Usage:
#   python drought.py <env_pid>
#   python drought.py --pidfile env.pid
#   python drought.py --random <env_pid>
#   python drought.py --random --pidfile env.pid

import argparse
import os
import random
import signal
import sys
import time


def read_pidfile(path: str) -> int:
    """
    Lit un PID depuis un fichier texte et le renvoie en int.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        return int(content)
    except Exception as exc:
        raise RuntimeError(f"Cannot read pidfile '{path}': {exc}") from exc


def send_signal(pid: int) -> None:
    """
    Envoie SIGUSR1 au processus cible.
    """
    os.kill(pid, signal.SIGUSR1)


def random_signal_loop(pid: int, min_delay: int, max_delay: int) -> None:
    """
    Envoie SIGUSR1 en boucle avec un delai aleatoire entre min_delay et max_delay.
    """
    if min_delay <= 0 or max_delay < min_delay:
        raise ValueError("Invalid delay range")

    while True:
        delay = random.randint(min_delay, max_delay)
        time.sleep(delay)
        send_signal(pid)
        print(f"[drought] sent SIGUSR1 to pid={pid} (delay={delay}s)")


def main() -> int:
    """
    Entree CLI pour envoyer SIGUSR1 une fois ou en boucle aleatoire.
    """
    parser = argparse.ArgumentParser(description="Signal env to start drought via SIGUSR1.")
    parser.add_argument("pid", nargs="?", type=int, help="PID of env process")
    parser.add_argument("--pidfile", default=None, help="Path to pidfile (default: none)")
    parser.add_argument("--random", action="store_true", help="Send random signals in a loop")
    parser.add_argument("--min-delay", type=int, default=10, help="Min delay in seconds")
    parser.add_argument("--max-delay", type=int, default=100, help="Max delay in seconds")
    args = parser.parse_args()

    if args.pidfile:
        pid = read_pidfile(args.pidfile)
    elif args.pid is not None:
        pid = args.pid
    else:
        print("error: provide <env_pid> or --pidfile", file=sys.stderr)
        return 2

    try:
        if args.random:
            random_signal_loop(pid, args.min_delay, args.max_delay)
        else:
            send_signal(pid)
            print(f"[drought] sent SIGUSR1 to pid={pid}")
    except Exception as exc:
        print(f"error: failed to signal pid {pid}: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
