# utils.py
import socket
import sysv_ipc
import queue
import time
from multiprocessing import shared_memory, resource_tracker
import struct

# Shared world definitions (must match env.py)
SHM_NAME = "WORLD"
SEMA_KEY = 0x1234
FMT = struct.Struct("iiiii")  # pred, prey, grass, drought, tick

# indices
PRED, PREY, GRASS, DROUGHT, TICK = 0, 1, 2, 3, 4


def recv_line(sock: socket.socket, buffer: bytearray) -> str | None:
    """
    Lit une ligne (terminée par '\n') depuis un socket TCP.
    Retourne la ligne sans '\n', ou None si la connexion est fermée.
    """
    while True:
        if b"\n" in buffer:
            line, _, rest = buffer.partition(b"\n")
            buffer[:] = rest
            return line.decode(errors="replace").strip()

        chunk = sock.recv(1024)
        if not chunk:
            return None
        buffer.extend(chunk)


def send_line(sock: socket.socket, line: str) -> None:
    """Envoie une commande terminée par '\\n'."""
    sock.sendall((line.strip() + "\n").encode())


def send_cmd_persistent(sock: socket.socket, buffer: bytearray, cmd: str) -> str:
    """
    Envoie une commande sur une connexion persistante et lit la réponse (1 ligne).
    """
    send_line(sock, cmd)
    resp = recv_line(sock, buffer)
    if resp is None:
        raise ConnectionError("env closed the connection unexpectedly")
    return resp


def mq_listener(mq: sysv_ipc.MessageQueue, ntype: int, inbox: queue.Queue):
    """
    Thread listener MQ:
    - bloque sur mq.receive(type=ntype)
    - décode le message
    - push dans inbox (Queue thread-safe) pour que le main thread traite
    """
    while True:
        try:
            msg, mtype = mq.receive(type=ntype)  # IMPORTANT: retourne (bytes, type)
            text = msg.decode("utf-8", errors="replace").strip()
            inbox.put(text)
            print("[env] MQ received:", text)
        except sysv_ipc.ExistentialError:
            print("[env] MQ removed, listener stops")
            break
        except Exception as e:
            print("[env] MQ listener error:", e)
            time.sleep(0.1)


def attach_world(retries: int = 50, delay: float = 0.1):
    """
    Attache la SHM + sAcmaphore de l'env (WORLD). Retry pour laisser le temps Aÿ env.
    """
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            mutex = sysv_ipc.Semaphore(SEMA_KEY)
            shared_mem = shared_memory.SharedMemory(name=SHM_NAME, create=False)
            try:
                resource_tracker.unregister(shared_mem._name, "shared_memory")
            except Exception:
                pass
            return shared_mem, mutex
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise RuntimeError(f"Unable to attach world SHM: {last_err}")


def read_state(shared_mem, mutex) -> list[int]:
    """
    Lit l'Actat global (5 entiers) depuis la mAcmoire partagAce (SHM), de faAon atomique.
    """
    mutex.acquire()
    try:
        return list(FMT.unpack(shared_mem.buf[:FMT.size]))
    finally:
        mutex.release()
