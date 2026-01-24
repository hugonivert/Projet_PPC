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
            line, _, rest = buffer.partition(b"\n") #découpe en : avant /n après
            buffer[:] = rest
            return line.decode(errors="replace").strip() #nettoyage 

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
    Attache la SHM + sémaphore de l'env. Retry pour laisser le temps à env.
    """
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            # 1) Attacher le semaphore du monde pour la synchro.
            mutex = sysv_ipc.Semaphore(SEMA_KEY)
            # 2) Attacher la memoire partagee deja creee par env.
            shared_mem = shared_memory.SharedMemory(name=SHM_NAME, create=False)
            try:
                # 3) Eviter que multiprocessing "resource_tracker" supprime la SHM
                # a la fin du process client. On desenregistre localement.
                name_for_tracker = shared_mem._name
                tracker = getattr(resource_tracker, "_resource_tracker", None)
                cache = getattr(tracker, "_cache", None)
                if isinstance(cache, dict):
                    shm_cache = cache.get("shared_memory")
                    if shm_cache is not None and name_for_tracker not in shm_cache and shared_mem.name in shm_cache:
                        name_for_tracker = shared_mem.name
                resource_tracker.unregister(name_for_tracker, "shared_memory")
            except Exception:
                # Si le tracker n'existe pas ou change, on ignore et on continue.
                pass
            return shared_mem, mutex
        except Exception as e:
            # Retry: env peut ne pas avoir encore cree la SHM ou le semaphore.
            last_err = e
            time.sleep(delay)
    raise RuntimeError(f"Unable to attach world SHM: {last_err}")


def read_state(shared_mem, mutex) -> list[int]:
    """
    Lit l'état global (5 entiers) depuis la mémoire partagée (SHM), de façon atomique.
    """
    mutex.acquire()
    try:
        return list(FMT.unpack(shared_mem.buf[:FMT.size]))
    finally:
        mutex.release()
