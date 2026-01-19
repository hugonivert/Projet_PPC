# env.py
from multiprocessing import shared_memory
import struct
import sysv_ipc
import socket
import threading
import time
import queue
import utils

# constantes
SHM_NAME = "WORLD"
SEMA_KEY = 0x1234
FMT = struct.Struct("iiiii")  # pred, prey, grass, drought, tick
MQ_KEY = 0x5678

TYPE_CMD = 1   # display -> env
TYPE_EVT = 2   # env -> display

# indices
PRED, PREY, GRASS, DROUGHT, TICK = 0, 1, 2, 3, 4


def send_evt(mq: sysv_ipc.MessageQueue, text: str) -> None:
    """
    Envoie un event simple (string) vers display.
    Ex: "ACK_INIT", "naissance proie", "secheresse", ...
    """
    mq.send(text.encode("utf-8"), type=TYPE_EVT)


def wait_for_init(mq: sysv_ipc.MessageQueue) -> tuple[int, int, int]:
    """
    Bloque tant que env n'a pas reçu INIT depuis display.
    Format attendu (simple) :
        INIT <predators> <preys> <grass>
    Ex:
        INIT 5 20 80
    """
    print("[env] Waiting for INIT from display...")

    while True:
        msg, mtype = mq.receive(type=TYPE_CMD)  # BLOQUANT
        text = msg.decode("utf-8", errors="replace").strip()
        print("[env] INIT channel got:", text)

        parts = text.split()
        if len(parts) == 4 and parts[0] == "INIT":
            try:
                predators = int(parts[1])
                preys = int(parts[2])
                grass = int(parts[3])
                return predators, preys, grass
            except ValueError:
                print("[env] Bad INIT numbers:", text)
                continue

        print("[env] Ignored (expect 'INIT p r g'):", text)


def apply_state(shared_mem, mutex, pred: int, prey: int, grass: int, drought: int, tick: int) -> None:
    mutex.acquire()
    try:
        shared_mem.buf[:FMT.size] = FMT.pack(pred, prey, grass, drought, tick)
    finally:
        mutex.release()


def read_state(shared_mem, mutex) -> list[int]:
    mutex.acquire()
    try:
        return list(FMT.unpack(shared_mem.buf[:FMT.size]))
    finally:
        mutex.release()


def handle_cmd(cmd: str, shared_mem, mutex, mq) -> bool:
    """
    Traite une commande runtime envoyée par display pendant la simulation.
    Retourne False si on veut quitter.
    Commandes proposées:
      - STATUS
      - PAUSE
      - RESUME
      - QUIT
      - SET GRASS <n>
      - SET DROUGHT <0|1>
    """
    parts = cmd.split()
    if not parts:
        return True

    if parts[0] == "STATUS":
        st = read_state(shared_mem, mutex)
        # tu peux aussi envoyer un snapshot à display
        send_evt(mq, f"STATE pred={st[PRED]} prey={st[PREY]} grass={st[GRASS]} drought={st[DROUGHT]} tick={st[TICK]}")
        return True

    if parts[0] == "QUIT":
        send_evt(mq, "ACK_QUIT")
        return False

    if parts[0] == "SET" and len(parts) == 3:
        field = parts[1]
        try:
            val = int(parts[2])
        except ValueError:
            send_evt(mq, "ERR bad SET value")
            return True

        st = read_state(shared_mem, mutex)
        if field == "GRASS":
            st[GRASS] = max(0, val)
            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
            send_evt(mq, f"ACK_SET GRASS {st[GRASS]}")
        elif field == "DROUGHT":
            st[DROUGHT] = 1 if val != 0 else 0
            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
            send_evt(mq, f"ACK_SET DROUGHT {st[DROUGHT]}")
        else:
            send_evt(mq, "ERR unknown SET field")
        return True

    if parts[0] in ("PAUSE", "RESUME"):
        # Ici tu peux mettre une variable globale/flag si tu veux réellement stopper la boucle sim.
        send_evt(mq, f"ACK_{parts[0]}")
        return True

    send_evt(mq, "ERR unknown command")
    return True


def socket_server(shared_mem, mutex, fmt, mq):
    HOST = "localhost"
    PORT = 6666

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()

        print("[env] socket server listening on", (HOST, PORT))

        while True:
            client_socket, address = server_socket.accept()
            print("[env] Connected:", address)

            t = threading.Thread(
                target=handle_client,
                args=(client_socket, address, shared_mem, mutex, fmt, mq),
                daemon=True
            )
            t.start()


def handle_client(client_socket, address, shared_mem, mutex, fmt, mq):
    print("[env] handling client:", address)

    buffer = b""
    with client_socket:
        while True:
            chunk = client_socket.recv(1024)
            if not chunk:
                print("[env] client disconnected:", address)
                break

            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                msg = line.decode(errors="replace").strip()
                if not msg:
                    continue

                parts = msg.split()

                # JOIN prey/predator
                if len(parts) == 2 and parts[0] == "JOIN":
                    kind = parts[1]

                    mutex.acquire()
                    try:
                        state = list(fmt.unpack(shared_mem.buf[:fmt.size]))
                        if kind == "prey":
                            state[PREY] += 1
                            shared_mem.buf[:fmt.size] = fmt.pack(*state)
                            send_evt(mq, "naissance proie")
                        elif kind == "predator":
                            state[PRED] += 1
                            shared_mem.buf[:fmt.size] = fmt.pack(*state)
                            send_evt(mq, "naissance predateur")
                        else:
                            client_socket.sendall(b"ERR unknown kind\n")
                            continue
                    finally:
                        mutex.release()

                    client_socket.sendall(b"OK\n")
                    print("[env] JOIN accepted:", kind, "-> state =", state)

                # DIE prey/predator
                elif len(parts) >= 2 and parts[0] == "DIE":
                    kind = parts[1]

                    mutex.acquire()
                    try:
                        state = list(fmt.unpack(shared_mem.buf[:fmt.size]))
                        if kind == "prey":
                            state[PREY] = max(0, state[PREY] - 1)
                            shared_mem.buf[:fmt.size] = fmt.pack(*state)
                            send_evt(mq, "mort proie")
                        elif kind == "predator":
                            state[PRED] = max(0, state[PRED] - 1)
                            shared_mem.buf[:fmt.size] = fmt.pack(*state)
                            send_evt(mq, "mort predateur")
                        else:
                            client_socket.sendall(b"ERR unknown kind\n")
                            continue
                    finally:
                        mutex.release()

                    client_socket.sendall(b"OK\n")
                    print("[env] DIE accepted:", kind, "-> state =", state)

                else:
                    client_socket.sendall(b"ERR bad request\n")
                    print("[env] unknown command:", msg)


def main():
    # mutex
    try:
        mutex = sysv_ipc.Semaphore(SEMA_KEY, sysv_ipc.IPC_CREX, initial_value=1)
        print("[env] mutex created")
    except sysv_ipc.ExistentialError:
        mutex = sysv_ipc.Semaphore(SEMA_KEY)
        print("[env] mutex attached (already exists)")

    # reset shm stale
    try:
        old = shared_memory.SharedMemory(name=SHM_NAME, create=False)
        old.close()
        old.unlink()
        print("[env] stale shared memory removed")
    except FileNotFoundError:
        pass

    shared_mem = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=FMT.size)

    # init shm zeros
    apply_state(shared_mem, mutex, 0, 0, 0, 0, 0)
    print("[env] shared memory ready:", SHM_NAME, "size=", FMT.size)

    # MQ
    mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)

    # 1) attendre INIT
    predators, preys, grass = wait_for_init(mq)

    # 2) appliquer INIT
    apply_state(shared_mem, mutex, predators, preys, grass, 0, 0)
    print("[env] INIT applied to SHM")

    # 3) ack display
    send_evt(mq, "ACK_INIT")

    # 4) lancer listener MQ runtime (thread) -> inbox
    inbox = queue.Queue()
    mq_thread = threading.Thread(target=utils.mq_listener, args=(mq, TYPE_CMD, inbox), daemon=True)
    mq_thread.start()

    # 5) lancer socket server
    t = threading.Thread(target=socket_server, args=(shared_mem, mutex, FMT, mq), daemon=True)
    t.start()

    # 6) boucle principale
    running = True
    try:
        while running:
            # traiter toutes les commandes reçues depuis display
            while True:
                try:
                    cmd = inbox.get_nowait()
                except queue.Empty:
                    break
                running = handle_cmd(cmd, shared_mem, mutex, mq)
                if not running:
                    break

            # tick
            time.sleep(1)
            st = read_state(shared_mem, mutex)
            st[TICK] += 1
            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
            print("[env] state =", st)

    except KeyboardInterrupt:
        print("[env] shutting down (Ctrl+C)")
    finally:
        # cleanup
        try:
            shared_mem.close()
            shared_mem.unlink()
        except Exception:
            pass
        try:
            mutex.remove()
        except Exception:
            pass
        try:
            mq.remove()
        except Exception:
            pass
        print("[env] cleanup done")


if __name__ == "__main__":
    main()
