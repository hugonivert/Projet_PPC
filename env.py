from multiprocessing import shared_memory
import struct
import sysv_ipc
import socket
import threading
import time

# constantes (évite les incohérences de noms / variables non définies)
SHM_NAME = "WORLD"
SEMA_KEY = 0x1234
FMT = struct.Struct("iiiii")  # 5 entiers (pred, prey, grass, drought, tick)
MQ_KEY = 0x5678

# indices dans le "tableau" (layout)
PRED, PREY, GRASS, DROUGHT, TICK = 0, 1, 2, 3, 4



# env Création de la mémoire partagée
def main():
    # création/attachement du mutex
    try:
        mutex = sysv_ipc.Semaphore(SEMA_KEY, sysv_ipc.IPC_CREX, initial_value=1)  # création du mutex
        print("[env] mutex created")
    except sysv_ipc.ExistentialError:
        mutex = sysv_ipc.Semaphore(SEMA_KEY)
        print("[env] mutex attached (already exists)")

    # création/initialisation de la mémoire partagée
    # (si elle existe déjà, on la supprime pour repartir propre)
    try:
        old = shared_memory.SharedMemory(name=SHM_NAME, create=False)
        old.close()
        old.unlink()
        print("[env] stale shared memory removed")
    except FileNotFoundError:
        pass

    shared_mem = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=FMT.size)

    # initialisation des octets de la mémoire partagée (protégée par mutex)
    mutex.acquire()
    try:
        shared_mem.buf[:FMT.size] = FMT.pack(0, 0, 0, 0, 0)
    finally:
        mutex.release()

    print("[env] shared memory ready:", SHM_NAME, "size=", FMT.size)

    # création de la message queue
    mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)
    mq_thread = threading.Thread(target=mq_listener, args=(mq,), daemon=True)
    mq_thread.start()
    # monopolisation d'un thread pour l'écoute du socket
    t = threading.Thread(target=socket_server, args=(shared_mem, mutex, FMT), daemon=True)
    t.start()

    # boucle principale env (simulation / tick) - à enrichir ensuite
    try:
        while True:
            time.sleep(1)
            # exemple: lecture d'état (facultatif, juste debug)
            mutex.acquire()
            try:
                state = FMT.unpack(shared_mem.buf[:FMT.size])
            finally:
                mutex.release()
            print("[env] state =", state)
    except KeyboardInterrupt:
        print("[env] shutting down (Ctrl+C)")
    finally:
        # cleanup ressources (important)
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


def socket_server(shared_mem, mutex, fmt):
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
                args=(client_socket, address, shared_mem, mutex, fmt),
                daemon=True
            )
            t.start()

'''
def handle_client(client_socket, address, shared_mem, mutex, fmt):
    with client_socket:
        print("[env] handling client:", address)

        data = client_socket.recv(1024)
        if not data:
            print("[env] client disconnected:", address)
            return

        msg = data.decode().strip()     # ex: "JOIN prey" ou "JOIN predator"
        parts = msg.split()             # décomposition du message

        if len(parts) == 2 and parts[0] == "JOIN":
            kind = parts[1]  # "prey" ou "predator"

            mutex.acquire()
            try:
                state = list(fmt.unpack(shared_mem.buf[:fmt.size]))

                if kind == "prey":
                    state[PREY] += 1
                elif kind == "predator":
                    state[PRED] += 1
                else:
                    client_socket.sendall(b"ERR unknown kind\n")
                    return

                shared_mem.buf[:fmt.size] = fmt.pack(*state)
            finally:
                mutex.release()

            client_socket.sendall(b"OK\n")
            print("[env] JOIN accepted:", kind, "-> state =", state)
        else:
            client_socket.sendall(b"ERR bad request\n")

        print("[env] client disconnected:", address)

'''

'''
code proposé par chat 
Cette version de handle_client permet de garder une connexion socket ouverte 
et de traiter plusieurs commandes successives envoyées par le même client,
au lieu de fermer la connexion après une seule requête.
'''



def handle_client(client_socket, address, shared_mem, mutex, fmt):
    print("[env] handling client:", address)

    buffer = b""
    with client_socket:
        while True:
            chunk = client_socket.recv(1024)
            if not chunk:
                print("[env] client disconnected:", address)
                break

            buffer += chunk

            # Tant qu'on a au moins une ligne complète (terminée par \n)
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                msg = line.decode().strip()

                if not msg:
                    continue

                parts = msg.split()

                # -------------------------
                # Commande JOIN
                # -------------------------
                if len(parts) == 2 and parts[0] == "JOIN":
                    kind = parts[1]  # "prey" ou "predator"

                    mutex.acquire()
                    try:
                        state = list(fmt.unpack(shared_mem.buf[:fmt.size]))

                        if kind == "prey":
                            state[PREY] += 1
                        elif kind == "predator":
                            state[PRED] += 1
                        else:
                            client_socket.sendall(b"ERR unknown kind\n")
                            continue

                        shared_mem.buf[:fmt.size] = fmt.pack(*state)
                    finally:
                        mutex.release()

                    client_socket.sendall(b"OK\n")
                    print("[env] JOIN accepted:", kind, "-> state =", state)

                # -------------------------
                # Commande DIE (ex: "DIE prey prey-123")
                # Pour l'instant: décrémente juste le compteur
                # -------------------------
                elif len(parts) >= 2 and parts[0] == "DIE":
                    kind = parts[1]  # "prey" ou "predator"

                    mutex.acquire()
                    try:
                        state = list(fmt.unpack(shared_mem.buf[:fmt.size]))

                        if kind == "prey":
                            state[PREY] = max(0, state[PREY] - 1)
                        elif kind == "predator":
                            state[PRED] = max(0, state[PRED] - 1)
                        else:
                            client_socket.sendall(b"ERR unknown kind\n")
                            continue

                        shared_mem.buf[:fmt.size] = fmt.pack(*state)
                    finally:
                        mutex.release()

                    client_socket.sendall(b"OK\n")
                    print("[env] DIE accepted:", kind, "-> state =", state)

                else:
                    client_socket.sendall(b"ERR bad request\n")
                    print("[env] unknown command:", msg)



def mq_listener(mq):
    while True:
            msg, mtype = mq.receive()   # bloque jusqu'à recevoir un message
            text = msg.decode()

            # TODO: traiter la commande (ex: "STATUS", "PAUSE", "QUIT"...)
            print("[env] MQ received:", mtype, text)


if __name__ == "__main__":
    main()
