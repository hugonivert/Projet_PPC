# env.py
from multiprocessing import shared_memory, Process
import struct
import sysv_ipc
import socket
import threading
import time
import queue
import random
import signal
import os

import utils
import prey
import predator
import cleanup
import drought

# constantes
SHM_NAME = "WORLD"
SEMA_KEY = 0x1234
FMT = struct.Struct("iiiii")  # pred, prey, grass, drought, tick
MQ_KEY = 0x5678

TYPE_CMD = 1   # display -> env
TYPE_EVT = 2   # env -> display

# indices
PRED, PREY, GRASS, DROUGHT, TICK = 0, 1, 2, 3, 4

# Events (strings EXACTES attendues par display)
EV_PRED_EATS_PREY  = "Predator eats Prey"
EV_PREY_EATS_GRASS = "Prey eats Grass"
EV_GRASS_GROWS     = "Grass grows"
EV_PRED_REPRO      = "Predator reproduces"
EV_PREY_REPRO      = "Prey reproduces"
EV_PREY_DIES       = "Prey dies"
EV_PRED_DIES       = "Predator dies"
EV_DROUGHT         = "Drought"
EV_GRASS_WITHERS   = "Grass withers"
PREY_ESCAPE_PROB   = 0.35
EV_PREY_ESCAPES    = "Prey escapes"
EV_SIM_STOP        = "SIM_STOP"
PREY_LIMIT         = 120 #pour éviter de faire planter le pc
DROUGHT_MIN_TICKS  = 10
DROUGHT_MAX_TICKS  = 20
DROUGHT_SIGNAL_MIN_DELAY = 10
DROUGHT_SIGNAL_MAX_DELAY = 100

drought_until_tick = None
toggle_drought_requested = False

def send_evt(mq,text):
    """
    Envoie un événement (texte) au processus display via la System V MessageQueue.
    """
    mq.send(text.encode("utf-8"), type=TYPE_EVT)


def set_drought_env(shared_mem, mutex, mq, on: bool, reason: str) -> None:
    """
    Bascule la secheresse dans l'etat global et notifie display.
    """
    global drought_until_tick

    changed = False
    duration = None

    def updater(st):
        nonlocal changed, duration
        global drought_until_tick
        if on:
            if st[DROUGHT] == 1:
                return
            duration = random.randint(DROUGHT_MIN_TICKS, DROUGHT_MAX_TICKS)  # temps aléatoire de la sécheresse
            st[DROUGHT] = 1
            drought_until_tick = st[TICK] + duration  # fin de la sécheresse
            changed = True
        else:
            if st[DROUGHT] == 0:
                return
            st[DROUGHT] = 0
            drought_until_tick = None
            changed = True

    update_state(shared_mem, mutex, updater)

    if not changed:
        return
    send_evt(mq, EV_DROUGHT)
    if on:
        print(f"[env] drought ON ({reason}, {duration} ticks)")
    else:
        print(f"[env] drought OFF ({reason})")


def on_drought_signal(signum, frame) -> None:
    del signum, frame
    global toggle_drought_requested
    toggle_drought_requested = True

def apply_state(shared_mem, mutex, pred: int, prey_n: int, grass: int, drought: int, tick: int) -> None:
    """
    Écrit l'état global de la simulation dans la mémoire partagée (SHM), de façon atomique.
    """
    mutex.acquire()
    try:
        shared_mem.buf[:FMT.size] = FMT.pack(pred, prey_n, grass, drought, tick)
    finally:
        mutex.release()


def read_state(shared_mem, mutex) -> list[int]:
    """
    Lit l'état global (5 entiers) depuis la mémoire partagée (SHM), de façon atomique.
    """
    mutex.acquire()
    try:
        return list(FMT.unpack(shared_mem.buf[:FMT.size]))
    finally:
        mutex.release()


def update_state(shared_mem, mutex, update_fn) -> list[int]:
    """
    Lit-modifie-écrit l'état global dans la SHM sous un seul mutex (atomique).
    """
    mutex.acquire()
    try:
        st = list(FMT.unpack(shared_mem.buf[:FMT.size]))
        update_fn(st)
        shared_mem.buf[:FMT.size] = FMT.pack(st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
        return st
    finally:
        mutex.release()


def wait_for_init(mq: sysv_ipc.MessageQueue) -> tuple[int, int, int]:
    """
    Bloque env tant que display n'a pas envoyé le message INIT.
    """
    print("[env] Waiting for INIT from display...")
    while True:
        msg, mtype = mq.receive(type=TYPE_CMD)  # BLOQUANT
        text = msg.decode("utf-8", errors="replace").strip() #décoder + enlever les /n
        print("[env] INIT channel got:", text)

        parts = text.split() #découpe en parties
        if len(parts) == 4 and parts[0] == "INIT":
            try:
                predators_n = int(parts[1])
                preys_n = int(parts[2])
                grass_n = int(parts[3])
                return predators_n, preys_n, grass_n
            except ValueError:
                print("[env] Bad INIT numbers:", text)
                continue

        print("[env] Ignored (expect 'INIT p r g'):", text)


def handle_cmd(cmd: str, shared_mem, mutex, mq, joined_counts, joined_lock) -> bool:
    """
    Traite une commande runtime envoyée par display pendant la simulation.
    """
    parts = cmd.split()
    if not parts:
        return True

    if parts[0] == "STATUS":
        st = read_state(shared_mem, mutex)
        with joined_lock:
            joined_pred = joined_counts["pred"]
            joined_prey = joined_counts["prey"]
        send_evt(
            mq,
            "STATE "
            f"pred={st[PRED]} "
            f"prey={st[PREY]} "
            f"grass={st[GRASS]} "
            f"drought={st[DROUGHT]} "
            f"tick={st[TICK]} "
            f"joined_pred={joined_pred} "
            f"joined_prey={joined_prey}"
        )
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

        if field == "GRASS":
            def set_grass(st):
                st[GRASS] = max(0, val)

            st = update_state(shared_mem, mutex, set_grass)
            send_evt(mq, f"ACK_SET GRASS {st[GRASS]}")
        elif field == "DROUGHT":
            desired = 1 if val != 0 else 0
            set_drought_env(shared_mem, mutex, mq, desired == 1, "manual")
            st = read_state(shared_mem, mutex)
            send_evt(mq, f"ACK_SET DROUGHT {st[DROUGHT]}")
        else:
            send_evt(mq, "ERR unknown SET field")
        return True

    if parts[0] in ("PAUSE", "RESUME"):
        send_evt(mq, f"ACK_{parts[0]}")
        return True

    send_evt(mq, "ERR unknown command")
    return True


def check_prey_limit(st: list[int], mq) -> bool:
    if st[PREY] > PREY_LIMIT:
        send_evt(mq, f"{EV_SIM_STOP} max_prey={PREY_LIMIT}")
        return False
    return True


def spawn_prey(procs: list[Process], prey_procs: list[Process], procs_lock) -> None:
    """
    Crée et démarre un nouveau processus proie (prey) et l'ajoute à la liste procs.
    """
    p = Process(target=prey.main)
    p.start()
    with procs_lock:
        procs.append(p)
        prey_procs.append(p)


def spawn_predator(procs: list[Process], pred_procs: list[Process], procs_lock) -> None:
    """
    Crée et démarre un nouveau processus prédateur (predator) et l’ajoute à procs.
    """
    p = Process(target=predator.main)
    p.start()
    with procs_lock:
        procs.append(p)
        pred_procs.append(p)


def spawn_population(
    n_pred: int,
    n_prey: int,
    procs: list[Process],
    prey_procs: list[Process],
    pred_procs: list[Process],
    procs_lock,
) -> None:
    """
    Lance la population initiale (n_prey proies + n_pred prédateurs).
    """
    for _ in range(n_prey):
        spawn_prey(procs, prey_procs, procs_lock)
    for _ in range(n_pred):
        spawn_predator(procs, pred_procs, procs_lock)


def socket_server(shared_mem, mutex, fmt, mq, procs: list[Process], prey_procs: list[Process], pred_procs: list[Process], joined_counts, joined_lock, procs_lock):
    """
    Serveur TCP dans env : point d'entrée des événements envoyés par les processus prey/predator.
    """
    HOST = "localhost"
    PORT = 6666

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #relancer rapidement si erreur
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print("[env] socket server listening on", (HOST, PORT))

        while True:
            client_socket, address = server_socket.accept()
            print("[env] Connected:", address)
            t = threading.Thread(
                target=handle_client,
                args=(client_socket, address, shared_mem, mutex, fmt, mq, procs, prey_procs, pred_procs, joined_counts, joined_lock, procs_lock),
                daemon=True
            )
            t.start()


def handle_client(client_socket, address, shared_mem, mutex, fmt, mq, procs: list[Process], prey_procs: list[Process], pred_procs: list[Process], joined_counts, joined_lock, procs_lock):
    """
    Gère une connexion TCP persistante provenant d'un agent (prey ou predator).
    """
    print("[env] handling client:", address)
    buffer = b""
    client_kind = None
    predator_accounted = False

    with client_socket:
        while True:
            try:
                chunk = client_socket.recv(1024) #message reçu complet
            except (ConnectionResetError, OSError):
                print("[env] client disconnected:", address)
                if client_kind == "predator" and not predator_accounted:
                    def dec_pred(st):
                        if st[PRED] > 0:
                            st[PRED] -= 1
                    update_state(shared_mem, mutex, dec_pred)
                    with joined_lock:
                        if joined_counts["pred"] > 0:
                            joined_counts["pred"] -= 1
                    send_evt(mq, EV_PRED_DIES)
                break
            if not chunk:
                print("[env] client disconnected:", address)
                if client_kind == "predator" and not predator_accounted:
                    def dec_pred(st):
                        if st[PRED] > 0:
                            st[PRED] -= 1
                    update_state(shared_mem, mutex, dec_pred)
                    with joined_lock:
                        if joined_counts["pred"] > 0:
                            joined_counts["pred"] -= 1
                    send_evt(mq, EV_PRED_DIES)
                break

            buffer += chunk 
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1) #redécoupage un message
                msg = line.decode(errors="replace").strip() #décodage + nettoyage 
                if not msg:
                    continue

                parts = msg.split() #redécoupage en parties

                #ENSEMBLES DES MESSAGES TRAITABLE 

                #JOIN
                if len(parts) == 2 and parts[0] == "JOIN":
                    kind = parts[1]
                    if kind in ("prey", "predator"):
                        client_kind = kind
                        with joined_lock:
                            if kind == "prey":
                                joined_counts["prey"] += 1
                            else:
                                joined_counts["pred"] += 1
                        client_socket.sendall(b"OK\n")
                    else:
                        client_socket.sendall(b"ERR unknown kind\n")

                #DIE
                elif len(parts) >= 2 and parts[0] == "DIE":
                    kind = parts[1]
                    if kind == "prey":
                        def dec_prey(st): #pour s'assurer d'avoir une simulation cohérente 
                            if st[PREY] > 0:
                                st[PREY] -= 1

                        update_state(shared_mem, mutex, dec_prey)
                        with joined_lock: 
                            if joined_counts["prey"] > 0: #pour s'assurer d'avoir une simulation cohérente 
                                joined_counts["prey"] -= 1
                        send_evt(mq, EV_PREY_DIES)
                        client_socket.sendall(b"OK\n")

                    elif kind in ("predator", "pred"):
                        def dec_pred(st): #pour s'assurer d'avoir une simulation cohérente 
                            if st[PRED] > 0:
                                st[PRED] -= 1

                        update_state(shared_mem, mutex, dec_pred)
                        with joined_lock: #pour s'assurer d'avoir une simulation cohérente 
                            if joined_counts["pred"] > 0:
                                joined_counts["pred"] -= 1
                        predator_accounted = True
                        send_evt(mq, EV_PRED_DIES)
                        client_socket.sendall(b"OK\n")

                    else:
                        client_socket.sendall(b"ERR bad DIE kind\n")

                #EAT_PREY
                elif len(parts) >= 2 and parts[0] == "EAT_PREY":
                    outcome = {"no_pred": False, "no_prey": False, "escaped": False, "ate": False}

                    def eat_prey(st):
                        if st[PRED] <= 0: #pour s'assurer d'avoir une simulation cohérente 
                            outcome["no_pred"] = True
                            return
                        if st[PREY] <= 0: #pour s'assurer d'avoir une simulation cohérente 
                            outcome["no_prey"] = True
                            return
                        if random.random() < PREY_ESCAPE_PROB: #chasse loupée ?
                            outcome["escaped"] = True
                            return
                        st[PREY] -= 1
                        outcome["ate"] = True

                    update_state(shared_mem, mutex, eat_prey)

                    if outcome["no_pred"]:
                        client_socket.sendall(b"ERR no predator\n")
                    elif outcome["no_prey"]:
                        client_socket.sendall(b"ERR no prey\n")
                    elif outcome["escaped"]:
                        send_evt(mq, EV_PREY_ESCAPES)
                        client_socket.sendall(b"ESCAPE\n")
                    elif outcome["ate"]:
                        with joined_lock: #pour s'assurer d'avoir une simulation cohérente 
                            if joined_counts["prey"] > 0:
                                joined_counts["prey"] -= 1
                        send_evt(mq, EV_PRED_EATS_PREY)
                        client_socket.sendall(b"OK\n")
                        with procs_lock:
                            if prey_procs: #fermeture d'un processus
                                p = prey_procs.pop()
                            else:
                                p = None
                        if p is not None:
                            try:
                                p.terminate()
                            except Exception:
                                pass
                            with procs_lock:
                                try:
                                    procs.remove(p)
                                except ValueError:
                                    pass
                    else:
                        client_socket.sendall(b"ERR no prey\n")

                #EAT_GRASS
                elif len(parts) >= 2 and parts[0] == "EAT_GRASS":
                    outcome = {"no_prey": False, "no_grass": False, "ate": False}

                    def eat_grass(st):
                        if st[PREY] <= 0: #pour s'assurer d'avoir une siulation cohérente
                            outcome["no_prey"] = True
                            return
                        if st[GRASS] <= 0: #pour s'assurer d'avoir une siulation cohérente
                            outcome["no_grass"] = True
                            return
                        st[GRASS] -= 1
                        outcome["ate"] = True

                    update_state(shared_mem, mutex, eat_grass)

                    if outcome["no_prey"]:
                        client_socket.sendall(b"ERR no prey\n")
                    elif outcome["no_grass"]:
                        client_socket.sendall(b"ERR no grass\n")
                    elif outcome["ate"]:
                        send_evt(mq, EV_PREY_EATS_GRASS)
                        client_socket.sendall(b"OK\n")
                    else:
                        client_socket.sendall(b"ERR no grass\n")

                #REPRODUCE
                elif len(parts) >= 2 and parts[0] == "REPRODUCE":
                    kind = parts[1]
                    if kind == "prey":
                        spawn = {"ok": False, "no_prey": False}

                        def reproduce_prey(st):
                            if st[PREY] <= 0: #pour s'assurer d'avoir une siulation cohérente
                                spawn["no_prey"] = True
                                return
                            st[PREY] += 1
                            spawn["ok"] = True

                        update_state(shared_mem, mutex, reproduce_prey)

                        if spawn["no_prey"]:
                            client_socket.sendall(b"ERR no prey\n")
                            continue
                        send_evt(mq, EV_PREY_REPRO)
                        spawn_prey(procs, prey_procs, procs_lock)
                        client_socket.sendall(b"OK\n")

                    elif kind == "predator":
                        spawn = {"ok": False, "no_pred": False}

                        def reproduce_pred(st):
                            if st[PRED] <= 0: #pour s'assurer d'avoir une siulation cohérente
                                spawn["no_pred"] = True
                                return
                            st[PRED] += 1
                            spawn["ok"] = True

                        update_state(shared_mem, mutex, reproduce_pred)

                        if spawn["no_pred"]:
                            client_socket.sendall(b"ERR no predator\n")
                            continue
                        send_evt(mq, EV_PRED_REPRO)
                        spawn_predator(procs, pred_procs, procs_lock)
                        client_socket.sendall(b"OK\n")

                    else:
                        client_socket.sendall(b"ERR bad REPRODUCE kind\n")

                else:
                    client_socket.sendall(b"ERR bad request\n")
                    print("[env] unknown command:", msg)


def main():
    global toggle_drought_requested

    # cleanup pour s'assurer que tout est bien clean même si erreur à la fin du précédant lancement
    cleanup.cleanup_ipc(SHM_NAME, SEMA_KEY, MQ_KEY, remove_mq=False)

    # mutex
    try:
        mutex = sysv_ipc.Semaphore(SEMA_KEY, sysv_ipc.IPC_CREX, initial_value=1) #création mutex
        print("[env] mutex created")
    except sysv_ipc.ExistentialError:
        mutex = sysv_ipc.Semaphore(SEMA_KEY)
        print("[env] mutex attached (already exists)")

    shared_mem = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=FMT.size) #création SHM
    apply_state(shared_mem, mutex, 0, 0, 0, 0, 0) 
    print("[env] shared memory ready:", SHM_NAME, "size=", FMT.size)

    # MQ
    mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)

    # attendre INIT de display
    predators_n, preys_n, grass_n = wait_for_init(mq)

    # appliquer INIT dans SHM
    apply_state(shared_mem, mutex, predators_n, preys_n, grass_n, 0, 0)
    print("[env] INIT applied to SHM")

    # ACK
    send_evt(mq, "ACK_INIT")

    # mise en place du signal -> si un signal arrive -> on_drought_signal
    signal.signal(signal.SIGUSR1, on_drought_signal)

    # lance un process externe drought pour des secheresses aleatoires
    drought_proc = Process(
        target=drought.random_signal_loop,
        args=(os.getpid(), DROUGHT_SIGNAL_MIN_DELAY, DROUGHT_SIGNAL_MAX_DELAY),
        daemon=True,
    )
    drought_proc.start()

    # listener MQ (display -> env)
    inbox = queue.Queue()
    joined_counts = {"pred": 0, "prey": 0}
    joined_lock = threading.Lock()
    mq_thread = threading.Thread(target=utils.mq_listener, args=(mq, TYPE_CMD, inbox), daemon=True)
    mq_thread.start()

    # socket server (prey/predator -> env)
    procs: list[Process] = [] #liste tous les processus pour fermeture
    prey_procs: list[Process] = [] #liste toutes les preys pour die et eat_prey
    pred_procs: list[Process] = [] #liste tous les prédateurs pour die
    procs_lock = threading.Lock()
    t = threading.Thread(
        target=socket_server,
        args=(shared_mem, mutex, FMT, mq, procs, prey_procs, pred_procs, joined_counts, joined_lock, procs_lock),
        daemon=True,
    )
    t.start()

    running = True
    # spawn population initiale (d'après INIT)
    time.sleep(0.2)
    spawn_population(predators_n, preys_n, procs, prey_procs, pred_procs, procs_lock)
    print(f"[env] spawned initial processes: pred={predators_n}, prey={preys_n}")
    st = read_state(shared_mem, mutex)
    if not check_prey_limit(st, mq): #s'assurer qu'il y a - de 120 proies dès le début
        running = False

    # boucle principale
    try:
        while running:
            # traiter commandes display->env
            while True:
                try:
                    cmd = inbox.get_nowait()
                except queue.Empty:
                    break
                running = handle_cmd(cmd, shared_mem, mutex, mq, joined_counts, joined_lock)
                if not running:
                    break

            # signal drought request
            if toggle_drought_requested:
                st = read_state(shared_mem, mutex)
                if st[DROUGHT] == 0:
                    set_drought_env(shared_mem, mutex, mq, True, "signal")
                toggle_drought_requested = False

            time.sleep(1)

            # tick + herbe (mise a jour atomique pour eviter les pertes)
            mutex.acquire()
            try:
                st = list(FMT.unpack(shared_mem.buf[:FMT.size]))
                st[TICK] += 1

                if st[DROUGHT] == 0:
                    grow = random.randint(5, 10)
                else:
                    grow = -random.randint(3, 5)

                if grow >= 0:
                    st[GRASS] += grow
                    delta_txt = f"(grass +{grow})"
                    loss = 0
                else:
                    loss = min(-grow, st[GRASS])
                    st[GRASS] -= loss
                    delta_txt = f"(grass -{loss})"
                    grow = 0

                shared_mem.buf[:FMT.size] = FMT.pack(
                    st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK]
                )
            finally:
                mutex.release()

            # fin automatique de secheresse (apres mise a jour tick)
            if st[DROUGHT] == 1 and drought_until_tick is not None and st[TICK] >= drought_until_tick: #fin de drought
                set_drought_env(shared_mem, mutex, mq, False, "timeout")

            # On envoie les events d'herbe apres la mise a jour SHM.
            for _ in range(grow):
                send_evt(mq, EV_GRASS_GROWS)
            for _ in range(loss):
                send_evt(mq, EV_GRASS_WITHERS)

            print("[env] state =", st, delta_txt)
            running = check_prey_limit(st, mq)

    except KeyboardInterrupt:
        print("[env] shutting down (Ctrl+C)")
    finally:
        # cleanup
        if drought_proc is not None:
            try:
                drought_proc.terminate()
                drought_proc.join(timeout=2)
            except Exception:
                pass
        with procs_lock:
            procs_snapshot = list(procs)
        for p in procs_snapshot:
            try:
                p.terminate()
            except Exception:
                pass

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
