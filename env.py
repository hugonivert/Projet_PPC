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

import utils
import prey
import predator
import cleanup

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

drought_until_tick = None
toggle_drought_requested = False

def send_evt(mq,text):
    """
    Envoie un événement (texte) au processus display via la System V MessageQueue.

    But:
        Permet à env de notifier display de tout événement significatif (morts,
        naissances, prédation, pousse d’herbe, sécheresse, ACK, erreurs, etc.).

    Fonctionnement:
        - Le message est envoyé sur le type TYPE_EVT (env -> display).
        - Le message est encodé en UTF-8 (bytes) car sysv_ipc MQ transporte des bytes.
        - La réception côté display consomme le message (receive()).
    """
    mq.send(text.encode("utf-8"), type=TYPE_EVT)


def set_drought_env(shared_mem, mutex, mq, st: list[int], on: bool, reason: str) -> None:
    """
    Bascule la secheresse dans l'etat global et notifie display.

    Effets:
        - Met a jour la SHM (flag drought + tick)
        - Envoie EV_DROUGHT a display
        - Gere la duree via drought_until_tick
    """
    global drought_until_tick

    if on:
        if st[DROUGHT] == 1:
            return
        duration = random.randint(DROUGHT_MIN_TICKS, DROUGHT_MAX_TICKS)
        st[DROUGHT] = 1
        drought_until_tick = st[TICK] + duration
        apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
        send_evt(mq, EV_DROUGHT)
        print(f"[env] drought ON ({reason}, {duration} ticks)")
    else:
        if st[DROUGHT] == 0:
            return
        st[DROUGHT] = 0
        drought_until_tick = None
        apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
        send_evt(mq, EV_DROUGHT)
        print(f"[env] drought OFF ({reason})")


def on_drought_signal(signum, frame) -> None:
    del signum, frame
    global toggle_drought_requested
    toggle_drought_requested = True

def apply_state(shared_mem, mutex, pred: int, prey_n: int, grass: int, drought: int, tick: int) -> None:
    """
    Écrit l'état global de la simulation dans la mémoire partagée (SHM), de façon atomique.

    But:
        Maintenir une “source de vérité” simple dans une SHM (5 entiers) accessible
        aux processus/threads du système (env, prey, predator, display si besoin).

    Fonctionnement:
        - Prend un mutex System V (Semaphore) pour garantir l’exclusion mutuelle.
        - Packe les 5 entiers selon FMT (struct) dans un buffer binaire.
        - Écrit le buffer directement dans shared_mem.buf.

    Paramètres:
        pred, prey_n, grass, drought, tick:
            Les 5 valeurs entières correspondant à l’état global.
    """
    mutex.acquire()
    try:
        shared_mem.buf[:FMT.size] = FMT.pack(pred, prey_n, grass, drought, tick)
    finally:
        mutex.release()


def read_state(shared_mem, mutex) -> list[int]:
    """
    Lit l'état global (5 entiers) depuis la mémoire partagée (SHM), de façon atomique.

    But:
        Obtenir un snapshot cohérent de (pred, prey, grass, drought, tick) sans
        lire pendant qu’un autre thread/processus écrit.

    Fonctionnement:
        - Prend le mutex System V (Semaphore).
        - Lit FMT.size octets.
        - Unpack les octets via struct.Struct et renvoie une liste d’entiers.
    """
    mutex.acquire()
    try:
        return list(FMT.unpack(shared_mem.buf[:FMT.size]))
    finally:
        mutex.release()


def wait_for_init(mq: sysv_ipc.MessageQueue) -> tuple[int, int, int]:
    """
    Bloque env tant que display n’a pas envoyé le message INIT.

    But:
        Synchroniser le démarrage : l’utilisateur choisit la population initiale
        dans display, puis env ne commence la simulation qu’après réception.

    Protocole:
        - display -> env (TYPE_CMD):
          "INIT <predators> <preys> <grass>"

    Fonctionnement:
        - Boucle infinie.
        - mq.receive(type=TYPE_CMD) bloque jusqu’à un message de commande.
        - Parse la string; si format OK, renvoie (predators, preys, grass).
        - Ignore tout autre message de commande.
    """
    print("[env] Waiting for INIT from display...")
    while True:
        msg, mtype = mq.receive(type=TYPE_CMD)  # BLOQUANT
        text = msg.decode("utf-8", errors="replace").strip()
        print("[env] INIT channel got:", text)

        parts = text.split()
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

    But:
        Permettre à display (donc à l’utilisateur) de piloter env “à chaud” :
        obtenir l’état, quitter, modifier des paramètres (herbe, sécheresse),
        pause/resume (acknowledgement pour l’instant).

    Entrées (exemples):
        - "STATUS"              -> env renvoie un snapshot textuel "STATE ..."
        - "QUIT"                -> env renvoie "ACK_QUIT" et stoppe la boucle main
        - "SET GRASS <n>"       -> modifie grass dans la SHM
        - "SET DROUGHT <0|1>"   -> modifie drought dans la SHM + notifie display
        - "PAUSE" / "RESUME"    -> ack (la logique de pause peut être ajoutée ensuite)

    Retour:
        - True  : env continue
        - False : env doit arrêter sa boucle principale
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

        st = read_state(shared_mem, mutex)
        if field == "GRASS":
            st[GRASS] = max(0, val)
            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
            send_evt(mq, f"ACK_SET GRASS {st[GRASS]}")
        elif field == "DROUGHT":
            desired = 1 if val != 0 else 0
            set_drought_env(shared_mem, mutex, mq, st, desired == 1, "manual")
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


def spawn_prey(procs: list[Process], prey_procs: list[Process]) -> None:
    """
    Crée et démarre un nouveau processus proie (prey) et l’ajoute à la liste procs.

    But:
        Permettre à env de “matérialiser” une naissance (REPRODUCE prey) en lançant
        un nouveau process indépendant exécutant prey.main().

    Fonctionnement:
        - multiprocessing.Process(target=prey.main)
        - start()
        - append dans procs pour pouvoir terminate() au cleanup
    """
    p = Process(target=prey.main)
    p.start()
    procs.append(p)
    prey_procs.append(p)


def spawn_predator(procs: list[Process], pred_procs: list[Process]) -> None:
    """
    Crée et démarre un nouveau processus prédateur (predator) et l’ajoute à procs.

    But:
        Permettre à env de créer un nouveau prédateur lors d’une reproduction
        (REPRODUCE pred / predator).

    Fonctionnement:
        - multiprocessing.Process(target=predator.main)
        - start()
        - append dans procs pour le cleanup
    """
    p = Process(target=predator.main)
    p.start()
    procs.append(p)
    pred_procs.append(p)


def spawn_population(n_pred: int, n_prey: int, procs: list[Process], prey_procs: list[Process], pred_procs: list[Process]) -> None:
    """
    Lance la population initiale (n_prey proies + n_pred prédateurs).

    But:
        Après INIT, env doit créer exactement le nombre de processus correspondant
        au choix utilisateur.

    Fonctionnement:
        - Pour chaque proie : spawn_prey()
        - Pour chaque prédateur : spawn_predator()
        - Retourne la liste complète des Process pour gestion/cleanup
    """
    for _ in range(n_prey):
        spawn_prey(procs, prey_procs)
    for _ in range(n_pred):
        spawn_predator(procs, pred_procs)


def socket_server(shared_mem, mutex, fmt, mq, procs: list[Process], prey_procs: list[Process], pred_procs: list[Process], joined_counts, joined_lock):
    """
    Serveur TCP dans env : point d’entrée des événements envoyés par les processus prey/predator.

    But:
        Centraliser dans env la logique “monde” :
        - Les agents (prey/predator) envoient des commandes simples sur socket.
        - env met à jour la SHM (compteurs) et notifie display via MQ.

    Fonctionnement:
        - Ouvre un socket TCP sur localhost:6666
        - accept() en boucle
        - pour chaque client, démarre un thread handle_client(...) (connexion persistante)
    """
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
                args=(client_socket, address, shared_mem, mutex, fmt, mq, procs, prey_procs, pred_procs, joined_counts, joined_lock),
                daemon=True
            )
            t.start()


def handle_client(client_socket, address, shared_mem, mutex, fmt, mq, procs: list[Process], prey_procs: list[Process], pred_procs: list[Process], joined_counts, joined_lock):
    """
    Gère une connexion TCP persistante provenant d’un agent (prey ou predator).

    But:
        - Lire des commandes ligne-par-ligne (séparées par '\\n')
        - Appliquer les effets sur l’état global (SHM)
        - Envoyer les événements correspondants à display via MQ
        - Répondre "OK\\n" ou "ERR ...\\n" au client (agent)

    Protocole commandes supportées:
        - JOIN prey|predator
        - DIE prey <id>  / DIE pred <id>
        - EAT_PREY <id>   -> si PREY>0 : décrémente + event Predator eats Prey; sinon ERR no prey
        - EAT_GRASS <id>  -> si GRASS>0 : décrémente + event Prey eats Grass; sinon ERR no grass
        - REPRODUCE prey <id> -> incrémente + spawn nouveau prey + event Prey reproduces
        - REPRODUCE pred <id> -> incrémente + spawn nouveau predator + event Predator reproduces

    Fonctionnement:
        - Accumule des bytes dans un buffer
        - Découpe dès qu’un '\\n' est présent (commande complète)
        - Parse et dispatch vers la logique associée
    """
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

                if len(parts) == 2 and parts[0] == "JOIN":
                    kind = parts[1]
                    if kind in ("prey", "predator"):
                        with joined_lock:
                            if kind == "prey":
                                joined_counts["prey"] += 1
                            else:
                                joined_counts["pred"] += 1
                        client_socket.sendall(b"OK\n")
                    else:
                        client_socket.sendall(b"ERR unknown kind\n")

                elif len(parts) >= 2 and parts[0] == "DIE":
                    kind = parts[1]
                    st = read_state(shared_mem, mutex)

                    if kind == "prey":
                        if st[PREY] > 0:
                            st[PREY] -= 1
                            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
                        with joined_lock:
                            if joined_counts["prey"] > 0:
                                joined_counts["prey"] -= 1
                        send_evt(mq, EV_PREY_DIES)
                        client_socket.sendall(b"OK\n")

                    elif kind in ("pred", "predator"):
                        if st[PRED] > 0:
                            st[PRED] -= 1
                            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
                        with joined_lock:
                            if joined_counts["pred"] > 0:
                                joined_counts["pred"] -= 1
                        send_evt(mq, EV_PRED_DIES)
                        client_socket.sendall(b"OK\n")

                    else:
                        client_socket.sendall(b"ERR bad DIE kind\n")

                elif len(parts) == 1 and parts[0] == "EAT_PREY":
                    st = read_state(shared_mem, mutex)

                    if st[PRED] <= 0:
                        client_socket.sendall(b"ERR no predator\n")
                    elif st[PREY] > 0:
                        if random.random() < PREY_ESCAPE_PROB:
                            send_evt(mq, EV_PREY_ESCAPES)
                            client_socket.sendall(b"ESCAPE\n")
                        else:
                            st[PREY] -= 1
                            apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
                            with joined_lock:
                                if joined_counts["prey"] > 0:
                                    joined_counts["prey"] -= 1
                            send_evt(mq, EV_PRED_EATS_PREY)
                            client_socket.sendall(b"OK\n")
                            if prey_procs:
                                p = prey_procs.pop()
                                try:
                                    p.terminate()
                                except Exception:
                                    pass
                                try:
                                    procs.remove(p)
                                except ValueError:
                                    pass
                    else:
                        client_socket.sendall(b"ERR no prey\n")

                elif len(parts) == 2 and parts[0] == "EAT_GRASS":
                    st = read_state(shared_mem, mutex)

                    if st[PREY] <= 0:
                        client_socket.sendall(b"ERR no prey\n")
                    elif st[GRASS] > 0:
                        st[GRASS] -= 1
                        apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
                        send_evt(mq, EV_PREY_EATS_GRASS)
                        client_socket.sendall(b"OK\n")
                    else:
                        client_socket.sendall(b"ERR no grass\n")

                elif len(parts) >= 2 and parts[0] == "REPRODUCE":
                    kind = parts[1]
                    st = read_state(shared_mem, mutex)

                    if kind == "prey":
                        if st[PREY] <= 0:
                            client_socket.sendall(b"ERR no prey\n")
                            continue
                        st[PREY] += 1
                        apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
                        send_evt(mq, EV_PREY_REPRO)
                        spawn_prey(procs, prey_procs)
                        client_socket.sendall(b"OK\n")

                    elif kind in ("pred", "predator"):
                        if st[PRED] <= 0:
                            client_socket.sendall(b"ERR no predator\n")
                            continue
                        st[PRED] += 1
                        apply_state(shared_mem, mutex, st[PRED], st[PREY], st[GRASS], st[DROUGHT], st[TICK])
                        send_evt(mq, EV_PRED_REPRO)
                        spawn_predator(procs, pred_procs)
                        client_socket.sendall(b"OK\n")

                    else:
                        client_socket.sendall(b"ERR bad REPRODUCE kind\n")

                else:
                    client_socket.sendall(b"ERR bad request\n")
                    print("[env] unknown command:", msg)


def main():
    """
    Point d’entrée du processus env : initialise IPC + attend INIT + lance la simulation.

    Responsabilités:
        1) Initialiser le mutex System V (Semaphore) utilisé pour protéger la SHM.
        2) Créer (ou recréer) la mémoire partagée SHM_NAME et initialiser les 5 entiers.
        3) Créer/attacher la System V MessageQueue (MQ_KEY).
        4) Bloquer sur wait_for_init() jusqu’au choix utilisateur envoyé par display.
        5) Mettre l’état initial dans la SHM et notifier display via "ACK_INIT".
        6) Lancer:
           - un thread mq_listener (commands display->env) qui push dans une Queue inbox
           - un thread socket_server (agents prey/predator -> env)
           - la population initiale (spawn_population)
        7) Boucle principale:
           - consommer les commandes inbox (STATUS/SET/QUIT/...)
           - avancer le tick
           - faire pousser l’herbe aléatoirement selon drought et notifier display via EV_GRASS_GROWS
        8) Cleanup: terminate agents, unlink SHM, remove semaphore, remove MQ.
    """
    global toggle_drought_requested

    # cleanup stale IPC from a previous run (if any)
    cleanup.cleanup_ipc(SHM_NAME, SEMA_KEY, MQ_KEY, remove_mq=False)

    # mutex
    try:
        mutex = sysv_ipc.Semaphore(SEMA_KEY, sysv_ipc.IPC_CREX, initial_value=1)
        print("[env] mutex created")
    except sysv_ipc.ExistentialError:
        mutex = sysv_ipc.Semaphore(SEMA_KEY)
        print("[env] mutex attached (already exists)")

    shared_mem = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=FMT.size)
    apply_state(shared_mem, mutex, 0, 0, 0, 0, 0)
    print("[env] shared memory ready:", SHM_NAME, "size=", FMT.size)

    # MQ
    mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)

    # 1) attendre INIT
    predators_n, preys_n, grass_n = wait_for_init(mq)

    # 2) appliquer INIT dans SHM
    apply_state(shared_mem, mutex, predators_n, preys_n, grass_n, 0, 0)
    print("[env] INIT applied to SHM")

    # 3) ACK
    send_evt(mq, "ACK_INIT")

    # signal handler for drought start (external process)
    signal.signal(signal.SIGUSR1, on_drought_signal)


    # 4) listener MQ runtime (display -> env)
    inbox = queue.Queue()
    joined_counts = {"pred": 0, "prey": 0}
    joined_lock = threading.Lock()
    mq_thread = threading.Thread(target=utils.mq_listener, args=(mq, TYPE_CMD, inbox), daemon=True)
    mq_thread.start()

    # 5) socket server (prey/predator -> env)
    procs: list[Process] = []
    prey_procs: list[Process] = []
    pred_procs: list[Process] = []
    t = threading.Thread(
        target=socket_server,
        args=(shared_mem, mutex, FMT, mq, procs, prey_procs, pred_procs, joined_counts, joined_lock),
        daemon=True,
    )
    t.start()

    running = True
    # 6) spawn population initiale (d'après INIT)
    time.sleep(0.2)
    spawn_population(predators_n, preys_n, procs, prey_procs, pred_procs)
    print(f"[env] spawned initial processes: pred={predators_n}, prey={preys_n}")
    st = read_state(shared_mem, mutex)
    if not check_prey_limit(st, mq):
        running = False

    # 7) boucle principale
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
                    set_drought_env(shared_mem, mutex, mq, st, True, "signal")
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
            if st[DROUGHT] == 1 and drought_until_tick is not None and st[TICK] >= drought_until_tick:
                set_drought_env(shared_mem, mutex, mq, st, False, "timeout")

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
        for p in procs:
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
