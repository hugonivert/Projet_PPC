# display.py
import sysv_ipc
import threading
import queue
import time

MQ_KEY = 0x5678

# Mapping cohérent avec env.py
TYPE_CMD = 1   # display -> env (commandes: INIT, PAUSE, QUIT...)
TYPE_EVT = 2   # env -> display (événements: naissance, mort, sécheresse...)

state = {
    "predators": 0,
    "preys": 0,
    "grass": 0,
    "drought": False,
}


def print_state():
    """
    Affiche un résumé lisible de l’état courant côté display.

    But:
        Donner à l’utilisateur une vision rapide des compteurs “live”
        tels que display les suit (en se basant sur les events reçus).

    Fonctionnement:
        - Convertit le bool drought en "ON"/"OFF"
        - Print une ligne formatée avec predators, preys, grass, drought
    """
    drought_txt = "ON" if state["drought"] else "OFF"
    print(f"[display] State => predators={state['predators']} | preys={state['preys']} | grass={state['grass']} | drought={drought_txt}")


def mq_listener(mq: sysv_ipc.MessageQueue, ntype: int, inbox: queue.Queue):
    """
    Thread de réception MQ côté display (consumer bas niveau).

    But:
        Recevoir en continu les événements envoyés par env sans bloquer la boucle
        principale d’affichage/interaction.

    Fonctionnement:
        - Appelle mq.receive(type=ntype) de façon bloquante (pas de polling CPU).
        - Décode le message (UTF-8) puis le push dans une queue Python (inbox),
          thread-safe, lue par le thread principal.
        - Gère les erreurs typiques:
          * ExistentialError si la MQ est supprimée
          * autres exceptions -> log + petite pause
    """
    while True:
        try:
            msg, mtype = mq.receive(type=ntype)  # bloque
            text = msg.decode("utf-8", errors="replace").strip()
            inbox.put(text)
        except sysv_ipc.ExistentialError:
            print("[display] MQ removed, listener stops")
            break
        except Exception as e:
            print("[display] MQ listener error:", e)
            time.sleep(0.1)


def send_init(mq: sysv_ipc.MessageQueue, predators: int, preys: int, grass: int):
    """
    Envoie la commande INIT à env pour démarrer la simulation avec les paramètres utilisateur.

    But:
        Déclencher le démarrage “synchronisé” de env (env est bloqué sur wait_for_init()).

    Protocole:
        - Message textuel: "INIT <predators> <preys> <grass>"
        - Envoyé sur TYPE_CMD (display -> env)

    Fonctionnement:
        - Construit la ligne INIT
        - Encode en UTF-8
        - mq.send(..., type=TYPE_CMD)
    """
    cmd = f"INIT {predators} {preys} {grass}"
    mq.send(cmd.encode("utf-8"), type=TYPE_CMD)
    print("[display] Sent:", cmd)


def handle_message(text: str):
    """
    Traite un événement reçu depuis env et met à jour l’état local + affiche un message joueur.

    But:
        display maintient ses compteurs (predators/preys/grass/drought) en temps réel,
        uniquement à partir des événements (flux MQ). Après chaque événement, display
        peut informer l’utilisateur et rafraîchir les compteurs.

    Événements attendus (strings):
        - "ACK_INIT"                : env confirme qu’il a appliqué INIT
        - "Predator eats Prey"      : une proie disparaît (-1 prey)
        - "Prey eats Grass"         : une herbe disparaît (-1 grass)
        - "Grass grows"             : une herbe apparaît (+1 grass)
        - "Predator reproduces"     : +1 predator
        - "Prey reproduces"         : +1 prey
        - "Prey dies"               : -1 prey
        - "Predator dies"           : -1 predator
        - "Drought"                 : toggle sécheresse (ON/OFF)

    Fonctionnement:
        - Switch/if-elif sur la string reçue
        - Met à jour state en appliquant les deltas
        - Print un message “narratif” pour l’utilisateur
        - Print ensuite l’état courant (print_state)
    """
    if text == "ACK_INIT":
        print("[display] ✅ Simulation started (env acknowledged INIT).")
        print_state()
        return

    if text == "Predator eats Prey":
        if state["preys"] > 0:
            state["preys"] -= 1
        print("[display] 🐺🍖 Un prédateur a mangé une proie (-1 proie).")

    elif text == "Prey eats Grass":
        if state["grass"] > 0:
            state["grass"] -= 1
        print("[display] 🐰🌿 Une proie a mangé de l’herbe (-1 herbe).")

    elif text == "Grass grows":
        state["grass"] += 1
        print("[display] 🌱 L’herbe pousse (+1 herbe).")

    elif text == "Predator reproduces":
        state["predators"] += 1
        print("[display] 🐺➕ Un prédateur est né (+1 prédateur).")

    elif text == "Prey reproduces":
        state["preys"] += 1
        print("[display] 🐰➕ Une proie est née (+1 proie).")

    elif text == "Prey dies":
        if state["preys"] > 0:
            state["preys"] -= 1
        print("[display] ☠️🐰 Une proie est morte (-1 proie).")

    elif text == "Predator dies":
        if state["predators"] > 0:
            state["predators"] -= 1
        print("[display] ☠️🐺 Un prédateur est mort (-1 prédateur).")

    elif text == "Drought":
        state["drought"] = not state["drought"]
        if state["drought"]:
            print("[display] 🌵🔥 Sécheresse ! (drought ON)")
        else:
            print("[display] 🌧️✅ Fin de sécheresse (drought OFF)")

    else:
        print("[display] (unknown event)", text)

    print_state()


def main():
    """
    Point d’entrée du processus display : configuration utilisateur + boucle UI événementielle.

    Responsabilités:
        1) Créer/attacher la MessageQueue MQ_KEY.
        2) Récupérer les paramètres init (ici hardcodés, mais remplaçables par input()).
        3) Initialiser l’état local state avec ces valeurs (hypothèse “affichage”).
        4) Envoyer INIT à env sur TYPE_CMD.
        5) Lancer un thread mq_listener pour recevoir les événements TYPE_EVT en continu.
        6) Boucle principale:
           - consomme inbox (queue.Queue)
           - appelle handle_message(event) pour mettre à jour l’affichage / compteurs
    """
    mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)

    predators0 = 5
    preys0 = 20
    grass0 = 80

    state["predators"] = predators0
    state["preys"] = preys0
    state["grass"] = grass0
    state["drought"] = False

    send_init(mq, predators0, preys0, grass0)

    inbox = queue.Queue()
    threading.Thread(target=mq_listener, args=(mq, TYPE_EVT, inbox), daemon=True).start()

    print("[display] Listening for events...")
    print_state()

    while True:
        try:
            text = inbox.get(timeout=0.5)
        except queue.Empty:
            continue
        handle_message(text)


if __name__ == "__main__":
    main()
