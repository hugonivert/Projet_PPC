import socket
import time
import random
import utils

#Pour la connexion au socket de env
HOST = "localhost"
PORT = 6666

# "Biologie" du prédateur
ENERGY_START = 30 
ENERGY_DECAY = 1 

HUNGER_THRESHOLD = 30     # seuil à partir duquel un predator peut tenter de manger
HUNT_COST = 2             # coût d'énergie quand le predateur tente de manger
EAT_GAIN = 10             # énergie gagnée si proie mangée

REPRO_THRESHOLD = 30     # seuil à partir duquel un predator peut tenter de se reproduire
REPRO_PROB = 0.8         # probabilité de créer un nouveau prédateur après un accouplement 
REPRO_COST = 10         # coût d'énergie d'une reproduction

def main():
    print("[pred] starting")
    cmd_tag = "pred"

    energy = ENERGY_START
    tick = 0
    shared_mem = None
    mutex = None

    # Connexion au socket de env
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10.0) # Timeout après 10.0s si connexion bloquante
        s.connect((HOST, PORT))
        buffer = bytearray() # Pour la reconstitution des messages reçu du socket

        # JOIN
        resp = utils.send_cmd_persistent(s, buffer, f"JOIN predator")
        if resp != "OK":
            print(f"[predator] JOIN failed -> {resp}")
            return
        print("[pred] JOIN OK")

        # Attacher la SHM pour lire l'état global
        try:
            shared_mem, mutex = utils.attach_world() #attache env grâce à un SHM qui sera protégée par un mutex
        except Exception as e:
            print(f"[pred] SHM attach failed -> {e}")
            return

        # Boucle de vie (calée sur le tick global)
        st = utils.read_state(shared_mem, mutex) #lecture protégée de la SHM
        last_tick = st[utils.TICK]
        while True:
            time.sleep(0.2)
            st = utils.read_state(shared_mem, mutex)
            if st[utils.TICK] == last_tick:
                continue
            delta = st[utils.TICK] - last_tick #nombre de ticks qui se sont écoulés depuis la dernière boucle de ce processus
            last_tick = st[utils.TICK] 
            if delta < 0 or (delta == 0 and last_tick > 0):
                print("[pred] env restarted, exiting")
                break
            if delta <= 0:
                continue
            tick += delta #comptage tick de l'individu 
            if st[utils.PRED] <= 0: #vérification que le process en cours n'est pas un process fantome
                print("[pred] no predators left in world, exiting")
                break

            # l'énergie diminue à chaque tick
            energy -= ENERGY_DECAY * delta

            # mort : prévenir env + quitter
            if energy < 0:
                try:
                    resp = utils.send_cmd_persistent(s, buffer, "DIE predator")
                except Exception as e:
                    print(f"[pred] DIE failed (env?) -> {e}")
                break

            # si faim : tenter de manger
            if energy < HUNGER_THRESHOLD:
                hunger_ratio = (HUNGER_THRESHOLD - energy) / HUNGER_THRESHOLD #plus l'énergie baisse plus l'individu à faim
                hunger_ratio = max(0.0, min(1.0, hunger_ratio)) #gérer le cas où energy > HUNGER_THRESHOLD
                eat_prob = hunger_ratio ** 1.4 #faim non linéaire 
                if st[utils.PREY] > 0 and random.random() < eat_prob: 
                    energy -= HUNT_COST #coût de chasse
                    resp = utils.send_cmd_persistent(s, buffer, f"EAT_PREY {cmd_tag}")
                    if resp == "OK": #chasse réussie
                        energy += EAT_GAIN
                        print(f"[pred] ate prey (+{EAT_GAIN}) -> energy={energy}")
                    elif resp == "ESCAPE": #chasse loupée
                        print(f"[pred] prey escaped -> energy={energy}")
                    else:
                        print(f"[pred] could not eat prey -> {resp}")

            # reproduction
            if energy > REPRO_THRESHOLD and random.random() < REPRO_PROB:
                resp = utils.send_cmd_persistent(s, buffer, f"REPRODUCE pred")
                print(f"[pred] reproduce attempt -> {resp}")
                energy -= REPRO_COST #coût de reproduction

    if shared_mem is not None:
        try:
            shared_mem.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
