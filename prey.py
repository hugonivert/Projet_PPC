import socket
import time
import random
import utils

#Pour la connexion au socket de env
HOST = "localhost"
PORT = 6666

# "Biologie" de la proie
ENERGY_START = 20
ENERGY_DECAY = 1

HUNGER_THRESHOLD = 25   # seuil à partir duquel une proie manger
EAT_GAIN = 5              # énergie gagnée si herbe mangée

REPRO_THRESHOLD = 21      # seuil à partir duquel un predator peut tenter de se reproduire
REPRO_PROB = 0.6         # probabilité de créer un nouveau prédateur après un accouplement 
REPRO_COST = 8          # coût d'énergie d'une reproduction

def main():
    print("[prey] starting")
    cmd_tag = "prey"

    energy = ENERGY_START
    tick = 0
    shared_mem = None
    mutex = None

    # Connexion au socket de env
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10.0)
        s.connect((HOST, PORT))
        buffer = bytearray()

        # JOIN
        resp = utils.send_cmd_persistent(s, buffer, f"JOIN prey")
        if resp != "OK":
            print(f"[prey] JOIN failed -> {resp}")
            return
        print("[prey] JOIN OK")

        # Attacher la SHM pour lire l'état global
        try:
            shared_mem, mutex = utils.attach_world() #attache env grâce à un SHM qui sera protégée par un mutex
        except Exception as e:
            print(f"[prey] SHM attach failed -> {e}")
            return

        # Boucle de vie (calée sur le tick global)
        st = utils.read_state(shared_mem, mutex)
        last_tick = st[utils.TICK]
        while True:
            time.sleep(0.2)
            st = utils.read_state(shared_mem, mutex)
            if st[utils.TICK] == last_tick:
                continue
            delta = st[utils.TICK] - last_tick #nombre de ticks qui se sont écoulés depuis la dernière boucle de ce processus
            last_tick = st[utils.TICK]
            if delta < 0 or (delta == 0 and last_tick > 0):
                print("[prey] env restarted, exiting")
                break
            if delta <= 0:
                continue
            tick += delta #comptage tick de l'individu 
            if st[utils.PREY] <= 0: #vérification que le process en cours n'est pas un process fantome
                print("[prey] no prey left in world, exiting")
                break

            # l'énergie diminue à chaque tick
            energy -= ENERGY_DECAY * delta

            # mort : prévenir env + quitter
            if energy < 0:
                try:
                    resp = utils.send_cmd_persistent(s, buffer, f"DIE prey")
                    print(f"[prey] DIE -> env says: {resp}")
                except Exception as e:
                    print(f"[prey] DIE failed (env?) -> {e}")
                break

            # si faim : tenter de manger
            if energy < HUNGER_THRESHOLD:
                hunger_ratio = (HUNGER_THRESHOLD - energy) / HUNGER_THRESHOLD #plus l'énergie baisse plus l'individu à faim
                hunger_ratio = max(0.0, min(1.0, hunger_ratio)) #gérer le cas où energy > HUNGER_THRESHOLD
                eat_prob = hunger_ratio ** 1.25 #faim non linéaire 
                if st[utils.GRASS] > 0 and random.random() < eat_prob:
                    resp = utils.send_cmd_persistent(s, buffer, f"EAT_GRASS {cmd_tag}")
                    if resp == "OK":
                        energy += EAT_GAIN
                        print(f"[prey] ate grass (+{EAT_GAIN}) -> energy={energy}")
                    else:
                        print(f"[prey] could not eat grass -> {resp}")

            # reproduction
            if energy > REPRO_THRESHOLD and random.random() < REPRO_PROB:
                resp = utils.send_cmd_persistent(s, buffer, f"REPRODUCE prey")
                print(f"[prey] reproduce attempt -> {resp}")
                energy -= REPRO_COST

    if shared_mem is not None:
        try:
            shared_mem.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
