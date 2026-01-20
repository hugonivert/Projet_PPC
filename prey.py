import socket
import time
import os
import random
import utils

HOST = "localhost"
PORT = 6666

# Simulation
TICK_SECONDS = 3

# "Biologie" de la proie
ENERGY_START = 20
ENERGY_DECAY = 1

HUNGER_THRESHOLD = 15     # si energy < 10 => proie cherche à manger
EAT_GAIN = 10              # énergie gagnée si herbe mangée

REPRO_THRESHOLD = 20      # si energy > 25 => proie peut tenter reproduction
REPRO_PROB = 0.6         # 30% de chance par tick au-dessus du seuil

def main():
    prey_id = f"prey-{os.getpid()}-{random.randint(1000, 9999)}"
    print(f"[prey] starting id={prey_id}")

    energy = ENERGY_START
    tick = 0
    shared_mem = None
    mutex = None

    # Connexion persistante : 1 socket pour toute la vie de la proie
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect((HOST, PORT))
        buffer = bytearray()

        # 1) JOIN
        resp = utils.send_cmd_persistent(s, buffer, f"JOIN prey")
        if resp != "OK":
            print(f"[prey {prey_id}] JOIN failed -> {resp}")
            return
        print(f"[prey {prey_id}] JOIN OK")

        # 1bis) attacher la SHM pour lire l'Actat global
        try:
            shared_mem, mutex = utils.attach_world()
        except Exception as e:
            print(f"[prey {prey_id}] SHM attach failed -> {e}")
            return

        # 2) Boucle de vie
        while True:
            tick += 1
            time.sleep(TICK_SECONDS)

            # A) l'énergie diminue à chaque tick
            energy -= ENERGY_DECAY

            # B) log
            #print(f"[prey {prey_id}] tick={tick} energy={energy}")

            # C) mort : prévenir env + quitter
            if energy < 0:
                try:
                    resp = utils.send_cmd_persistent(s, buffer, f"DIE prey {prey_id}")
                    print(f"[prey {prey_id}] DIE -> env says: {resp}")
                except Exception as e:
                    print(f"[prey {prey_id}] DIE failed (env?) -> {e}")
                break

            # D) si faim : tenter de manger
            if energy < HUNGER_THRESHOLD:
                st = utils.read_state(shared_mem, mutex)
                if st[utils.GRASS] > 0:
                    resp = utils.send_cmd_persistent(s, buffer, f"EAT_GRASS {prey_id}")
                    if resp == "OK":
                        energy += EAT_GAIN
                        print(f"[prey {prey_id}] ate grass (+{EAT_GAIN}) -> energy={energy}")
                    else:
                        print(f"[prey {prey_id}] could not eat grass -> {resp}")

            # E) reproduction (optionnel)
            if energy > REPRO_THRESHOLD and random.random() < REPRO_PROB:
                resp = utils.send_cmd_persistent(s, buffer, f"REPRODUCE prey {prey_id}")
                print(f"[prey {prey_id}] reproduce attempt -> {resp}")

    if shared_mem is not None:
        try:
            shared_mem.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
