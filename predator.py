import socket
import time
import os
import random
import utils

HOST = "localhost"
PORT = 6666

# Simulation
TICK_SECONDS = 3

# "Biologie" du prédateur
ENERGY_START = 30
ENERGY_DECAY = 1

HUNGER_THRESHOLD = 25     # si energy < 10 => predateur cherche à manger
HUNT_COST = 2             # coût d'energie quand le predateur tente de manger
EAT_GAIN = 20             # énergie gagnée si proie mangée

REPRO_THRESHOLD = 30     # si energy > 25 => predateur peut tenter reproduction
REPRO_PROB = 1         # 30% de chance par tick au-dessus du seuil
REPRO_COST = 10

def main():
    pred_id = f"pred-{os.getpid()}-{random.randint(1000, 9999)}"
    print(f"[pred] starting id={pred_id}")

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
        resp = utils.send_cmd_persistent(s, buffer, f"JOIN predator")
        if resp != "OK":
            print(f"[predator {pred_id}] JOIN failed -> {resp}")
            return
        print(f"[pred {pred_id}] JOIN OK")

        # 1bis) attacher la SHM pour lire l'Actat global
        try:
            shared_mem, mutex = utils.attach_world()
        except Exception as e:
            print(f"[pred {pred_id}] SHM attach failed -> {e}")
            return

        # 2) Boucle de vie (calée sur le tick global)
        st = utils.read_state(shared_mem, mutex)
        last_tick = st[utils.TICK]
        while True:
            time.sleep(0.2)
            st = utils.read_state(shared_mem, mutex)
            if st[utils.TICK] == last_tick:
                continue
            delta = st[utils.TICK] - last_tick
            last_tick = st[utils.TICK]
            if delta < 0 or (delta == 0 and last_tick > 0):
                print(f"[pred {pred_id}] env restarted, exiting")
                break
            if delta <= 0:
                continue
            tick += delta
            if st[utils.PRED] <= 0:
                print(f"[pred {pred_id}] no predators left in world, exiting")
                break

            # A) l'énergie diminue à chaque tick
            energy -= ENERGY_DECAY * delta

            # B) log
            #print(f"[pred {pred_id}] tick={tick} energy={energy}")

            # C) mort : prévenir env + quitter
            if energy < 0:
                try:
                    resp = utils.send_cmd_persistent(s, buffer, f"DIE pred {pred_id}")
                    #print(f"[pred {pred_id}] DIE -> env says: {resp}")
                except Exception as e:
                    print(f"[pred {pred_id}] DIE failed (env?) -> {e}")
                break

            # D) si faim : tenter de manger
            if energy < HUNGER_THRESHOLD:
                hunger_ratio = (HUNGER_THRESHOLD - energy) / HUNGER_THRESHOLD
                hunger_ratio = max(0.0, min(1.0, hunger_ratio))
                eat_prob = hunger_ratio ** 1.5
                if st[utils.PREY] > 0 and random.random() < eat_prob:
                    energy -= HUNT_COST
                    resp = utils.send_cmd_persistent(s, buffer, f"EAT_PREY {pred_id}")
                    if resp == "OK":
                        energy += EAT_GAIN
                        print(f"[pred {pred_id}] ate prey (+{EAT_GAIN}) -> energy={energy}")
                    elif resp == "ESCAPE":
                        print(f"[pred {pred_id}] prey escaped -> energy={energy}")
                    else:
                        print(f"[pred {pred_id}] could not eat prey -> {resp}")

            # E) reproduction (optionnel)
            if energy > REPRO_THRESHOLD and random.random() < REPRO_PROB:
                resp = utils.send_cmd_persistent(s, buffer, f"REPRODUCE pred {pred_id}")
                print(f"[pred {pred_id}] reproduce attempt -> {resp}")
                energy -= REPRO_COST

    if shared_mem is not None:
        try:
            shared_mem.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
