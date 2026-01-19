import socket
import time
import os
import random
import utils

HOST = "localhost"
PORT = 6666

# Simulation
TICK_SECONDS = 1.0

# "Biologie" du prédateur
ENERGY_START = 30
ENERGY_DECAY = 1

HUNGER_THRESHOLD = 15     # si energy < 10 => predateur cherche à manger
EAT_GAIN = 8              # énergie gagnée si proie mangée

REPRO_THRESHOLD = 35     # si energy > 25 => predateur peut tenter reproduction
REPRO_PROB = 0.30         # 30% de chance par tick au-dessus du seuil


def main():
    pred_id = f"pred-{os.getpid()}-{random.randint(1000, 9999)}"
    print(f"[pred] starting id={pred_id}")

    energy = ENERGY_START
    tick = 0

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

        # 2) Boucle de vie
        while True:
            tick += 1
            time.sleep(TICK_SECONDS)

            # A) l'énergie diminue à chaque tick
            energy -= ENERGY_DECAY

            # B) log
            print(f"[pred {pred_id}] tick={tick} energy={energy}")

            # C) mort : prévenir env + quitter
            if energy < 0:
                try:
                    resp = utils.send_cmd_persistent(s, buffer, f"DIE pred {pred_id}")
                    print(f"[pred {pred_id}] DIE -> env says: {resp}")
                except Exception as e:
                    print(f"[pred {pred_id}] DIE failed (env?) -> {e}")
                break

            # D) si faim : tenter de manger
            if energy < HUNGER_THRESHOLD:
                resp = utils.send_cmd_persistent(s, buffer, f"EAT_PREY {pred_id}")
                if resp == "OK":
                    energy += EAT_GAIN
                    print(f"[pred {pred_id}] ate prey (+{EAT_GAIN}) -> energy={energy}")
                else:
                    print(f"[pred {pred_id}] could not eat prey -> {resp}")

            # E) reproduction (optionnel)
            if energy > REPRO_THRESHOLD and random.random() < REPRO_PROB:
                resp = utils.send_cmd_persistent(s, buffer, f"REPRODUCE pred {pred_id}")
                print(f"[pred {pred_id}] reproduce attempt -> {resp}")


if __name__ == "__main__":
    main()
