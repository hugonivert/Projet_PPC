import socket
import time
import os
import random

HOST = "localhost"
PORT = 6666

# Simulation
TICK_SECONDS = 1.0

# "Biologie" de la proie
ENERGY_START = 20
ENERGY_DECAY = 1

HUNGER_THRESHOLD = 10     # si energy < 10 => proie cherche à manger
EAT_GAIN = 8              # énergie gagnée si herbe mangée

REPRO_THRESHOLD = 25      # si energy > 25 => proie peut tenter reproduction
REPRO_PROB = 0.30         # 30% de chance par tick au-dessus du seuil


def recv_line(sock: socket.socket, buffer: bytearray) -> str | None:
    """
    Lit une ligne (terminée par '\n') depuis un socket TCP.
    Retourne la ligne sans '\n', ou None si la connexion est fermée.
    """
    while True:
        if b"\n" in buffer:
            line, _, rest = buffer.partition(b"\n")
            buffer[:] = rest
            return line.decode(errors="replace").strip()

        chunk = sock.recv(1024)
        if not chunk:
            return None
        buffer.extend(chunk)


def send_line(sock: socket.socket, line: str) -> None:
    """Envoie une commande terminée par '\\n'."""
    sock.sendall((line.strip() + "\n").encode())


def send_cmd_persistent(sock: socket.socket, buffer: bytearray, cmd: str) -> str:
    """
    Envoie une commande sur une connexion persistante et lit la réponse (1 ligne).
    """
    send_line(sock, cmd)
    resp = recv_line(sock, buffer)
    if resp is None:
        raise ConnectionError("env closed the connection unexpectedly")
    return resp


def main():
    prey_id = f"prey-{os.getpid()}-{random.randint(1000, 9999)}"
    print(f"[prey] starting id={prey_id}")

    energy = ENERGY_START
    tick = 0

    # Connexion persistante : 1 socket pour toute la vie de la proie
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect((HOST, PORT))
        buffer = bytearray()

        # 1) JOIN
        resp = send_cmd_persistent(s, buffer, f"JOIN prey")
        if resp != "OK":
            print(f"[prey {prey_id}] JOIN failed -> {resp}")
            return
        print(f"[prey {prey_id}] JOIN OK")

        # 2) Boucle de vie
        while True:
            tick += 1
            time.sleep(TICK_SECONDS)

            # A) l'énergie diminue à chaque tick
            energy -= ENERGY_DECAY

            # B) log
            print(f"[prey {prey_id}] tick={tick} energy={energy}")

            # C) mort : prévenir env + quitter
            if energy < 0:
                try:
                    resp = send_cmd_persistent(s, buffer, f"DIE prey {prey_id}")
                    print(f"[prey {prey_id}] DIE -> env says: {resp}")
                except Exception as e:
                    print(f"[prey {prey_id}] DIE failed (env?) -> {e}")
                break

            # D) si faim : tenter de manger
            if energy < HUNGER_THRESHOLD:
                resp = send_cmd_persistent(s, buffer, f"EAT_GRASS {prey_id}")
                if resp == "OK":
                    energy += EAT_GAIN
                    print(f"[prey {prey_id}] ate grass (+{EAT_GAIN}) -> energy={energy}")
                else:
                    print(f"[prey {prey_id}] could not eat grass -> {resp}")

            # E) reproduction (optionnel)
            if energy > REPRO_THRESHOLD and random.random() < REPRO_PROB:
                resp = send_cmd_persistent(s, buffer, f"REPRODUCE prey {prey_id}")
                print(f"[prey {prey_id}] reproduce attempt -> {resp}")


if __name__ == "__main__":
    main()
