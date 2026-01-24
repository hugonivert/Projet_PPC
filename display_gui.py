import queue
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import sys
import os
import socket
import signal

import sysv_ipc
import cleanup

MQ_KEY = 0x5678
TYPE_CMD = 1   # display -> env
TYPE_EVT = 2   # env -> display


class DisplayGUI:
    def __init__(self, root: tk.Tk) -> None:
        """Initialise l'interface, l'IPC et les tâches périodiques."""
        self.root = root
        self.root.title("Circle of Life - Display")

        self.state = {
            "predators": 0,
            "preys": 0,
            "grass": 0,
            "drought": False,
        }
        self.started = False
        self.env_proc = None
        
        # cleanup pour s'assurer que tout est bien clean même si erreur à la fin du précédant lancement
        cleanup.cleanup_ipc("WORLD", 0x1234, MQ_KEY, remove_mq=True)
        self.mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)
        self.inbox = queue.Queue()
        threading.Thread(target=self.mq_listener, daemon=True).start()

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self.process_inbox)
        self.root.after(1000, self.request_status)

    def _build_ui(self) -> None:
        """Construit et place les widgets Tkinter."""
        main = ttk.Frame(self.root, padding=12)
        main.grid(row=0, column=0, sticky="nsew")

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)

        ttk.Label(main, text="Initial parameters").grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Label(main, text="Predators").grid(row=1, column=0, sticky="w")
        ttk.Label(main, text="Preys").grid(row=2, column=0, sticky="w")
        ttk.Label(main, text="Grass").grid(row=3, column=0, sticky="w")

        self.pred_var = tk.StringVar(value="5")
        self.prey_var = tk.StringVar(value="20")
        self.grass_var = tk.StringVar(value="80")

        ttk.Entry(main, textvariable=self.pred_var, width=10).grid(row=1, column=1, sticky="w")
        ttk.Entry(main, textvariable=self.prey_var, width=10).grid(row=2, column=1, sticky="w")
        ttk.Entry(main, textvariable=self.grass_var, width=10).grid(row=3, column=1, sticky="w")

        self.init_btn = ttk.Button(main, text="Init", command=self.on_init)
        self.init_btn.grid(row=4, column=0, columnspan=2, sticky="we", pady=(6, 12))

        ttk.Label(main, text="Live state").grid(row=5, column=0, columnspan=2, sticky="w")

        self.pred_lbl = ttk.Label(main, text="Predators (effective): 0")
        self.prey_lbl = ttk.Label(main, text="Preys (effective): 0")
        self.grass_lbl = ttk.Label(main, text="Grass: 0")
        self.drought_lbl = ttk.Label(main, text="Drought: OFF")

        self.pred_lbl.grid(row=6, column=0, columnspan=2, sticky="w")
        self.prey_lbl.grid(row=7, column=0, columnspan=2, sticky="w")
        self.grass_lbl.grid(row=8, column=0, columnspan=2, sticky="w")
        self.drought_lbl.grid(row=9, column=0, columnspan=2, sticky="w")

        self.drought_btn = ttk.Button(main, text="Toggle drought", command=self.on_toggle_drought, state="disabled")
        self.drought_btn.grid(row=10, column=0, columnspan=2, sticky="we", pady=(10, 0))

        self.stop_btn = ttk.Button(main, text="Stop simulation", command=self.on_stop, state="disabled")
        self.stop_btn.grid(row=11, column=0, columnspan=2, sticky="we", pady=(6, 0))

        ttk.Label(main, text="Events").grid(row=12, column=0, columnspan=2, sticky="w", pady=(10, 0))
        self.log = tk.Text(main, height=10, width=40, state="disabled")
        self.log.grid(row=13, column=0, columnspan=2, sticky="nsew")
        main.rowconfigure(13, weight=1)

    def mq_listener(self) -> None:
        """Boucle en arrière-plan qui reçoit les événements de la MQ."""
        while True:
            try:
                msg, _ = self.mq.receive(type=TYPE_EVT)
                text = msg.decode("utf-8", errors="replace").strip()
                self.inbox.put(text)
            except sysv_ipc.ExistentialError:
                break
            except Exception:
                pass

    def process_inbox(self) -> None:
        """Traite les événements en file et reprogramme le prochain poll."""
        while True:
            try:
                text = self.inbox.get_nowait()
            except queue.Empty:
                break
            self.handle_message(text)

        self.root.after(100, self.process_inbox)

    def request_status(self) -> None:
        """Demande périodiquement l'état à env."""
        if self.started:
            try:
                self.send_cmd("STATUS")
            except Exception:
                pass
        self.root.after(1000, self.request_status)

    def send_cmd(self, cmd: str) -> None:
        """Envoie une commande à env via la MQ."""
        self.mq.send(cmd.encode("utf-8"), type=TYPE_CMD)

    def on_init(self) -> None:
        """Valide les entrées, démarre env si besoin, et envoie INIT."""
        try:
            predators = int(self.pred_var.get())
            preys = int(self.prey_var.get())
            grass = int(self.grass_var.get())
        except ValueError:
            messagebox.showerror("Error", "Please enter valid integers.")
            return

        if predators < 0 or preys < 0 or grass < 0:
            messagebox.showerror("Error", "Values must be >= 0.")
            return

        self.state["predators"] = 0
        self.state["preys"] = 0
        self.state["grass"] = grass
        self.state["drought"] = False
        self.refresh_labels()

        if self.env_proc is None or self.env_proc.poll() is not None:
            if self.is_port_in_use("localhost", 6666):
                messagebox.showerror(
                    "Error",
                    "Le port 6666 est deja utilise. Arrete l'ancien env.py puis relance.",
                )
                return
            self.env_proc = subprocess.Popen(
                [sys.executable, "env.py"],
                stdout=None,
                stderr=None,
                start_new_session=True,
            )

        self.send_cmd(f"INIT {predators} {preys} {grass}")
        self.started = True
        self.drought_btn.config(state="normal")
        self.stop_btn.config(state="normal")

    def on_toggle_drought(self) -> None:
        """Bascule l'état de sécheresse via une commande env."""
        if not self.started:
            return
        desired = 0 if self.state["drought"] else 1
        self.send_cmd(f"SET DROUGHT {desired}")

    def refresh_labels(self) -> None:
        """Rafraîchit les labels à partir de l'état courant."""
        self.pred_lbl.config(text=f"Predators (effective): {self.state['predators']}")
        self.prey_lbl.config(text=f"Preys (effective): {self.state['preys']}")
        self.grass_lbl.config(text=f"Grass: {self.state['grass']}")
        self.drought_lbl.config(text=f"Drought: {'ON' if self.state['drought'] else 'OFF'}")

    @staticmethod
    def is_port_in_use(host: str, port: int) -> bool:
        """Retourne True si un port TCP est déjà utilisé."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                return s.connect_ex((host, port)) == 0
        except Exception:
            return False

    def log_event(self, text: str) -> None:
        """Ajoute une ligne au journal d'événements."""
        self.log.config(state="normal")
        self.log.insert("end", text + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def handle_message(self, text: str) -> None:
        """Traite un message d'événement venant de env."""
        if text.startswith("STATE "):
            parts = text.split() #redécoupage du message
            data = {}
            for item in parts[1:]:
                if "=" in item:
                    k, v = item.split("=", 1)
                    data[k] = v
            try:
                pred_val = data.get("joined_pred", data.get("pred", self.state["predators"]))
                prey_val = data.get("joined_prey", data.get("prey", self.state["preys"]))
                self.state["predators"] = int(pred_val)
                self.state["preys"] = int(prey_val)
                self.state["grass"] = int(data.get("grass", self.state["grass"]))
                self.state["drought"] = int(data.get("drought", 0)) == 1
            except ValueError:
                pass
            self.refresh_labels()
            return

        if text == "ACK_INIT":
            self.refresh_labels()
            return

        if text == "Predator eats Prey":
            self.log_event("Un predateur a mange une proie.")

        elif text == "Prey eats Grass":
            if self.state["grass"] > 0:
                self.state["grass"] -= 1
            self.log_event("Une proie a mange de l'herbe.")

        elif text == "Grass grows":
            self.state["grass"] += 1

        elif text == "Grass withers":
            if self.state["grass"] > 0:
                self.state["grass"] -= 1

        elif text == "Predator reproduces":
            self.log_event("Un predateur s'est reproduit.")

        elif text == "Prey reproduces":
            self.log_event("Une proie s'est reproduite.")

        elif text == "Prey dies":
            self.log_event("Une proie est morte.")

        elif text == "Predator dies":
            self.log_event("Un predateur est mort.")

        elif text == "Drought":
            self.state["drought"] = not self.state["drought"]
            if self.state["drought"]:
                self.log_event("La secheresse commence.")
            else:
                self.log_event("La secheresse s'arrete.")
        elif text == "Prey escapes":
            self.log_event("La proie a fait break le prédateur, PICORE MAINTENANT")
        elif text.startswith("SIM_STOP"):
            self.log_event("Simulation arretee pour preserver le PC (trop de proies).")
            messagebox.showinfo("Simulation stoppee", "Simulation arretee pour preserver le PC (trop de proies).")
            self.started = False
            self.drought_btn.config(state="disabled")
            self.stop_btn.config(state="disabled")

        self.refresh_labels()

    def on_stop(self) -> None:
        """Arrête la simulation et tente de terminer env."""
        if not self.started:
            return
        try:
            self.send_cmd("QUIT")
        except Exception:
            pass
        if self.env_proc is not None and self.env_proc.poll() is None:
            try:
                self.env_proc.wait(timeout=2)
            except Exception:
                try:
                    os.killpg(self.env_proc.pid, signal.SIGTERM)
                except Exception:
                    pass
                try:
                    self.env_proc.wait(timeout=2)
                except Exception:
                    try:
                        os.killpg(self.env_proc.pid, signal.SIGKILL)
                    except Exception:
                        pass
        self.started = False
        self.drought_btn.config(state="disabled")
        self.stop_btn.config(state="disabled")

    def on_close(self) -> None:
        """Gère la fermeture en stoppant puis détruisant l'UI."""
        try:
            self.on_stop()
        finally:
            self.root.destroy()


def main() -> None:
    """Point d'entrée du programme pour l'interface d'affichage."""
    root = tk.Tk()
    DisplayGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
