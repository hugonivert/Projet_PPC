# display_gui.py
#
# GUI display/controller for the simulation.
# - Set initial populations and start the simulation (INIT)
# - Show live counts + drought status
# - Trigger drought manually
#
# Requires sysv_ipc (Linux/WSL).

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

MQ_KEY = 0x5678
TYPE_CMD = 1   # display -> env
TYPE_EVT = 2   # env -> display


class DisplayGUI:
    def __init__(self, root: tk.Tk) -> None:
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

        self.mq = sysv_ipc.MessageQueue(MQ_KEY, sysv_ipc.IPC_CREAT)
        self.inbox = queue.Queue()
        threading.Thread(target=self.mq_listener, daemon=True).start()

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self.process_inbox)

    def _build_ui(self) -> None:
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

        self.pred_lbl = ttk.Label(main, text="Predators: 0")
        self.prey_lbl = ttk.Label(main, text="Preys: 0")
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
        while True:
            try:
                text = self.inbox.get_nowait()
            except queue.Empty:
                break
            self.handle_message(text)

        self.root.after(100, self.process_inbox)

    def send_cmd(self, cmd: str) -> None:
        self.mq.send(cmd.encode("utf-8"), type=TYPE_CMD)

    def on_init(self) -> None:
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

        self.state["predators"] = predators
        self.state["preys"] = preys
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
        if not self.started:
            return
        desired = 0 if self.state["drought"] else 1
        self.send_cmd(f"SET DROUGHT {desired}")

    def refresh_labels(self) -> None:
        self.pred_lbl.config(text=f"Predators: {self.state['predators']}")
        self.prey_lbl.config(text=f"Preys: {self.state['preys']}")
        self.grass_lbl.config(text=f"Grass: {self.state['grass']}")
        self.drought_lbl.config(text=f"Drought: {'ON' if self.state['drought'] else 'OFF'}")

    @staticmethod
    def is_port_in_use(host: str, port: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                return s.connect_ex((host, port)) == 0
        except Exception:
            return False

    def log_event(self, text: str) -> None:
        self.log.config(state="normal")
        self.log.insert("end", text + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def handle_message(self, text: str) -> None:
        if text == "ACK_INIT":
            self.refresh_labels()
            return

        if text == "Predator eats Prey":
            if self.state["preys"] > 0:
                self.state["preys"] -= 1
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
            self.state["predators"] += 1
            self.log_event("Un predateur s'est reproduit.")

        elif text == "Prey reproduces":
            self.state["preys"] += 1
            self.log_event("Une proie s'est reproduite.")

        elif text == "Prey dies":
            if self.state["preys"] > 0:
                self.state["preys"] -= 1
            self.log_event("Une proie est morte.")

        elif text == "Predator dies":
            if self.state["predators"] > 0:
                self.state["predators"] -= 1
            self.log_event("Un predateur est mort.")

        elif text == "Drought":
            self.state["drought"] = not self.state["drought"]
            if self.state["drought"]:
                self.log_event("La secheresse commence.")
            else:
                self.log_event("La secheresse s'arrete.")

        self.refresh_labels()

    def on_stop(self) -> None:
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
        try:
            self.on_stop()
        finally:
            self.root.destroy()


def main() -> None:
    root = tk.Tk()
    DisplayGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
