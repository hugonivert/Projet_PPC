import sysv_ipc
import threading
import queue
import time

MQ_KEY = 0x5678
TYPE_CMD = 1   # env -> display
TYPE_EVT = 2   #display -> env
state = {
  "predators": 0,
  "preys": 0,
  "grass": 0,
  "drought": False
}


def mq_listener(mq: sysv_ipc.MessageQueue, ntype: int, inbox: queue.Queue):
    while True:
        try:
            msg, mtype = mq.receive(type=ntype)  # bloque
            text = msg.decode("utf-8", errors="replace")
            inbox.put(text)  # push thread-safe
            print("[env] MQ received:", text)
        except sysv_ipc.ExistentialError:
            # la MQ a été supprimée -> on stop le thread
            print("[env] MQ removed, listener stops")
            break
        except Exception as e:
            print("[env] MQ listener error:", e)
            time.sleep(0.1)

def main():
    ntype = 2  # exemple : messages entrants pour env
    mq = sysv_ipc.MessageQueue(MQ_KEY)

    inbox = queue.Queue()  # <- liste/buffer de messages reçus (thread-safe)

    mq_thread = threading.Thread(
        target=mq_listener,
        args=(mq, ntype, inbox),
        daemon=True
    )
    mq_thread.start()

    # boucle principale: traite les messages quand ils arrivent
    while True:
        try:
            text = inbox.get(timeout=0.5)  # bloque max 0.5s
        except queue.Empty:
            # rien à traiter, tu peux faire autre chose (simulation, etc.)
            continue

        # ici TU traites la commande
        handle_message(text)
        mq.send(b"INIT 5 20 80", type=1) #initialiser env


def handle_message(text: str):
    print("[env] MAIN handles:", text)
    if text == "STATUS":
        # ...
        pass
    elif text == "PAUSE":
        # ...
        pass
    elif text == "QUIT":
        # ...
        pass

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()

