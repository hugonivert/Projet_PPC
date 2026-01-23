# cleanup.py
from multiprocessing import shared_memory
import sysv_ipc


def cleanup_ipc(shm_name: str, sema_key: int, mq_key: int, remove_mq: bool = True) -> None:
    try:
        old = shared_memory.SharedMemory(name=shm_name, create=False)
        old.close()
        old.unlink()
        print("[cleanup] stale shared memory removed")
    except FileNotFoundError:
        pass
    except Exception as e:
        print("[cleanup] shared memory cleanup error:", e)

    try:
        sema = sysv_ipc.Semaphore(sema_key)
        sema.remove()
        print("[cleanup] stale semaphore removed")
    except sysv_ipc.ExistentialError:
        pass
    except Exception as e:
        print("[cleanup] semaphore cleanup error:", e)

    if remove_mq:
        try:
            mq = sysv_ipc.MessageQueue(mq_key)
            mq.remove()
            print("[cleanup] stale message queue removed")
        except sysv_ipc.ExistentialError:
            pass
        except Exception as e:
            print("[cleanup] message queue cleanup error:", e)
