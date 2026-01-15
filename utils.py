
import socket

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