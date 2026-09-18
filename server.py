import socket
import threading
import sys

# username -> data socket, so pushes (broadcast/private/join/leave) can
# reach a client without going through their control connection.
clients = {}
clients_lock = threading.Lock()

next_data_port = None
next_data_port_lock = threading.Lock()


def allocate_data_port():
    global next_data_port
    with next_data_port_lock:
        port = next_data_port
        next_data_port += 1
    return port


def make_response(status, data_lines=None):
    text = f"{status}\n\n"
    if data_lines:
        text += "\n".join(data_lines) + "\n"
    return text.encode("utf-8")


def send_to(conn, status, data_lines=None):
    try:
        conn.sendall(make_response(status, data_lines))
    except OSError:
        pass


def broadcast_notice(kind, username, exclude=None):
    with clients_lock:
        targets = [conn for uname, conn in clients.items() if uname != exclude]
    payload = make_response(200, [kind, username])
    for conn in targets:
        try:
            conn.sendall(payload)
        except OSError:
            pass


def do_login(username, data_conn, current_username):
    print(f"Login requested by: {username}")
    if not username or current_username is not None:
        send_to(data_conn, 500)
        return current_username
    with clients_lock:
        if username in clients:
            send_to(data_conn, 500)
            return current_username
        clients[username] = data_conn
    send_to(data_conn, 200)
    broadcast_notice("join", username, exclude=username)
    return username


def do_who(data_conn):
    print("Who requested. Sending users.")
    with clients_lock:
        names = list(clients.keys())
    send_to(data_conn, 200, [", ".join(names)])


def do_broadcast(username, message, data_conn):
    if not username:
        send_to(data_conn, 500)
        return
    message = message.strip()
    print(f"Broadcast requested by {username}")
    print(f"Message: {message}")
    with clients_lock:
        targets = list(clients.values())
    payload = make_response(200, ["Broadcast", username, message])
    for conn in targets:
        try:
            conn.sendall(payload)
        except OSError:
            pass


def do_private(username, rest, data_conn):
    if not username:
        send_to(data_conn, 500)
        return
    target_name, _, message = rest.strip().partition(" ")
    print(f"Private message from {username} to {target_name}")
    with clients_lock:
        target_conn = clients.get(target_name)
    if target_conn is None:
        send_to(data_conn, 500)
        return
    try:
        target_conn.sendall(make_response(200, ["Private", username, message]))
    except OSError:
        pass
    send_to(data_conn, 200)


def handle_client(control_conn, addr):
    print("Connection requested. Creating data socket")

    data_port = allocate_data_port()
    data_listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    data_listen_sock.bind(('', data_port))
    data_listen_sock.listen(1)

    control_conn.sendall(make_response(200, [str(data_port)]))

    data_conn, _ = data_listen_sock.accept()
    data_listen_sock.close()

    username = None
    buffer = ""

    try:
        while True:
            chunk = control_conn.recv(1024)
            if not chunk:
                break
            buffer += chunk.decode("utf-8")

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                cmd, _, rest = line.partition(" ")
                cmd = cmd.lower()

                if cmd == "login":
                    username = do_login(rest.strip(), data_conn, username)
                elif cmd == "who":
                    do_who(data_conn)
                elif cmd == "broadcast":
                    do_broadcast(username, rest, data_conn)
                elif cmd == "private":
                    do_private(username, rest, data_conn)
                elif cmd == "quit":
                    print(f"Quit requested by {username}")
                    send_to(data_conn, 200)
                    return
                else:
                    send_to(data_conn, 500)
    except (ConnectionError, OSError):
        pass
    finally:
        if username:
            with clients_lock:
                clients.pop(username, None)
            broadcast_notice("leave", username)
        control_conn.close()
        data_conn.close()


def main():
    global next_data_port

    if len(sys.argv) != 2:
        print("Usage: python server.py <control_port>")
        sys.exit(1)

    control_port = int(sys.argv[1])
    next_data_port = control_port + 1000

    print("Starting server...")
    control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    control_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("Creating server socket")
    control_sock.bind(('', control_port))
    control_sock.listen(5)
    print("Awaiting connections...")

    try:
        while True:
            client_conn, addr = control_sock.accept()
            t = threading.Thread(target=handle_client, args=(client_conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        control_sock.close()


if __name__ == "__main__":
    main()
