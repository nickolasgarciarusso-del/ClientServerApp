import socket
import threading
import sys

# Registry of connected clients, keyed by username.
# Populated once login is implemented. Using a dict (not a list/array)
# because lookups by username (for 'private') and membership checks
# (for login uniqueness) both need to be O(1), not a linear scan.
clients = {}
clients_lock = threading.Lock()  # protects `clients` since multiple client threads touch it

next_data_port = None  # set at startup based on control port
next_data_port_lock = threading.Lock()


def allocate_data_port():
    """Hand out a unique data port for each new client connection."""
    global next_data_port
    with next_data_port_lock:
        port = next_data_port
        next_data_port += 1
    return port


def handle_client(control_conn, addr):
    print("Connection requested. Creating data socket")

    # Allocate and open a dedicated data socket/port for this client
    data_port = allocate_data_port()
    data_listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    data_listen_sock.bind(('', data_port))
    data_listen_sock.listen(1)

    # Respond to the connect command over the CONTROL socket
    # (data socket isn't established yet, so this can't go over it)
    response = f"200\n{data_port}\n"
    control_conn.send(response.encode('utf-8'))

    # Client is expected to open a second connection to the data port
    # we just advertised. Block here until they do.
    data_conn, data_addr = data_listen_sock.accept()

    # --- Beyond this point: login/who/broadcast/private/quit go here ---
    # Not implemented yet. This is intentionally left as a stub so the
    # connect handshake can be tested in isolation first.
    #
    # buffer = ""
    # while True:
    #     chunk = control_conn.recv(1024)
    #     if not chunk:
    #         break
    #     buffer += chunk.decode('utf-8')
    #     # split buffer on newline, parse command, dispatch, respond...

    # Cleanup for this connection (currently only reached if the client
    # closes without ever sending a further command)
    control_conn.close()
    data_conn.close()
    data_listen_sock.close()


def main():
    global next_data_port

    if len(sys.argv) != 2:
        print("Usage: python server.py <control_port>")
        sys.exit(1)

    control_port = int(sys.argv[1])
    next_data_port = control_port + 1000  # arbitrary offset scheme; revisit if it collides with anything

    print("Starting server...")
    control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    control_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("Creating server socket")
    control_sock.bind(('', control_port))
    control_sock.listen(5)
    print("Awaiting connections...")

    try:
        while True:
            # accept() blocks until a client connects, then hands back
            # a NEW socket object specific to that client. The listening
            # socket (control_sock) keeps running the loop for the next one.
            client_conn, addr = control_sock.accept()
            # One thread per client so a slow/blocked client can't stall
            # the accept() loop for everyone else.
            t = threading.Thread(target=handle_client, args=(client_conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        control_sock.close()


if __name__ == "__main__":
    main()