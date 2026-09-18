"""Chat client for server.py.

Protocol summary (as implemented by the server):
  * Connect to the control port. The server replies "200\\n\\n<data_port>\\n".
  * Connect a second socket to <data_port>. This is the data connection.
  * Send commands (one per line) on the control connection:
        login <name> | who | broadcast <msg> | private <user> <msg> | quit
  * Everything the server sends back arrives on the data connection as
        "<status>\\n\\n" followed by zero or more body lines, each ending in "\\n".
    Server pushes are recognised by their first body line:
        Broadcast / <sender> / <message>
        Private   / <sender> / <message>
        join      / <username>
        leave     / <username>

Usage: python client.py <host> <control_port>
"""

import codecs
import os
import re
import socket
import sys
import threading
import time
from collections import deque

STATUS_RE = re.compile(r"^\d{3}$")

# First body line of a server push -> total number of body lines in that push.
PUSH_BODY_LEN = {"Broadcast": 3, "Private": 3, "join": 2, "leave": 2}

# The wire format has no length prefix, so a bare "200\n\n" (empty body) can't be
# told apart from a header whose body is still in flight. If nothing more shows
# up within this many seconds, we treat the message as having an empty body.
FLUSH_TIMEOUT = 0.15

HELP = """Commands:
  login <username>          log in with a username
  who                       list users currently online
  broadcast <message>       send a message to everyone
  private <user> <message>  send a message to one user
  help                      show this help
  quit                      disconnect and exit"""


def parse_messages(buf, flush=False):
    """Pull complete messages out of buf.

    Returns (messages, remaining_buf) where each message is (status, body_lines).
    With flush=True, a trailing header with no body is delivered as an empty message.
    """
    lines = buf.split("\n")
    partial = lines.pop()  # text after the last "\n" (incomplete line, possibly "")
    n = len(lines)
    msgs = []
    i = 0  # index of the first unconsumed complete line

    while n - i >= 2:
        if not STATUS_RE.match(lines[i]) or lines[i + 1] != "":
            i += 1  # not a header; skip a line to resynchronise
            continue

        status = int(lines[i])
        j = i + 2  # first body line

        if j >= n:
            # Header only, nothing after it yet.
            if flush and partial == "":
                msgs.append((status, []))
                i = j
                continue
            break

        first = lines[j]
        if STATUS_RE.match(first):
            # Next line is another header, so this message had an empty body.
            msgs.append((status, []))
            i = j
        elif status == 200 and first in PUSH_BODY_LEN:
            need = PUSH_BODY_LEN[first]
            if n - j < need:
                break  # rest of the push hasn't arrived yet
            msgs.append((status, lines[j:j + need]))
            i = j + need
        else:
            # Single-line body (the reply to "who").
            msgs.append((status, [first]))
            i = j + 1

    rest = "".join(line + "\n" for line in lines[i:]) + partial
    return msgs, rest


class ChatClient:
    def __init__(self, control_sock, data_sock):
        self.control = control_sock
        self.data = data_sock
        self.username = None
        self.logged_in = False
        # Commands awaiting a direct reply on the data socket, oldest first.
        self.pending = deque()
        self.print_lock = threading.Lock()

    # ---------- output ----------

    def show(self, text):
        """Print text from the reader thread without mangling the prompt."""
        with self.print_lock:
            print(f"\r{text}\n> ", end="", flush=True)

    def terminate(self, message=None):
        with self.print_lock:
            if message:
                print(f"\r{message}")
            sys.stdout.flush()
        for sock in (self.data, self.control):
            try:
                sock.close()
            except OSError:
                pass
        # os._exit because the main thread may be blocked in input().
        os._exit(0)

    # ---------- receiving ----------

    def reader_loop(self):
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        buf = ""
        self.data.settimeout(FLUSH_TIMEOUT)

        while True:
            try:
                chunk = self.data.recv(4096)
            except socket.timeout:
                msgs, buf = parse_messages(buf, flush=True)
                for status, body in msgs:
                    self.handle_message(status, body)
                continue
            except OSError:
                break
            if not chunk:
                break
            buf += decoder.decode(chunk)
            msgs, buf = parse_messages(buf)
            for status, body in msgs:
                self.handle_message(status, body)

        # Socket closed: deliver anything still buffered (e.g. the quit ack).
        msgs, _ = parse_messages(buf, flush=True)
        for status, body in msgs:
            self.handle_message(status, body)
        self.terminate("Disconnected from server.")

    def handle_message(self, status, body):
        # Unsolicited pushes.
        if status == 200 and body and body[0] in PUSH_BODY_LEN:
            kind = body[0]
            if kind == "Broadcast":
                sender = "you" if body[1] == self.username else body[1]
                self.show(f"[broadcast] {sender}: {body[2]}")
            elif kind == "Private":
                self.show(f"[private] {body[1]}: {body[2]}")
            elif kind == "join":
                self.show(f"* {body[1]} joined")
            elif kind == "leave":
                self.show(f"* {body[1]} left")
            return

        # Otherwise it's the direct reply to the oldest outstanding command.
        cmd = self.pending.popleft() if self.pending else None
        ok = status == 200

        if cmd is None:
            self.show(f"Server replied {status}")
        elif cmd[0] == "login":
            if ok:
                self.logged_in = True
                self.username = cmd[1]
                self.show(f"Logged in as {cmd[1]}")
            else:
                self.show("Login failed (name taken, or you're already logged in)")
        elif cmd[0] == "who":
            if ok:
                names = body[0] if body else ""
                self.show(f"Online: {names}" if names else "No users online")
            else:
                self.show("Who request failed")
        elif cmd[0] == "private":
            if ok:
                self.show(f"[private] you -> {cmd[1]}: {cmd[2]}")
            else:
                self.show(f"Couldn't send: no user named '{cmd[1]}'")
        elif cmd[0] == "quit":
            self.terminate("Goodbye.")

    # ---------- sending ----------

    def send_command(self, line, expect=None):
        # Register the expected reply *before* sending so it can't arrive first.
        if expect is not None:
            self.pending.append(expect)
        try:
            self.control.sendall((line + "\n").encode("utf-8"))
        except OSError:
            self.terminate("Lost connection to server.")

    def request_quit(self):
        self.send_command("quit", ("quit",))
        # The reader terminates on the server's ack; this is just a fallback.
        time.sleep(2)
        self.terminate("Goodbye.")

    def input_loop(self):
        print(HELP)
        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                self.request_quit()
                return
            if not line:
                continue

            cmd, _, rest = line.partition(" ")
            cmd = cmd.lower()
            rest = rest.strip()

            if cmd == "help":
                print(HELP)

            elif cmd == "login":
                if self.logged_in:
                    print(f"Already logged in as {self.username}.")
                elif not rest or len(rest.split()) != 1:
                    print("Usage: login <username>  (no spaces)")
                elif rest in PUSH_BODY_LEN or rest.isdigit() or "," in rest:
                    print("That username can't be used (it would confuse the "
                          "protocol). Pick another.")
                else:
                    self.send_command(f"login {rest}", ("login", rest))

            elif cmd == "who":
                self.send_command("who", ("who",))

            elif cmd == "broadcast":
                if not self.logged_in:
                    print("Log in first.")
                elif not rest:
                    print("Usage: broadcast <message>")
                else:
                    # The reply is the broadcast echoed back like everyone else's.
                    self.send_command(f"broadcast {rest}")

            elif cmd == "private":
                target, _, message = rest.partition(" ")
                message = message.strip()
                if not self.logged_in:
                    print("Log in first.")
                elif not target or not message:
                    print("Usage: private <user> <message>")
                else:
                    self.send_command(f"private {target} {message}",
                                      ("private", target, message))

            elif cmd == "quit":
                self.request_quit()
                return

            else:
                print(f"Unknown command '{cmd}'. Type 'help' for a list.")


def connect(host, control_port):
    """Do the control-connection handshake, then open the data connection."""
    control = socket.create_connection((host, control_port))
    buf = ""
    while True:
        if buf.count("\n") >= 2 and not buf.startswith("200"):
            raise ConnectionError(f"server refused connection: {buf.strip()!r}")
        if buf.count("\n") >= 3:
            break
        chunk = control.recv(1024)
        if not chunk:
            raise ConnectionError("server closed the connection during handshake")
        buf += chunk.decode("utf-8")

    port_line = buf.split("\n")[2].strip()
    if not port_line.isdigit():
        raise ConnectionError(f"bad data port from server: {port_line!r}")

    data = socket.create_connection((host, int(port_line)))
    return control, data


def main():
    if len(sys.argv) != 3:
        print("Usage: python client.py <host> <control_port>")
        sys.exit(1)

    host = sys.argv[1]
    try:
        control_port = int(sys.argv[2])
    except ValueError:
        print("control_port must be a number")
        sys.exit(1)

    try:
        control, data = connect(host, control_port)
    except (OSError, ConnectionError) as e:
        print(f"Could not connect: {e}")
        sys.exit(1)

    client = ChatClient(control, data)
    threading.Thread(target=client.reader_loop, daemon=True).start()
    client.input_loop()


if __name__ == "__main__":
    main()