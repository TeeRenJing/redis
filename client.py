import socket

HOST = "127.0.0.1"
PORT = 6379

def send_command(cmd: str):
    """Send a single command and print the server response."""
    with socket.create_connection((HOST, PORT)) as sock:
        # Redis protocol expects commands in RESP format:
        # *N\r\n$len(arg1)\r\narg1\r\n... 
        parts = cmd.strip().split()
        resp = f"*{len(parts)}\r\n"
        for p in parts:
            resp += f"${len(p)}\r\n{p}\r\n"

        sock.sendall(resp.encode())
        data = sock.recv(4096)
        print(data.decode(errors="ignore"))

if __name__ == "__main__":
    # Examples:
    send_command("PING")
    send_command("SET mykey hello")
    send_command("GET mykey")

    # Add two stream entries
    send_command("XADD mystream * temperature 36 humidity 95")
    send_command("XADD mystream * temperature 37 humidity 94")

    # Retrieve entries in a range
    send_command("XRANGE mystream 0 9999999999999")