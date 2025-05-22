# Unit II Distributed Computing Architectures: Characteristics and goals of distributed computing, architectural styles: centralized, decentralized, and hybrid architectures, layered, object-based and service oriented, resource-based, publish-subscribe architectures, middleware organization: wrappers, interceptors, and modifiable middleware, system architecture, example architectures: network file system and web

import multiprocessing
import socket
import threading
import time
import queue

# Basic configuration
HOST = '127.0.0.1'
SERVER_PORT = 50000
PEER_PORT_START = 51000

# Middleware interceptor example
def logging_interceptor(func):
    def wrapper(*args, **kwargs):
        print(f"[Middleware] Calling {func.__name__} with args {args[1:]}")
        result = func(*args, **kwargs)
        print(f"[Middleware] Result: {result}")
        return result
    return wrapper

# Service Layer (file service example)
class FileService:
    def __init__(self):
        self.files = {"readme.txt": "This is a simple NFS-like service.\nHello from server!"}

    @logging_interceptor
    def read_file(self, filename):
        return self.files.get(filename, "File not found.")

    @logging_interceptor
    def write_file(self, filename, content):
        self.files[filename] = content
        return f"Written {len(content)} bytes to {filename}"

# Centralized Server (like NFS server)
def centralized_server(stop_event):
    print("[Server] Starting centralized file server")
    service = FileService()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, SERVER_PORT))
    s.listen()

    s.settimeout(1.0)
    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr, service)).start()
        except socket.timeout:
            continue
    s.close()
    print("[Server] Server stopped")

def handle_client(conn, addr, service):
    with conn:
        print(f"[Server] Connected by {addr}")
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            cmd = data.split()
            if cmd[0] == "READ":
                filename = cmd[1]
                content = service.read_file(filename)
                conn.sendall(content.encode())
            elif cmd[0] == "WRITE":
                filename = cmd[1]
                content = " ".join(cmd[2:])
                result = service.write_file(filename, content)
                conn.sendall(result.encode())
            else:
                conn.sendall(b"Unknown command")

# Decentralized Peer Node
def peer_node(node_id, peer_ports, stop_event, pubsub_queue):
    print(f"[Peer {node_id}] Starting decentralized peer node")
    # Listen socket
    port = PEER_PORT_START + node_id
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, port))
    s.listen()
    s.settimeout(1.0)

    # Run listener thread
    threading.Thread(target=peer_listener, args=(s, stop_event)).start()

    # Periodically send messages to peers
    while not stop_event.is_set():
        for peer_port in peer_ports:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_sock:
                    peer_sock.settimeout(0.5)
                    peer_sock.connect((HOST, peer_port))
                    msg = f"HELLO from peer {node_id}"
                    peer_sock.sendall(msg.encode())
            except Exception:
                pass
        # Publish a notification (publish-subscribe)
        pubsub_queue.put(f"Peer {node_id} alive at {time.time()}")
        time.sleep(3)

def peer_listener(sock, stop_event):
    while not stop_event.is_set():
        try:
            conn, addr = sock.accept()
            threading.Thread(target=handle_peer_message, args=(conn, addr)).start()
        except socket.timeout:
            continue
    sock.close()

def handle_peer_message(conn, addr):
    with conn:
        data = conn.recv(1024).decode()
        print(f"[Peer Listener] Received from {addr}: {data}")

# Publish-Subscribe Broker
def pubsub_broker(stop_event, pubsub_queue):
    subscribers = []
    print("[PubSub] Broker started")
    while not stop_event.is_set():
        try:
            msg = pubsub_queue.get(timeout=1)
            print(f"[PubSub] Publishing message: {msg}")
            # In real systems, would push to subscribers here
        except queue.Empty:
            continue
    print("[PubSub] Broker stopped")

# Hybrid client that can talk to server or peers
def hybrid_client(client_id, stop_event):
    print(f"[Hybrid Client {client_id}] Starting")
    time.sleep(1)  # wait for server to start

    # Talk to centralized server
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, SERVER_PORT))
            s.sendall(b"READ readme.txt")
            data = s.recv(1024).decode()
            print(f"[Hybrid Client {client_id}] Read from server: {data}")
            # Write to file
            s.sendall(b"WRITE newfile.txt This is from hybrid client.")
            resp = s.recv(1024).decode()
            print(f"[Hybrid Client {client_id}] Write response: {resp}")
    except Exception as e:
        print(f"[Hybrid Client {client_id}] Server connection failed: {e}")

    # Talk to a peer (simulate decentralized interaction)
    peer_port = PEER_PORT_START + ((client_id + 1) % 3)  # some peer
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, peer_port))
            msg = f"Hi peer from hybrid client {client_id}"
            s.sendall(msg.encode())
    except Exception as e:
        print(f"[Hybrid Client {client_id}] Peer connection failed: {e}")

    while not stop_event.is_set():
        time.sleep(5)

if __name__ == "__main__":
    stop_event = multiprocessing.Event()
    pubsub_queue = multiprocessing.Queue()

    # Start centralized server
    server_proc = multiprocessing.Process(target=centralized_server, args=(stop_event,))
    server_proc.start()

    # Start pub-sub broker
    pubsub_proc = multiprocessing.Process(target=pubsub_broker, args=(stop_event, pubsub_queue))
    pubsub_proc.start()

    # Start decentralized peers
    peer_procs = []
    peer_count = 3
    peer_ports = [PEER_PORT_START + i for i in range(peer_count)]
    for i in range(peer_count):
        # Each peer connects to all others except itself
        others = [p for p in peer_ports if p != (PEER_PORT_START + i)]
        p = multiprocessing.Process(target=peer_node, args=(i, others, stop_event, pubsub_queue))
        peer_procs.append(p)
        p.start()

    # Start hybrid clients
    client_procs = []
    client_count = 2
    for i in range(client_count):
        p = multiprocessing.Process(target=hybrid_client, args=(i, stop_event))
        client_procs.append(p)
        p.start()

    try:
        # Let system run for 20 seconds
        time.sleep(20)
    except KeyboardInterrupt:
        pass

    # Stop all processes
    stop_event.set()
    server_proc.join()
    pubsub_proc.join()
    for p in peer_procs:
        p.join()
    for p in client_procs:
        p.join()

    print("Distributed system simulation stopped.")
