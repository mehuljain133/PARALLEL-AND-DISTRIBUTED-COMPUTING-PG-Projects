# Unit-III Distributed Processes and Communication in Distributed Systems: Threads indistributed systems, principle of virtualization, clients: network user interfaces and client-sidesoftware for distribution transparency; servers: design issues, object servers, server clusters; codemigration; Layered protocols, remote procedure call: RPC operation, parameter passing; messageoriented communication: transient messaging with sockets, message-oriented persistentcommunication; multicast communication: application-level tree-based multicasting, flooding-basedmulticasting, gossip-based data dissemination.

import multiprocessing
import threading
import socket
import pickle
import time
import queue
import random

HOST = '127.0.0.1'
BASE_PORT = 60000

# -------------------------------
# Helper: RPC Framework
# -------------------------------

def rpc_server(port, handler_obj, stop_event):
    """Simple RPC server: listens for requests, calls handler methods, returns result."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, port))
    s.listen()
    s.settimeout(1)
    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            threading.Thread(target=handle_rpc_client, args=(conn, handler_obj)).start()
        except socket.timeout:
            continue
    s.close()

def handle_rpc_client(conn, handler_obj):
    with conn:
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
            if len(chunk) < 4096:
                break
        try:
            request = pickle.loads(data)
            method_name, args, kwargs = request
            method = getattr(handler_obj, method_name)
            result = method(*args, **kwargs)
            response = pickle.dumps(result)
            conn.sendall(response)
        except Exception as e:
            err = pickle.dumps(f"RPC error: {e}")
            conn.sendall(err)

def rpc_client(port, method_name, *args, **kwargs):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, port))
    request = pickle.dumps((method_name, args, kwargs))
    s.sendall(request)
    data = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(chunk) < 4096:
            break
    s.close()
    return pickle.loads(data)

# -------------------------------
# Object Server with simple cluster simulation
# -------------------------------

class ObjectServer:
    def __init__(self):
        self.store = {}
    def store_object(self, key, obj):
        self.store[key] = obj
        return f"Stored object at key {key}"
    def get_object(self, key):
        return self.store.get(key, None)
    def list_objects(self):
        return list(self.store.keys())

# -------------------------------
# Client abstraction with virtualization
# -------------------------------

class ClientVirtual:
    def __init__(self, server_port):
        self.server_port = server_port
    def store(self, key, obj):
        return rpc_client(self.server_port, 'store_object', key, obj)
    def fetch(self, key):
        return rpc_client(self.server_port, 'get_object', key)
    def list_keys(self):
        return rpc_client(self.server_port, 'list_objects')

# -------------------------------
# Code migration simulation (sending code as string + exec)
# -------------------------------

def code_migration_worker(port, stop_event):
    """Worker that accepts code and executes it."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, port))
    s.listen()
    s.settimeout(1)
    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            threading.Thread(target=handle_code_migration, args=(conn,)).start()
        except socket.timeout:
            continue
    s.close()

def handle_code_migration(conn):
    with conn:
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
            if len(chunk) < 4096:
                break
        code_str = data.decode()
        print(f"[Code Migration] Received code:\n{code_str}")
        try:
            # Dangerous in real life! Just simulating
            exec_globals = {}
            exec(code_str, exec_globals)
            result = exec_globals.get('result', 'No result returned')
        except Exception as e:
            result = f"Execution error: {e}"
        conn.sendall(str(result).encode())

def migrate_code(port, code_str):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, port))
    s.sendall(code_str.encode())
    data = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(chunk) < 4096:
            break
    s.close()
    return data.decode()

# -------------------------------
# Message-oriented communication
# -------------------------------

class MessageQueue:
    def __init__(self):
        self.queue = queue.Queue()
    def send(self, msg):
        self.queue.put(msg)
    def receive(self, timeout=1):
        try:
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            return None

def transient_message_sender(port, messages, delay=1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    for msg in messages:
        try:
            s.connect((HOST, port))
            s.sendall(msg.encode())
            s.close()
            print(f"[Transient Msg Sender] Sent: {msg}")
        except Exception as e:
            print(f"[Transient Msg Sender] Error: {e}")
        time.sleep(delay)

def transient_message_receiver(port, stop_event):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, port))
    s.listen()
    s.settimeout(1)
    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            with conn:
                data = conn.recv(1024).decode()
                print(f"[Transient Msg Receiver] Received: {data}")
        except socket.timeout:
            continue
    s.close()

# Persistent message queue via multiprocessing.Queue
def persistent_message_sender(msg_queue, messages, delay=1):
    for msg in messages:
        msg_queue.put(msg)
        print(f"[Persistent Msg Sender] Queued: {msg}")
        time.sleep(delay)

def persistent_message_receiver(msg_queue, stop_event):
    while not stop_event.is_set():
        msg = None
        try:
            msg = msg_queue.get(timeout=1)
        except queue.Empty:
            continue
        print(f"[Persistent Msg Receiver] Got: {msg}")

# -------------------------------
# Multicast communication
# -------------------------------

def tree_multicast(node_id, children, msg, send_func):
    print(f"[Tree Multicast Node {node_id}] Received: {msg}")
    for c in children:
        send_func(c, msg)

def flooding_multicast(nodes, origin_id, msg, send_func):
    visited = set()
    def flood(node):
        if node in visited:
            return
        print(f"[Flooding Multicast Node {node}] Received: {msg}")
        visited.add(node)
        neighbors = [n for n in nodes if n != node]
        for nb in neighbors:
            flood(nb)
    flood(origin_id)

def gossip_multicast(nodes, origin_id, msg, send_func, rounds=3):
    informed = {origin_id}
    for r in range(rounds):
        new_informed = set()
        for node in informed:
            # each informed node contacts one random uninformed node
            uninformed = set(nodes) - informed
            if uninformed:
                target = random.choice(list(uninformed))
                print(f"[Gossip Round {r} Node {node} -> Node {target}] Msg: {msg}")
                new_informed.add(target)
        informed.update(new_informed)

# -------------------------------
# Main orchestration
# -------------------------------

def main():
    stop_event = multiprocessing.Event()

    # 1. Start Object Server RPC
    obj_server_port = BASE_PORT
    obj_server = ObjectServer()
    server_proc = multiprocessing.Process(target=rpc_server, args=(obj_server_port, obj_server, stop_event))
    server_proc.start()

    time.sleep(1)

    # 2. Client with virtualization
    client = ClientVirtual(obj_server_port)
    print("[Client] Store object:", client.store("foo", {"data": 123}))
    print("[Client] Fetch object:", client.fetch("foo"))
    print("[Client] List keys:", client.list_keys())

    # 3. Code migration
    migration_port = BASE_PORT + 1
    migration_stop = multiprocessing.Event()
    migration_proc = multiprocessing.Process(target=code_migration_worker, args=(migration_port, migration_stop))
    migration_proc.start()

    time.sleep(1)

    code_to_migrate = """
result = 0
for i in range(5):
    result += i*i
"""

    print("[Code Migration] Result:", migrate_code(migration_port, code_to_migrate))

    # 4. Transient message communication
    trans_port = BASE_PORT + 2
    trans_stop = multiprocessing.Event()
    trans_receiver_proc = multiprocessing.Process(target=transient_message_receiver, args=(trans_port, trans_stop))
    trans_receiver_proc.start()

    time.sleep(1)
    messages = ["Hello", "This is transient", "Goodbye"]
    transient_message_sender(trans_port, messages, delay=0.5)
    time.sleep(2)
    trans_stop.set()
    trans_receiver_proc.join()

    # 5. Persistent message communication
    msg_queue = multiprocessing.Queue()
    persist_stop = multiprocessing.Event()
    persist_receiver_proc = multiprocessing.Process(target=persistent_message_receiver, args=(msg_queue, persist_stop))
    persist_receiver_proc.start()

    persistent_message_sender(msg_queue, ["Msg1", "Msg2", "Msg3"], delay=0.5)
    time.sleep(2)
    persist_stop.set()
    persist_receiver_proc.join()

    # 6. Multicast communication demo
    nodes = [0,1,2,3]

    def dummy_send_func(target_node, message):
        print(f"[SendFunc] Sending to node {target_node}: {message}")

    print("\n--- Tree-based Multicast ---")
    tree_multicast(0, [1,2], "Tree message", dummy_send_func)

    print("\n--- Flooding Multicast ---")
    flooding_multicast(nodes, 0, "Flood message", dummy_send_func)

    print("\n--- Gossip Multicast ---")
    gossip_multicast(nodes, 0, "Gossip message", dummy_send_func, rounds=3)

    # Stop server
    stop_event.set()
    server_proc.join()
    migration_stop.set()
    migration_proc.join()

if __name__ == "__main__":
    main()
