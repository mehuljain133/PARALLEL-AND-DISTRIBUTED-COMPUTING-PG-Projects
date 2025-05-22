# Unit-V Consistency and replication, Fault Tolerance, and Security: Introduction to consistency models and protocols, fault tolerance, and security issues in distributed systems.

import threading
import time
import random
import hmac
import hashlib

# -------------------
# Replicated Key-Value Store with Eventual Consistency
# -------------------
class ReplicatedStore:
    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.nodes = nodes  # dict node_id -> ReplicatedStore
        self.store = {}
        self.version = {}  # key -> version number (vector clock simplified)
        self.lock = threading.Lock()

    def put(self, key, value):
        with self.lock:
            self.store[key] = value
            self.version[key] = self.version.get(key, 0) + 1
        print(f"[{self.node_id}] PUT {key}={value} v{self.version[key]}")
        self.propagate_update(key, value, self.version[key])

    def get(self, key):
        with self.lock:
            return self.store.get(key, None)

    def propagate_update(self, key, value, version):
        # Simulate asynchronous propagation to other nodes
        for nid, node in self.nodes.items():
            if nid != self.node_id:
                threading.Thread(target=node.receive_update, args=(key, value, version)).start()

    def receive_update(self, key, value, version):
        with self.lock:
            current_version = self.version.get(key, 0)
            if version > current_version:
                self.store[key] = value
                self.version[key] = version
                print(f"[{self.node_id}] RECEIVED UPDATE {key}={value} v{version}")

# -------------------
# Fault Tolerance: Node Failure Simulation & Recovery
# -------------------
class Node(threading.Thread):
    def __init__(self, node_id, nodes, store):
        super().__init__()
        self.node_id = node_id
        self.nodes = nodes
        self.store = store
        self.alive = True
        self.lock = threading.Lock()

    def run(self):
        while self.alive:
            time.sleep(random.uniform(2,5))
            # Randomly simulate failure
            if random.random() < 0.1:
                self.simulate_failure()
            else:
                self.perform_work()

    def simulate_failure(self):
        with self.lock:
            self.alive = False
            print(f"[{self.node_id}] Simulating FAILURE!")

    def perform_work(self):
        # Just a dummy operation: read random key
        key = random.choice(["x", "y", "z"])
        val = self.store.get(key)
        print(f"[{self.node_id}] Read {key}={val}")

    def recover(self):
        with self.lock:
            self.alive = True
            print(f"[{self.node_id}] RECOVERED")

# -------------------
# Security: Message Signing and Verification using HMAC
# -------------------
class SecureChannel:
    def __init__(self, key):
        self.key = key.encode()

    def sign(self, message):
        return hmac.new(self.key, message.encode(), hashlib.sha256).hexdigest()

    def verify(self, message, signature):
        expected = hmac.new(self.key, message.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)

# -------------------
# Demo Usage
# -------------------
def main():
    print("--- Consistency & Replication Demo ---")
    nodes = {}
    stores = {}

    # Create 3 replicated stores for nodes N1, N2, N3
    for nid in ["N1", "N2", "N3"]:
        stores[nid] = ReplicatedStore(nid, nodes)
    # Initialize nodes dict now stores are created
    for nid in stores:
        nodes[nid] = stores[nid]

    # Simulate writes from N1
    stores["N1"].put("x", 10)
    stores["N1"].put("y", 20)

    time.sleep(1)  # Wait for propagation

    # Read values from N2 and N3
    print(f"N2 reads x: {stores['N2'].get('x')}")
    print(f"N3 reads y: {stores['N3'].get('y')}")

    print("\n--- Fault Tolerance Demo ---")
    # Create Node objects with thread
    node_threads = {}
    for nid in ["N1", "N2", "N3"]:
        node_threads[nid] = Node(nid, nodes, stores[nid])
        node_threads[nid].start()

    # Let nodes run and simulate failure/recovery
    time.sleep(10)

    # Recover failed nodes
    for nid, node in node_threads.items():
        if not node.alive:
            node.recover()
            node.alive = True
            threading.Thread(target=node.run).start()

    time.sleep(5)

    # Stop all threads gracefully
    for node in node_threads.values():
        node.alive = False
    for node in node_threads.values():
        node.join()

    print("\n--- Security Demo (Message Signing & Verification) ---")
    channel = SecureChannel("supersecretkey")
    msg = "Transfer $1000 to account 12345"
    signature = channel.sign(msg)
    print(f"Message: {msg}")
    print(f"Signature: {signature}")
    print("Verification:", "Passed" if channel.verify(msg, signature) else "Failed")

    # Try verifying with tampered message
    tampered_msg = "Transfer $10000 to account 12345"
    print("Tampered verification:", "Passed" if channel.verify(tampered_msg, signature) else "Failed")

if __name__ == "__main__":
    main()
