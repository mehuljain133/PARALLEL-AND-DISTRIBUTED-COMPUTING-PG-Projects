# Unit IV Naming and Coordination in Distributed Systems: Names, identifiers, and addresses, flatnaming, Structured naming, and attribute-based naming; coordination: clock synchronization, logicalclocks, mutual exclusion: centralized, distributed, token-ring, and decentralized algorithms; electionalgorithms, location systems, distributed event matching, gossip-based coordination

import threading
import time
import random

# -------------------
# Naming Systems: flat, structured, attribute-based
# -------------------
class NamingSystem:
    def __init__(self):
        self.flat_names = {}
        self.structured_names = {}

    def bind_flat(self, name, address):
        self.flat_names[name] = address

    def resolve_flat(self, name):
        return self.flat_names.get(name, None)

    def bind_structured(self, path_list, address):
        d = self.structured_names
        for part in path_list[:-1]:
            d = d.setdefault(part, {})
        d[path_list[-1]] = address

    def resolve_structured(self, path_list):
        d = self.structured_names
        for part in path_list:
            if isinstance(d, dict) and part in d:
                d = d[part]
            else:
                return None
        return d

    def resolve_by_attr(self, attr_key, attr_value):
        for name, addr in self.flat_names.items():
            if isinstance(addr, dict) and addr.get(attr_key) == attr_value:
                return addr
        return None

# -------------------
# Logical Clocks (Lamport)
# -------------------
class LamportClock:
    def __init__(self):
        self.time = 0
        self.lock = threading.Lock()

    def tick(self):
        with self.lock:
            self.time += 1
            return self.time

    def update(self, recv_time):
        with self.lock:
            self.time = max(self.time, recv_time) + 1
            return self.time

# -------------------
# Mutual Exclusion: Centralized Coordinator
# -------------------
class CentralizedMutex:
    def __init__(self):
        self.locked = False
        self.cond = threading.Condition()

    def acquire(self, client_id):
        with self.cond:
            while self.locked:
                self.cond.wait()
            self.locked = True
            print(f"[CentralizedMutex] Lock granted to {client_id}")

    def release(self, client_id):
        with self.cond:
            self.locked = False
            print(f"[CentralizedMutex] Lock released by {client_id}")
            self.cond.notify_all()

# -------------------
# Mutual Exclusion: Token Ring
# -------------------
class TokenRing:
    def __init__(self, node_ids):
        self.node_ids = node_ids
        self.token_holder = node_ids[0]
        self.lock = threading.Lock()
        self.token_available = threading.Event()
        self.token_available.set()

    def acquire(self, node_id):
        print(f"[TokenRing] {node_id} waiting for token")
        while True:
            self.token_available.wait()
            with self.lock:
                if self.token_holder == node_id:
                    print(f"[TokenRing] {node_id} acquired token")
                    self.token_available.clear()
                    return

    def release(self, node_id):
        print(f"[TokenRing] {node_id} releasing token")
        with self.lock:
            idx = self.node_ids.index(node_id)
            self.token_holder = self.node_ids[(idx + 1) % len(self.node_ids)]
            print(f"[TokenRing] Token passed to {self.token_holder}")
            self.token_available.set()

# -------------------
# Election Algorithm (Ring)
# -------------------
class RingElection:
    def __init__(self, node_ids):
        self.node_ids = node_ids
        self.leader = None

    def start_election(self, initiator_id):
        print(f"[Election] Election started by {initiator_id}")
        election_msg = [initiator_id]
        idx = (self.node_ids.index(initiator_id) + 1) % len(self.node_ids)
        while True:
            node = self.node_ids[idx]
            if node > max(election_msg):
                election_msg.append(node)
            idx = (idx + 1) % len(self.node_ids)
            if node == initiator_id:
                break
        self.leader = max(election_msg)
        print(f"[Election] Leader elected: {self.leader}")

# -------------------
# Location Service
# -------------------
class LocationService:
    def __init__(self):
        self.locations = {}

    def register(self, node_id, address):
        self.locations[node_id] = address

    def lookup(self, node_id):
        return self.locations.get(node_id)

# -------------------
# Event Matching: Pub-Sub
# -------------------
class EventBus:
    def __init__(self):
        self.subscriptions = {}

    def subscribe(self, event_type, callback):
        self.subscriptions.setdefault(event_type, []).append(callback)

    def publish(self, event_type, data):
        for cb in self.subscriptions.get(event_type, []):
            cb(data)

# -------------------
# Gossip-based Coordination
# -------------------
class GossipNode(threading.Thread):
    def __init__(self, node_id, nodes, stop_event):
        super().__init__()
        self.node_id = node_id
        self.nodes = nodes
        self.state = {}
        self.stop_event = stop_event

    def run(self):
        print(f"[GossipNode {self.node_id}] started")
        while not self.stop_event.is_set():
            self.state[self.node_id] = random.randint(1, 100)
            peer = random.choice([n for n in self.nodes if n != self.node_id])
            print(f"[GossipNode {self.node_id}] gossiping to {peer}")
            self.nodes[peer].receive_gossip(self.state)
            time.sleep(random.uniform(0.5, 1.5))

    def receive_gossip(self, peer_state):
        for k, v in peer_state.items():
            self.state[k] = max(self.state.get(k, 0), v)
        print(f"[GossipNode {self.node_id}] state updated: {self.state}")

# -------------------
# Demo Main
# -------------------
def main():
    print("\n--- Naming System ---")
    naming = NamingSystem()
    naming.bind_flat("printer", "192.168.1.10")
    naming.bind_structured(["company", "dept", "printer"], "10.0.0.20")
    naming.bind_flat("node1", {"ip": "10.0.0.1", "role": "worker"})

    print("Flat resolve printer:", naming.resolve_flat("printer"))
    print("Structured resolve company/dept/printer:", naming.resolve_structured(["company","dept","printer"]))
    print("Attr resolve role=worker:", naming.resolve_by_attr("role", "worker"))

    print("\n--- Lamport Clock ---")
    clock = LamportClock()
    print("Tick:", clock.tick())
    print("Update with 5:", clock.update(5))
    print("Tick:", clock.tick())

    print("\n--- Centralized Mutual Exclusion ---")
    mutex = CentralizedMutex()

    def client_centralized(id):
        mutex.acquire(id)
        time.sleep(1)
        mutex.release(id)

    t1 = threading.Thread(target=client_centralized, args=("Client1",))
    t2 = threading.Thread(target=client_centralized, args=("Client2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print("\n--- Token Ring Mutual Exclusion ---")
    token_ring = TokenRing(["NodeA", "NodeB", "NodeC"])

    def client_token(id):
        token_ring.acquire(id)
        time.sleep(1)
        token_ring.release(id)

    threads = []
    for nid in ["NodeA", "NodeB", "NodeC"]:
        t = threading.Thread(target=client_token, args=(nid,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    print("\n--- Election Algorithm ---")
    election = RingElection([1,3,5,2,4])
    election.start_election(3)

    print("\n--- Location Service ---")
    loc = LocationService()
    loc.register("Node1", "10.0.0.1")
    loc.register("Node2", "10.0.0.2")
    print("Lookup Node1:", loc.lookup("Node1"))
    print("Lookup Node3:", loc.lookup("Node3"))

    print("\n--- Event Matching ---")
    bus = EventBus()
    bus.subscribe("alert", lambda data: print(f"Received alert: {data}"))
    bus.publish("alert", {"level": "high", "msg": "Overheat detected"})

    print("\n--- Gossip-based Coordination ---")
    stop_event = threading.Event()
    nodes = {}
    for nid in ["N1", "N2", "N3"]:
        node = GossipNode(nid, nodes, stop_event)
        nodes[nid] = node
    for node in nodes.values():
        node.start()
    time.sleep(5)
    stop_event.set()
    for node in nodes.values():
        node.join()

if __name__ == "__main__":
    main()
