# Unit-I Parallel Computing: Trends in microprocessor architectures, memory system performance,dichotomy of parallel computing platforms, physical organization of parallel platforms,communication costs in parallel machines, SIMD versus MIMD architectures, global versusdistributed memory, the PRAM shared-memory model, distributed-memory or graph models, basic algorithms for some simple architectures: linear array, binary tree, 2D mesh with shared variables.

import multiprocessing
import time
import math

# Simulation parameters
COMM_DELAY = 0.01  # simulate communication cost

def linear_array_worker(rank, size, shared_array):
    """
    Each node passes a token along linear array.
    Shared memory is used for communication.
    """
    if rank == 0:
        # Start token
        shared_array[0] = 1
        print(f"Processor {rank} sets token=1")
    else:
        # wait until previous has token
        while shared_array[rank - 1] == 0:
            time.sleep(0.001)
        # receive token and increment
        shared_array[rank] = shared_array[rank - 1] + 1
        print(f"Processor {rank} received token={shared_array[rank]}")
    time.sleep(COMM_DELAY)

def binary_tree_worker(rank, size, values, results, barrier):
    """
    Simulate binary tree reduction sum.
    values: initial values for each processor
    results: shared memory for results
    barrier: to sync before reduce
    """
    # Initial value per processor
    val = values[rank]
    barrier.wait()
    
    step = 1
    while step < size:
        if rank % (2*step) == 0:
            # Receive from partner
            partner = rank + step
            if partner < size:
                time.sleep(COMM_DELAY)  # communication delay
                val += results[partner]
        barrier.wait()
        results[rank] = val
        step *= 2
    
    if rank == 0:
        print(f"Binary Tree Reduction Result: {val}")

def mesh_worker(row, col, rows, cols, mesh_data, barrier):
    """
    2D mesh communication: each processor updates based on neighbors.
    mesh_data: shared 2D array
    """
    idx = row * cols + col
    barrier.wait()
    # Compute average with neighbors (simple stencil)
    neighbors = []
    if row > 0: neighbors.append(mesh_data[(row-1)*cols + col])
    if row < rows-1: neighbors.append(mesh_data[(row+1)*cols + col])
    if col > 0: neighbors.append(mesh_data[row*cols + col-1])
    if col < cols-1: neighbors.append(mesh_data[row*cols + col+1])
    
    new_val = (mesh_data[idx] + sum(neighbors)) / (1 + len(neighbors))
    time.sleep(COMM_DELAY)  # simulate communication
    mesh_data[idx] = new_val
    print(f"Mesh node ({row},{col}) updated value to {new_val:.2f}")

def simd_worker(rank, size, data, results):
    """
    SIMD-like: all processors perform same operation (square numbers).
    """
    val = data[rank]
    results[rank] = val * val
    print(f"SIMD Processor {rank} squared {val} -> {results[rank]}")

def mimd_worker(rank, size, data, results):
    """
    MIMD-like: different computations per processor.
    Even ranks: square, Odd ranks: cube
    """
    val = data[rank]
    if rank % 2 == 0:
        results[rank] = val * val
        print(f"MIMD Processor {rank} squared {val} -> {results[rank]}")
    else:
        results[rank] = val * val * val
        print(f"MIMD Processor {rank} cubed {val} -> {results[rank]}")

if __name__ == "__main__":
    size = 8  # number of processors/nodes
    
    print("\n--- Linear Array Token Passing (Shared Memory) ---")
    with multiprocessing.Manager() as manager:
        shared_array = manager.list([0]*size)
        procs = []
        for i in range(size):
            p = multiprocessing.Process(target=linear_array_worker, args=(i, size, shared_array))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()
    
    print("\n--- Binary Tree Reduction Sum ---")
    with multiprocessing.Manager() as manager:
        values = manager.list([i+1 for i in range(size)])  # values 1 to size
        results = manager.list([0]*size)
        barrier = multiprocessing.Barrier(size)
        procs = []
        for i in range(size):
            results[i] = values[i]
        for i in range(size):
            p = multiprocessing.Process(target=binary_tree_worker, args=(i, size, values, results, barrier))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()
    
    print("\n--- 2D Mesh Update ---")
    rows, cols = 3, 3
    with multiprocessing.Manager() as manager:
        mesh_data = manager.list([float(i+1) for i in range(rows*cols)])
        barrier = multiprocessing.Barrier(rows*cols)
        procs = []
        for r in range(rows):
            for c in range(cols):
                p = multiprocessing.Process(target=mesh_worker, args=(r, c, rows, cols, mesh_data, barrier))
                procs.append(p)
                p.start()
        for p in procs:
            p.join()
    
    print("\n--- SIMD Computation ---")
    with multiprocessing.Manager() as manager:
        data = manager.list([i+1 for i in range(size)])
        results = manager.list([0]*size)
        procs = []
        for i in range(size):
            p = multiprocessing.Process(target=simd_worker, args=(i, size, data, results))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()

    print("\n--- MIMD Computation ---")
    with multiprocessing.Manager() as manager:
        data = manager.list([i+1 for i in range(size)])
        results = manager.list([0]*size)
        procs = []
        for i in range(size):
            p = multiprocessing.Process(target=mimd_worker, args=(i, size, data, results))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()
