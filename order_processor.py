"""
Distributed Order Processing System

"""

from mpi4py import MPI
from multiprocessing import Manager, Lock
import time
import random


# SHARED ITEMS LIST (used by master to generate orders)
ITEMS = [
    "Laptop", "Phone", "Headphones", "Keyboard",
    "Mouse", "Monitor", "Webcam", "USB Hub",
]


# WORKER FUNCTION
def worker_process(rank, shared_orders, lock):
    comm = MPI.COMM_WORLD

    # Receive number of orders assigned to this worker
    num_orders = comm.recv(source=0, tag=10)
    print(f"[Worker {rank}] Assigned {num_orders} order(s) to process.", flush=True)

    for _ in range(num_orders):
        # Receive each order from master
        order = comm.recv(source=0, tag=11)
        print(f"[Worker {rank}] Received Order #{order['id']} — {order['item']}", flush=True)

        # Simulate processing delay (0.5 to 2 seconds)
        delay = round(random.uniform(0.5, 2.0), 2)
        time.sleep(delay)

        # Build result entry
        result = {
            "order_id":   order["id"],
            "item":       order["item"],
            "handled_by": rank,
            "delay_sec":  delay,
            "status":     "COMPLETED"
        }

        # Write to shared memory safely using Lock
        with lock:
            shared_orders.append(result)
            print(
                f"[Worker {rank}] ✔ Order #{order['id']} ({order['item']}) "
                f"completed in {delay}s — saved to shared memory.",
                flush=True
            )

    
    comm.send({"worker": rank, "done": True}, dest=0, tag=20)



# MASTER FUNCTION
def master_process(num_workers, shared_orders, lock):
    comm = MPI.COMM_WORLD

    # Generate 5–8 random orders
    num_orders = random.randint(5, 8)
    orders = [
        {"id": i + 1, "item": random.choice(ITEMS)}
        for i in range(num_orders)
    ]

    print(f"\n[Master] Generated {num_orders} orders:", flush=True)
    for o in orders:
        print(f"         Order #{o['id']} — {o['item']}", flush=True)
    print(flush=True)

    # Distribute orders round-robin to workers
    assignment = {rank: [] for rank in range(1, num_workers + 1)}
    for idx, order in enumerate(orders):
        target_rank = (idx % num_workers) + 1
        assignment[target_rank].append(order)

    # Send each worker its order count, then the orders
    for rank in range(1, num_workers + 1):
        worker_orders = assignment[rank]
        comm.send(len(worker_orders), dest=rank, tag=10)
        for order in worker_orders:
            comm.send(order, dest=rank, tag=11)
        print(
            f"[Master] Sent {len(worker_orders)} order(s) to Worker {rank}",
            flush=True
        )

    print(flush=True)

    # Wait for all workers to finish
    for rank in range(1, num_workers + 1):
        ack = comm.recv(source=rank, tag=20)
        print(f"[Master] Worker {ack['worker']} has finished all its orders.", flush=True)

   
    print("\n" + "=" * 55, flush=True)
    print("         FINAL COMPLETED ORDERS (from shared memory)", flush=True)
    print("=" * 55, flush=True)

    completed = sorted(list(shared_orders), key=lambda x: x["order_id"])

    for entry in completed:
        print(
            f"  Order #{entry['order_id']:>2} | {entry['item']:<12} | "
            f"Worker {entry['handled_by']} | "
            f"{entry['delay_sec']}s | {entry['status']}",
            flush=True
        )

    print("=" * 55, flush=True)
    print(f"  Total orders processed: {len(completed)} / {num_orders}", flush=True)
    print("=" * 55 + "\n", flush=True)



# ENTRY POINT
if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        if rank == 0:
            print("ERROR: Run with at least 2 processes.")
            print("Example: mpiexec -n 4 python order_processor.py")
        MPI.Finalize()
        exit(1)

    num_workers = size - 1

    # Initialize shared memory BEFORE branching into master/worker
    manager = Manager()
    shared_orders = manager.list()
    lock = Lock()

    if rank == 0:
        print(f"\n[Master] Starting with {num_workers} worker(s)...\n", flush=True)
        master_process(num_workers, shared_orders, lock)
    else:
        worker_process(rank, shared_orders, lock)

    comm.Barrier()