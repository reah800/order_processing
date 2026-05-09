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
