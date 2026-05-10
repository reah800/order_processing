Reahlyn S. Ermita  CS3C

Reflection Questions — Distributed Order Processing

1. How did you distribute orders among worker processes?
I distributed the orders using a round-robin approach. What I did was have the master process (rank 0) generate 5–8 random orders and then assign them one by one to each worker, cycling through them in order. So for example, if I had 3 workers, Order 1 went to Worker 1, Order 2 went to Worker 2, Order 3 went to Worker 3, and then Order 4 went back to Worker 1 again. I used comm.send() to first tell each worker how many orders it would receive, and then sent the actual orders one at a time. The workers received them using comm.recv() with matching tags.

2. What happens if there are more orders than workers?
What I noticed is that when there are more orders than workers, the extra orders just wrap back around to the earlier workers. For instance, when I had 3 workers but 7 orders, Worker 1 ended up handling Orders 1, 4, and 7 while Worker 2 got Orders 2 and 5, and Worker 3 got Orders 3 and 6. Each worker just loops through however many orders it was assigned, so no order gets skipped or lost. I made sure every order was assigned before any processing started, so the full workload always gets completed even when the distribution isn't perfectly even.

3. How did processing delays affect the order completion?
I added a time.sleep() call in each worker with a random delay between 0.5 and 2.0 seconds to simulate real processing time, like what would happen with a database write or an external API call. Because each worker is its own separate MPI process, they all run in parallel and don't wait for each other. What I observed is that orders don't finish in the same order they were sent — a worker with a shorter delay finishes earlier even if it received its order later. This made the output look out of order sometimes, which I thought was actually a realistic behavior of how distributed systems work in the real world.

4. How did you implement shared memory, and where was it initialized?
I implemented shared memory using multiprocessing.Manager().list(). The way it works is that the Manager starts a background server process that manages the shared list and lets all the MPI processes access it. I made sure to initialize it before the if rank == 0 branch so that both the master and all the workers would have access to the same shared_orders list from the beginning. After each worker finished processing an order, I had it append a dictionary with details like the order ID, item name, which worker handled it, how long it took, and the status. Then at the end, the master just reads the full list and prints the summary.

5. What issues occurred when multiple workers wrote to shared memory simultaneously?
When I first ran the system without any synchronization, I noticed that the results were sometimes incomplete or inconsistent. What was happening is that multiple workers were trying to append to the shared list at the same time, which caused a race condition. Because the timing of each process is unpredictable, some writes would overlap and entries would end up getting lost. There were cases where I processed 6 orders but the master only showed 5 in the final list. It made me realize that without controlling access to shared memory, you really can't trust the output.

6. How did you ensure consistent results when using multiple processes?
To fix the race condition, I used a multiprocessing.Lock() to protect the part of the code where workers write to the shared list. I wrapped the append() call inside a with lock: block, which makes sure only one worker at a time can write to shared_orders. If another worker tries to write while the lock is held, it just waits until the first one is done. I initialized the lock in the same place as the Manager, before branching into master and worker logic, so all processes share the same lock. After adding this, the master consistently printed the correct number of completed orders every single time.

-----------------------------------------------------------------------------------------------------------------------------------------------

Carmichael M. Damalan CS3C
Reflection Questions — Distributed Order Processing

1 How did you distribute orders among worker processes?
- The orders were divided using a round-robin technique. The master process, which is rank 0, created around 5–8 random orders and sent them to workers one after another. If there were 3 workers, the first order was sent to Worker 1, the second to Worker 2, the third to Worker 3, and then the next order started again from Worker 1. I used comm.send() to tell workers how many orders they would handle before sending the actual order data. On the worker side, the orders were accepted using comm.recv() with the correct tags.

2. What happens if there are more orders than workers?
- If the number of orders is larger than the number of workers, the remaining orders are simply shared again starting from the first worker. For example, with 7 orders and 3 workers, some workers receive more tasks than others. Worker 1 may process Orders 1, 4, and 7, while the others handle the remaining orders. Since every worker loops through all assigned tasks, all orders are still completed even when the workload is uneven.

3. How did processing delays affect the order completion?
- To imitate real system processing, I added a random delay using time.sleep() between 0.5 and 2 seconds. This delay represented situations like database access or API requests. Because MPI workers run independently, they process tasks at the same time instead of waiting for one another. As a result, some orders finished earlier even though they were assigned later. The final output sometimes appeared mixed or out of sequence, which is common in distributed systems.

4.How did you implement shared memory, and where was it initialized?
- Shared memory was implemented with multiprocessing.Manager().list(). This shared list allowed all processes to store and access completed order information. I initialized both the Manager and the shared list before separating the master and worker processes so they could all access the same memory space. Whenever a worker completed an order, it inserted information like the order number, item, worker ID, processing time, and status into the shared list. The master process later displayed all completed results.

5. What issues occurred when multiple workers wrote to shared memory simultaneously?
- When there was no synchronization, the shared memory sometimes produced incorrect results. Multiple workers tried to update the shared list at the same time, which caused a race condition. Due to this conflict, some entries disappeared or were not saved correctly. There were tests where several orders were processed, but not all of them appeared in the final output. This showed the importance of controlling access to shared resources in parallel systems.

How did you ensure consistent results when using multiple processes?
- I solved the synchronization problem by using multiprocessing.Lock(). The lock was applied around the part where workers added data into the shared list. Using with lock: ensured that only one process could update the memory at a time, while the others waited for their turn. I initialized the lock together with the shared memory setup before the master and workers started running separately. After adding the lock, the output became stable and all processed orders were recorded correctly every run.

-----------------------------------------------------------------------------------------------------------------------------------------------
Anne Margarette G. Daniel

Reflection Questions - Distributed Order Processing

1. How did you distribute orders among worker processes?

The orders were distributed by the master process using round-robin assignment. The master first created around 5 to 8 orders with IDs and item names, then it sent them one by one to the workers. Each worker received different orders depending on their rank. This helped divide the tasks more equally between the worker processes and made sure every worker had something to process.

2. What happens if there are more orders than workers?

If there are more orders than workers, then some workers will handle more than one order. The master just keeps sending orders in sequence until all orders are assigned. Because of this, workers may process multiple tasks while others finish earlier, but overall the work is still balanced between all workers in the system.

3. How did processing delays affect the order completion?

The processing delays changed the order of completion because every worker had a random waiting time using time.sleep(). Some workers finished faster while others took longer, so the completed orders were not always in the same order they were assigned. This showed that the workers were processing independently and running at the same time.

4. How did you implement shared memory, and where was it initialized?

Shared memory was implemented using Manager().list() which created a shared list called shared_orders. It was initialized in the main part of the program before the master and worker processes started running separately. Workers added their completed orders into this shared list, and later the master process collected and printed all the results from it.

5. What issues occurred when multiple workers wrote to shared memory simultaneously?

When multiple workers wrote to the shared memory at the same time without synchronization, some problems could happen. The data inside the shared list could become inconsistent, missing, or mixed because many workers were trying to update it together. This is called a race condition and it can make the final output unreliable sometimes.

6. How did you ensure consistent results when using multiple processes?

To make the results consistent, a Lock() was used before workers wrote to the shared list. The lock allowed only one worker at a time to access the critical section where the data was being added. Because of this, the shared memory stayed organized and complete, and the master was able to print the correct final list of processed orders.

-----------------------------------------------------------------------------------------------------------------------------------------------

**CS3C · Labial Jay Mark S.**
## Reflection Questions

---

## Q1. How did you distribute orders among worker processes?

For distributing orders, I relied on a round-robin strategy where the master process (rank 0) took charge of generating between five and eight orders and then systematically parceled them out to each worker in turn. The assignment cycled through the workers repeatedly until all orders were handed off, so if there were four workers and eight orders, each worker ended up with exactly two. I used comm.send() to transmit both the count of incoming orders and the order data itself, while each worker called comm.recv() with the appropriate source and tag to receive its share. This approach kept the workload balanced and made the communication pattern predictable and easy to debug.

---

## Q2. What happens if there are more orders than workers?

When orders outnumber workers, the round-robin loop simply continues cycling back to the first worker after reaching the last one, so the extra orders get stacked onto workers that have already received at least one assignment. In a scenario with four workers and nine orders, the first worker would handle orders one, five, and nine, while the remaining workers each handle two. No orders fall through the cracks because the master process tracks every assignment before signaling workers to begin processing. The only trade-off is that some workers carry a slightly heavier load than others, but since processing happens in parallel, this rarely causes a significant bottleneck.

---

## Q3. How did processing delays affect the order completion?

Each worker simulated variable processing time using time.sleep() with a randomly chosen duration, which meant that the time any given order took to finish was unpredictable at runtime. Because MPI spawns each worker as a fully independent process, none of them block or wait on the others, so they all churn through their assigned orders concurrently. The side effect was that the completion sequence rarely matched the original submission order — an order sent later could easily finish before one sent earlier if its worker happened to draw a shorter sleep duration. Watching that unfold in the terminal output was actually a helpful reminder of why distributed systems need explicit coordination rather than assumptions about ordering.

---

## Q4. How did you implement shared memory, and where was it initialized?

I used multiprocessing.Manager().list() to create a shared list that all MPI processes could read from and write to through a managed proxy object. The Manager and its list were both instantiated near the top of the script, before any branching on the rank value, so the shared_orders reference was available to the master and every worker from the moment the program started. After finishing each order, a worker would append a dictionary to the list containing the order ID, item name, processing duration, the worker's rank, and a completion status. Once all workers finished, the master simply iterated over shared_orders to produce the final summary printout.

---

## Q5. What issues occurred when multiple workers wrote to shared memory simultaneously?

Running the program without any locking mechanism quickly revealed that the shared list was unreliable under concurrent writes. Multiple workers would attempt to append their result dictionaries at nearly the same moment, and because the Manager proxy serializes operations internally but doesn't prevent interleaving at a higher level, some appends were effectively lost or corrupted. The symptom was straightforward: the final summary would show fewer completed orders than were actually processed, and the count varied unpredictably from run to run. It was a concrete demonstration of why race conditions are so difficult to catch through testing alone — the bug only surfaced sometimes, depending on timing.

---

## Q6. How did you ensure consistent results when using multiple processes?

The fix was to introduce a multiprocessing.Lock() and require every worker to acquire it before appending to shared_orders. Wrapping the append call in a with lock: block guaranteed that only one worker at a time could modify the list, turning what had been a chaotic free-for-all into a strictly sequential series of writes. Like the Manager, the lock was created before the rank-based branching so that all processes shared the exact same lock object. After making that change, the output became fully deterministic — every run showed the correct number of completed orders, and the race condition never resurfaced.

-----------------------------------------------------------------------------------------------------------------------------------------------
Jim Francis C. Margaja

Reflection
Distributed Order Processing System

1.  How did you distribute orders among worker processes?

The master process (rank 0) handles order generation and distribution. It creates a batch of 5 to 8 orders at random and distributes them to workers using a round-robin pattern — each order is assigned to a worker based on its index modulo the number of workers, then incremented by 1 to skip rank 0. Before sending the actual orders, the master first sends each worker the count of how many orders to expect via comm.send() on tag 10, then sends the order dictionaries one by one on tag 11. Workers receive them using comm.recv() with the matching source and tags. This approach keeps the distribution predictable and ensures no worker is left idle while others are overloaded.

2.  What happens if there are more orders than workers?

The round-robin logic naturally handles this without any extra condition. When the number of orders exceeds the number of workers, the modulo operation simply wraps the index back to the beginning, so excess orders are reassigned starting from Worker 1 again. For example, with 3 workers and 8 orders, Workers 1, 2, and 3 each get 2 or 3 orders depending on the total. The master sends each worker its count upfront, so each worker knows exactly how many recv() calls to make. No order goes unprocessed, and the master waits for all workers to confirm completion before printing the final summary.

3.  How did processing delays affect the order completion?

Each worker simulates a processing delay using time.sleep() with a random duration between 0.5 and 2.0 seconds. Since workers run as separate MPI processes, they execute independently and in parallel — one worker's sleep does not block another. The result is that orders finish out of sequence. A worker that received its order later but drew a shorter delay will still complete before a worker that started earlier with a longer delay. This is visible in the output where completed entries appear in a non-sequential order. The master accounts for this by sorting the final shared_orders list by order ID before printing, so the summary always looks clean even when the execution was not.

4.  How did you implement shared memory, and where was it initialized?

Shared memory is implemented using multiprocessing.Manager().list(), which creates a list managed by a background server process that all MPI ranks can access. The key detail is where it is initialized: both the manager and the lock are set up before the if rank == 0 branch, which means every process — master and workers alike — gets a reference to the same shared list and the same lock from the start. After a worker finishes processing an order, it builds a result dictionary and appends it to shared_orders inside a with lock: block. Once all workers are done, the master reads from that list to produce the final report.

5.  What issues occurred when multiple workers wrote to shared memory simultaneously?

Without the lock, multiple workers can attempt to append to the shared list at the same time. Because Manager().list() operations are not atomic by default across processes, concurrent writes can interfere with each other — some entries get lost or the internal state of the list becomes inconsistent. This shows up as a mismatch between the number of orders sent and the number of entries in the final output. The problem is not always reproducible, which actually makes it harder to catch, since the race condition only manifests when two workers collide at the exact same moment.

6.  How did you ensure consistent results when using multiple processes?
A multiprocessing.Lock() is used to serialize access to the shared list. The append operation is wrapped in a with lock: block, which acquires the lock before writing and releases it automatically afterward. If a second worker tries to enter the block while the lock is held, it blocks until the first one exits. Because the lock is initialized before the master-worker branch, all processes reference the same lock object. This guarantees that writes to shared_orders happen one at a time, regardless of how many workers are running or how their delays overlap, and the final count always matches the number of orders that were distributed.
