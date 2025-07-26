# Provide the pros and cons of using MMAP as a cache for the disk data.


Using mmap (memory-mapped I/O) to cache disk data is a powerful technique that offers both performance and simplicity — but it's not a silver bullet.
Here's a clear breakdown of the pros and cons when using mmap as a disk-backed cache or persistent store layer, such as in a B+ tree or log-structured system.

Pros of Using mmap as a Cache
1. Zero-Copy Reads/Writes
   OS loads pages from disk into memory transparently.
   Your application reads/writes directly to memory, avoiding extra buffer copies.
   Performance win: great for read-heavy and memory-aligned workloads.

2. Automatic Page Caching
   The OS manages which pages are in memory via the page cache.
   You don't have to implement your own LRU/clock cache (like in RocksDB or LevelDB).
   Simpler design, fewer bugs, less memory management overhead.

3. On-Demand Loading
   Pages are only faulted into memory when accessed.
   You can mmap a huge file, but only touch parts of it as needed.
   Efficient for sparse access patterns.

4. Built-in Persistence (via msync or force)
   Changes are persisted to disk via flush (MappedByteBuffer.force() in Java).
   The memory map can act like a durable backing store.
   Makes writing persistent structures (B+ trees, WALs) easier.

6. No Context Switching or Syscalls for Reads
   Reads (and potentially writes) avoid expensive syscall overhead.
   Excellent throughput for random reads on SSDs.

Cons of Using mmap as a Cache
1. Poor Control Over Eviction
   The OS decides what to keep in RAM, not your application.
   Pages you care about may get evicted while cold pages stay cached.
   You lose fine-tuned control compared to a custom page cache.

2. Unpredictable Latency (Page Faults)
   This one I've experienced in tests, before I added a manual load of buffer on page read.
   If a page isn't in RAM, a major page fault will occur and block the thread.
   This can cause unpredictable latency spikes, especially on HDDs.
   Dangerous for real-time or low-latency applications.

3. Limited Write Safety
   Writes to mapped memory are not guaranteed to be persisted unless explicitly flushed.
   If the app or OS crashes before msync(), data may be lost or corrupted.
   You must handle flushing explicitly or risk silent data loss.

4. Harder Portability / Debugging
   mmap behavior can vary subtly between OSes (e.g., Windows vs. Linux).
   Bugs (like segmentation faults) can be harder to trace.
   More complex debugging than heap memory.

5. Resource Limits
   OS limits the number of memory-mapped files and total virtual memory space.
   Large-scale use may require careful tuning (ulimit, vm.max_map_count).

When MMAP is a Great Fit
    You’re building a read-heavy system (e.g., analytics, key-value store).
    Your access patterns are sequential or locality-rich.
    You need persistence with minimal syscall overhead.
    You want to minimize RAM usage and complexity.

When MMAP is Risky
    You need low-latency, real-time access with hard bounds.
    You require precise eviction control (e.g., hot LRU working set).
    You want cross-platform, crash-safe guarantees out of the box.

# Outline changes, but do not implement them, that need to be made for a given implementation to support a serializable level of isolation for multiple changes at once.

Required Changes for Serializable Isolation

To support serializable isolation (i.e., atomic multi-step transactions), the current implementation would require the following non-invasive architectural changes:
1. Transactional Context
   Introduce a TransactionContext class that tracks:
        Modified nodes.
        Temporary node allocations.
        Uncommitted root. (This is already done, should be just extended for more than one change at once)

```java
class TransactionContext {
   Map<Long, BTreeNode> modifiedNodes;
   BTreeNode uncommittedRoot;
}
```

2. Atomic Root Swap(this is done already)
   Only commit transaction after:
        All modified nodes are written.
        New root is persisted in an atomic operation.

3. Isolation Between Transactions
   Each transaction should operate on its own version.
   Readers and Writers use lockVersion() to make sure their pages remain accessible even if new root is commited.
   Writers use advanceVersion() at commit to publish changes.
   Since new pages are only written after commit (there is no access to them from previous root),
   this guarantees transaction isolation.

4. Transactional Memory Management(this is already done)
   Delay freeing pages until:
        The transaction is committed.
        No older readers are using them.

5. Optimistic Concurrency Control(this is already implemented)
   On advanceVersion(), validates that the current root/version hasn’t changed since the transaction started.
   Retry if conflicting concurrent commit occurred.
   However, while writing this document, I realized that if conflict happens, I am not deallocating pages
   allocated during the transaction, so this should be fixed before implementing transaction support.

These changes preserve existing structure while extending it into full MVCC with serializable isolation — a requirement for transactional key-value stores or databases.