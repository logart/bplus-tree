NodeManager
==================
Pros:
- Copy-on-Write versioning enables safe concurrent reads and writes without explicit locks on data structures.
- The freeCandidates list ensures freed pages are recycled, minimizing fragmentation and I/O pressure.
- VersionedRefCounter helps avoid race conditions during version switching and cleanup.
- VersionedRefCounter allows for deferred cleanup when no active readers of the version exist.
Cons:
- If write fails due to concurrency allocated page is not freed, this is not hard to implement, I just noticed it after spending all the available time.

Pages
===================
Pages are organized in a compact binary layout optimized for:
- Predictable memory access: Binary search is applied over sorted entries (searchKeyIdx), reducing read complexity from O(n) to O(log n).
- Data is read directly from ByteBuffer, reducing GC load and object churn.

Cons:
- Right sibling pointers in leaf pages are not implemented (needed for efficient range scans).
- Slot table grows from the end of the file. However, this is a common practice, this could slow down reads and cause issue with cache invalidation. This is fixed by manually loading buffer after pageRead. If system meory would be enough, this should work relatively fast.

Page structure is optimized for modern chips cache which is usually 64 bytes.
False sharing risk avoided, page is read only so no parallel thread could modify and read it.

Some drawback that requires additional performance testing which is out of scope of this project:
Bitfield penalties:	Modern compilers sometimes handle bitfields suboptimally. If access to bits spans byte boundaries, it can cause unaligned reads.

Page Layout (4096 bytes):

Header:
| Field              | Size        | Description                                 |
|--------------------|-------------|---------------------------------------------|
| Page metadata:     | 1 byte      |                                             |
| Page Type          | 1 bit       | Leaf or internal                            |
| Full flag          | 1 bit       | Indicates if the page is full               |
| Is deleted         | 1 bit       | Indicates if the page is deleted            |
| Padding (flags)    | 5 bits      | Reserved for future use                     |
| Padding            | 7 bytes     | Padding to align to 8 bytes                 |
|--------------------|-------------|---------------------------------------------|
| Page ID            | 8 bytes     | This page's ID                              |
|--------------------|-------------|---------------------------------------------|
| Number of entries  | 2 bytes     | Slot count                                  |
| Free space offset  | 2 bytes     | Start of free space                         |
| Padding            | 4 bytes     | Padding to align to 8 bytes                 |
|--------------------|-------------|---------------------------------------------|
| Right sibling ptr  | 8 bytes     | Only for leaf pages — not yet implemented   |
|--------------------|-------------|---------------------------------------------|

Left sibling ptr	8 bytes	Only for internal pages 

Slot table:
- Internal: [offset_to_payload, right_ptr] per entry
- Leaf: [offset_to_payload] per entry

- Payload Area (grows downward from end):
- [key_len][key] entries in internal
- [key_len][key][val_len][val] entries in leaf

PageManager
===================
Pros:
- Safe reuse of freed pages using freePagesIds
- Deleted pages are marked and tracked explicitly using an in-page metadata flag (IS_DELETED) and requeued in freePagesIds after a load from disk.
Cons:
- I was not able to test mmap crash protection correctly, event when I don't use close/force I still get correct results. This happens because OS flushes memory mapped buffer in the background.

PutHandler
===================
Pros:
- Reduce complexity by separating split and put logic from the main B+Tree logic
- Recursively finding the correct insertion location
- Proactive node splitting before overflow occurs
- Allocating new nodes via NodeManager to maintain immutability of prior versions
- Tracking old node IDs for cleanup
Cons:
- Lack of Write Batching
- Read-Modify-Write Overhead

VersuonedRefCounter
===================
Pros:
- Reference counts for each version
- Safe version advancement, ensuring readers and writers don’t interfere
- Tracking which versions are safe to clean up
