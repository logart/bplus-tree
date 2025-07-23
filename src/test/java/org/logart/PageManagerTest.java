package org.logart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.logart.page.mmap.MMAPBasedPageManager;
import org.logart.page.Page;
import org.logart.page.PageManager;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.logart.page.mmap.LeafPage.FREE_SPACE_OFFSET;
import static org.logart.page.mmap.PageFactory.LEAF_FLAG;

public class PageManagerTest {

    private static final int PAGE_SIZE = 4096;
    private File tempFile;
    private PageManager pageManager;

    @BeforeEach
    void setUp() throws Exception {
        tempFile = File.createTempFile("pagemanager-test-" + System.nanoTime(), ".db");
        pageManager = new MMAPBasedPageManager(tempFile, PAGE_SIZE);
    }

    @AfterEach
    void tearDown() throws Exception {
        pageManager.close();
        if (tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    void testConcurrentPageAllocation() throws Exception {
        int threadCount = 16;
        int pagesPerThread = 100;
        Set<Long> allocatedPages = ConcurrentHashMap.newKeySet();
        testConcurrently(threadCount, new TestCore() {
            @Override
            void run(long threadId, Queue<AssertionError> errors) throws Exception {
                for (int i = 0; i < pagesPerThread; i++) {
                    Page page = pageManager.allocatePage();
                    assertTrue(allocatedPages.add(page.pageId()), "Duplicate pageId allocated: " + page.pageId());
                }
            }
        });
        assertEquals(threadCount * pagesPerThread, allocatedPages.size(),
                "Total allocated pages count mismatch");
    }

    @Test
    void testConcurrentReadWriteDistinctPages() throws Exception {
        int threadCount = 8;
        int pagesPerThread = 50;
        // pre allocate pages
        for (int i = 0; i < threadCount * pagesPerThread; i++) {
            pageManager.allocateLeafPage();
        }
        testConcurrently(threadCount, new TestCore() {
            @Override
            void run(long threadId, Queue<AssertionError> errors) throws Exception {
                for (int i = 0; i < pagesPerThread; i++) {
                    long pageId = threadId * pagesPerThread + i;
                    Page writePage = pageManager.readPage(pageId);
                    byte val = (byte) (threadId + 1);
                    fill(writePage, val);
                    pageManager.writePage(pageId, writePage);

                    Page readPage = pageManager.readPage(pageId);
                    validatePageContent(readPage, val, pageId);
                }
            }
        });
    }

    @Test
    void testFreePage() {
        Page page = pageManager.allocatePage();
        assertTrue(page.pageId() >= 0, "Allocated invalid page id: " + page.pageId());

        // Free the allocated page
        pageManager.freePage(page.pageId());

        // Try to allocate again, should reuse the freed page
        Page reusedPage = pageManager.allocatePage();
        assertEquals(page.pageId(), reusedPage.pageId(), "Reused page id does not match freed page id");
    }

    @Test
    void testConcurrentFreeAndAllocatePages() throws Exception {
        int threadCount = 10;
        int pagesPerThread = 20;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            Set<Page> allocatedPages = ConcurrentHashMap.newKeySet();

            // Pre-allocate pages
            for (int i = 0; i < threadCount * pagesPerThread; i++) {
                allocatedPages.add(pageManager.allocatePage());
            }

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount * 2);

            // Free pages concurrently
            for (Page page : allocatedPages) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        pageManager.freePage(page.pageId());
                    } catch (Exception e) {
                        fail("Exception in free thread: " + e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Concurrently, allocate pages while freeing is ongoing
            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < pagesPerThread; i++) {
                            Page page = pageManager.allocatePage();
                            assertTrue(page.pageId() >= 0, "Allocated invalid page id: " + page.pageId());
                        }
                    } catch (Exception e) {
                        fail("Exception in allocate thread: " + e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await();
            executor.shutdown();
        }
    }

    @Test
    void testStressConcurrentAllocWriteRead() throws Exception {
        int threadCount = 20;
        int iterations = 100;
        testConcurrently(threadCount, new TestCore() {
            @Override
            void run(long threadId, Queue<AssertionError> errors) throws Exception {
                for (int i = 0; i < iterations; i++) {
                    Page page = pageManager.allocateLeafPage();
                    Page writePage = pageManager.readPage(page.pageId());
                    byte val = (byte) ((threadId + i) % 256);
                    fill(writePage, val);
                    pageManager.writePage(page.pageId(), writePage);

                    Page readPage = pageManager.readPage(page.pageId());
                    validatePageContent(readPage, val, page.pageId());
                    pageManager.freePage(page.pageId());
                }
            }
        });
    }

    //page is not thread-safe anymore, so concurrent writes to the same page can lead to data corruption.
//    @Test
//    void testConcurrentWriteSamePage() throws Exception {
//        int threadCount = 20;
//        // pre allocate concurrent page
//        final Page page = pageManager.allocatePage();
//        testConcurrently(threadCount, new TestCore() {
//            @Override
//            void run(long threadId, Queue<AssertionError> errors) throws Exception {
//                Page writePage = pageManager.readPage(page.pageId());
//                byte val = (byte) ((threadId) % 256);
//                int keysWritten = fill(writePage, val);
//                pageManager.writePage(page.pageId(), writePage);
//
//                Page readPage = pageManager.readPage(page.pageId());
//                int keysRead = 0;
//                while (keysRead <= readPage.getEntryCount()) {
//                    byte[][] currentKey = readPage.getEntry(keysRead);
//                    assertEquals(val, currentKey[0][0], "Key data mismatch on page " + page.pageId());
//                    assertEquals(val, currentKey[1][0], "Value data mismatch on page " + page.pageId());
//                    keysRead++;
//                }
//                assertEquals(keysWritten, keysRead, "Page " + page.pageId() + " does not contain enough data");
//            }
//        });
//    }

    @Test
    void testConcurrentAllocateAndWriteRace() throws Exception {
        int threadCount = 10;
        int iterations = 1000;

        testConcurrently(threadCount, new TestCore() {
            @Override
            void run(long threadId, Queue<AssertionError> errors) {
                for (int i = 0; i < iterations; i++) {
                    Page page = pageManager.allocateLeafPage();
                    fill(page, (byte) 0x5A);
                    pageManager.writePage(page.pageId(), page);
                }
            }
        });
        // Now, verify file size is a multiple of page size
        long expectedSize = threadCount * iterations * PAGE_SIZE;
        assertEquals(threadCount * iterations, ((MMAPBasedPageManager) pageManager).getAllocatedPageCount(),
                "Total allocated pages count mismatch after concurrent allocation");
        assertEquals(expectedSize, new File(tempFile.getAbsolutePath()).length(),
                "File size mismatch – indicates race in allocation or write");
    }

    // page is not thread-safe anymore, so concurrent writes to the same page can lead to data corruption.
//    @Test
//    void testConcurrentWritesCorruptPages() throws Exception {
//        final int pageCount = 10;
//        final Page[] pages = new Page[pageCount];
//        for (int i = 0; i < pageCount; i++) {
//            pages[i] = pageManager.allocatePage();
//        }
//
//        testConcurrently(20, new TestCore() {
//            @Override
//            void run(long threadId, Queue<AssertionError> errors) throws Exception {
//                for (int i = 0; i < 100; i++) {
//                    Page page = pages[i % pageCount];
//                    byte val = (byte) (threadId & 0xFF);
//                    fill(page, val);
//                    pageManager.writePage(page.pageId(), page);
//
//                    // Optional: small delay to increase collision chances
//                    Thread.sleep(1);
//
//                    Page readPage = pageManager.readPage(page.pageId());
//                    byte[][] first = readPage.getEntry(0);
//                    for (int j = 1; j < readPage.getEntryCount(); j++) {
//                        byte[][] entry = readPage.getEntry(j);
//                        if (Arrays.equals(entry[0], first[0])) {
//                            errors.add(new AssertionError("Key data corruption detected on page " + page.pageId()));
//                        }
//                        if (Arrays.equals(entry[1], first[1])) {
//                            errors.add(new AssertionError("Value data corruption detected on page " + page.pageId()));
//                        }
//                    }
//                }
//            }
//        });
//    }

    @Disabled // todo
    @Test
    void testRemovePageLockAfterDeletionOfThePage() throws Exception {
        final Page page = pageManager.allocatePage();
        pageManager.freePage(page.pageId());
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(PageManager.class, MethodHandles.lookup());
        VarHandle pageLocksHandle = lookup.findVarHandle(PageManager.class, "pageLocks", ConcurrentMap.class);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, ReentrantReadWriteLock> pageLocks =
                (ConcurrentMap<Long, ReentrantReadWriteLock>) pageLocksHandle.get(pageManager);

        assertNull(pageLocks.get(page.pageId()));
    }

    @Test
    void allocateShouldWriteDefaultPageHeader() throws Exception {
        final Page page = pageManager.allocateLeafPage();
        ByteBuffer pageContent = page.buffer();
        assertEquals(LEAF_FLAG, LEAF_FLAG & pageContent.get(0), "Page should be leaf");
        assertEquals(PAGE_SIZE, pageContent.getShort(FREE_SPACE_OFFSET), "Free space offset should be initialized to header size");
    }

    void testConcurrently(int threadCount, TestCore test) throws Exception {
        Queue<AssertionError> errors = new ConcurrentLinkedQueue<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        test.run(threadId, errors);
                    } catch (InterruptedException e) {
                        errors.add(new AssertionError("Thread interrupted: " + e));
                    } catch (Exception e) {
                        e.printStackTrace();
                        errors.add(new AssertionError("Unexpected exception: " + e));
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await();
            executor.shutdown();
        }
        if (!errors.isEmpty()) {
            throw errors.poll();
        }
    }

    private int fill(Page page, byte val) {
        int keys = 0;
        byte[] key = new byte[1];
        key[0] = val;
        byte[] value = new byte[1];
        value[0] = val;
        while (!page.isAlmostFull(key.length + value.length)) {
            page.put(key, value);
            keys++;
        }
        return keys;
    }

    private static void validatePageContent(Page readPage, byte val, long pageId) {
        for (int j = 0; j < readPage.getEntryCount(); j++) {
            byte[][] entry = readPage.getEntry(j);
            assertEquals(val, entry[0][0], "Key data mismatch on page " + pageId);
            assertEquals(val, entry[1][0], "Value data mismatch on page " + pageId);
        }
    }

    private abstract static class TestCore {
        abstract void run(long threadId, Queue<AssertionError> errors) throws Exception;
    }
}
