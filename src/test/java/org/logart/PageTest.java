package org.logart;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logart.page.mmap.InternalPage;
import org.logart.page.mmap.LeafPage;
import org.logart.page.Page;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

public class PageTest {

    private static final int THREADS = 8;
    private static final int ENTRIES_PER_THREAD = 80;
    private static final int PAGE_SIZE = 4096;

    @TempDir
    static Path tempDir;

    @Test
    void shouldReadLeafFlag() throws Exception {
        assertTrue(LeafPage.newPage(0, ByteBuffer.allocate(PAGE_SIZE)).isLeaf());
        assertFalse(InternalPage.newPage(0, ByteBuffer.allocate(PAGE_SIZE)).isLeaf());
    }

    @Test
    void shouldMarkPageAsFull() throws Exception {
        Page page = LeafPage.newPage(0, ByteBuffer.allocate(PAGE_SIZE));
        while (page.put("key".getBytes(), "value".getBytes())) {
            // Fill the page
        }
        assertTrue(page.isAlmostFull(0));
    }

    @Test
    void shouldReadChildId() throws Exception {
        Page page = InternalPage.newPage(0, ByteBuffer.allocate(PAGE_SIZE));
        page.addChild("key10".getBytes(), 1, 2);
        assertEquals(1L, page.getChild("key09".getBytes()), "Child ID should match the one added");
        assertEquals(2L, page.getChild("key10".getBytes()), "Child ID should match the one added");
        assertEquals(2L, page.getChild("key11".getBytes()), "Child ID should match the one added");
    }

    @Disabled // page is not threadsafe, this should not work
    @Test
    void stressTestMultiThreadedWritesAndReads() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(THREADS)) {
            List<Future<TestResult>> futures = new ArrayList<>();

            for (int i = 0; i < THREADS; i++) {
                int threadId = i;
                futures.add(executor.submit(() -> runThreadTest(threadId)));
            }

            for (Future<TestResult> future : futures) {
                TestResult result = future.get();
                assertTrue(result.passed, "Thread " + result.threadId + " failed at entry " +
                        result.failedAtIndex + ": " + result.message);
            }

            executor.shutdown();
        }
    }

    private TestResult runThreadTest(int threadId) {
        Map<String, String> expected = new TreeMap<>();
        Path filePath = tempDir.resolve("page_" + threadId + ".dat");

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            MappedByteBuffer mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);

            Page page = LeafPage.newPage(threadId, mmap);
            for (int i = 0; i < ENTRIES_PER_THREAD; i++) {
                String key = "k-" + threadId + "-" + i;
                String value = UUID.randomUUID().toString();
                if (!page.put(key.getBytes(), value.getBytes())) {
                    return new TestResult(threadId, false, i, "Page out of space before expected", expected.size());
                }
                expected.put(key, value);
            }

            mmap.force();
        } catch (Exception e) {
            return new TestResult(threadId, false, -1, "Write error: " + e.getMessage(), 0);
        }

        // Read & verify
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            MappedByteBuffer mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, PAGE_SIZE);
            Page readPage = LeafPage.newPage(0, mmap);
            int count = readPage.getEntryCount();

            if (count != expected.size()) {
                return new TestResult(threadId, false, -1,
                        "Mismatch entry count: expected " + expected.size() + ", found " + count, count);
            }

            for (int i = 0; i < count; i++) {
                byte[][] kv = readPage.getEntry(i);
                if (kv == null || kv.length != 2) {
                    return new TestResult(threadId, false, i, "Malformed key-value entry", count);
                }
                String key = new String(kv[0]);
                String value = new String(kv[1]);

                String expectedVal = expected.get(key);
                if (expectedVal == null) {
                    return new TestResult(threadId, false, i, "Unexpected key: " + key, count);
                }
                if (!expectedVal.equals(value)) {
                    return new TestResult(threadId, false, i,
                            "Value mismatch for key=" + key + ": expected=" + expectedVal + ", got=" + value, count);
                }
            }

        } catch (Exception e) {
            return new TestResult(threadId, false, -1, "Read error: " + e.getMessage(), 0);
        }

        return new TestResult(threadId, true, -1, null, expected.size());
    }

    static class TestResult {
        final int threadId;
        final boolean passed;
        final int failedAtIndex;
        final String message;
        final int entries;

        TestResult(int threadId, boolean passed, int failedAtIndex, String message, int entries) {
            this.threadId = threadId;
            this.passed = passed;
            this.failedAtIndex = failedAtIndex;
            this.message = message;
            this.entries = entries;
        }
    }
}
