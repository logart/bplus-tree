package org.logart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PageTest {

    private static final int THREADS = 8;
    private static final int ENTRIES_PER_THREAD = 80;
    private static final int PAGE_SIZE = 4096;

    @TempDir
    static Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReadLeafFlag(boolean flag) throws Exception {
        assertEquals(flag, Page.newPage(0, flag, ByteBuffer.allocate(PAGE_SIZE)).isLeaf());
    }

    @Test
    void shouldMarkPageAsFull() throws Exception {
        Page page = Page.newPage(0, true, ByteBuffer.allocate(PAGE_SIZE));
        while (page.put("key".getBytes(), "value".getBytes())) {
            // Fill the page
        }
        assertTrue(page.isFull());
    }

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

            Page page = Page.newPage(threadId, true, mmap);
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
            Page readPage = new Page(mmap);
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
