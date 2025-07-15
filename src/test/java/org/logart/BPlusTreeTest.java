package org.logart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.logart.node.PageBasedNodeManager;
import org.logart.page.MMAPBasedPageManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Disabled // disk implementation is not ready yet
public class BPlusTreeTest {

    private BPlusTree tree;
    private Path tempFile;

    @BeforeEach
    void setup() throws Exception {
        tempFile = Files.createTempFile("bplustree-test", ".db");
        tree = new DefaultBPlusTree(new PageBasedNodeManager(new MMAPBasedPageManager(tempFile.toFile(), 4096))); // assuming 4KB pages
    }

    @AfterEach
    void teardown() throws Exception {
        tree.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    void testInsertAndGetSingleKeyValue() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();
        tree.put(key, value);
        assertArrayEquals(value, tree.get(key));
    }

    @Test
    void testInsertMultipleAndGetCorrectValues() {
        for (int i = 0; i < 100; i++) {
            tree.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }
        for (int i = 0; i < 100; i++) {
            assertArrayEquals(("value" + i).getBytes(), tree.get(("key" + i).getBytes()),
                    "Value for key" + i + " should match");
        }
    }

    @Test
    void testOverwriteKeyValue() {
        byte[] key = "dupkey".getBytes();
        tree.put(key, "initial".getBytes());
        tree.put(key, "updated".getBytes());
        assertArrayEquals("updated".getBytes(), tree.get(key));
    }

    @Test
    void testGetMissingKeyReturnsNull() throws Exception {
        assertNull(tree.get("notfound".getBytes()));
    }

    @Test
    void testInsertEmptyKeyAndValue() throws Exception {
        byte[] key = new byte[0];
        byte[] value = new byte[0];
        tree.put(key, value);
        assertArrayEquals(value, tree.get(key));
    }

    @Test
    void testConcurrentPutsAndGets() throws Exception {
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            tasks.add(() -> {
                for (int i = 0; i < 100; i++) {
                    byte[] key = ("key-" + threadId + "-" + i).getBytes();
                    byte[] value = ("value-" + threadId + "-" + i).getBytes();
                    tree.put(key, value);
                    assertArrayEquals(value, tree.get(key));
                }
                return null;
            });
        }
        executor.invokeAll(tasks);
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void testInsertMaxSizeKeyValue() throws Exception {
        byte[] key = new byte[1024];  // 1KB key
        byte[] value = new byte[2048];  // 2KB value
        new Random().nextBytes(key);
        new Random().nextBytes(value);
        tree.put(key, value);
        assertArrayEquals(value, tree.get(key));
    }
}
