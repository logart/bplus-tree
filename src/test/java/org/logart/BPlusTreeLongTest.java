package org.logart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.mmap.MMAPBasedPageManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class BPlusTreeLongTest {
    private BPlusTree tree;
    private Path tempFile;

    @BeforeEach
    void setup() throws Exception {
        tempFile = Files.createTempFile("bplustree-test", ".db");
        tree = new DefaultBPlusTree(new DefaultNodeManager(new MMAPBasedPageManager(tempFile.toFile(), 4096))); // assuming 4KB pages
    }

    @AfterEach
    void teardown() throws Exception {
        tree.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    public void shouldKeepALotOfKeys() {
        for (int i = 0; i < 10_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            tree.put(key, value);
        }
        for (int i = 0; i < 10_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            assertArrayEquals(value, tree.get(key), "Value for key-" + i + " should match");
        }
    }

    @Test
    public void shouldKeepALotOfKeysConcurrentHighContention() throws InterruptedException {
        ConcurrentMap<String, List<String>> reference = new ConcurrentHashMap<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(16)) {
            CountDownLatch latch = new CountDownLatch(16);
            Queue<Exception> errors = new ConcurrentLinkedQueue<>();
            for (int t = 0; t < 16; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < 1_00; i++) {
                            String key = ("key-" + i);
                            String value = ("value-" + i);
                            tree.put(key.getBytes(), value.getBytes());
                            reference.compute(key, (k, v) -> {
                                if (v == null) {
                                    v = new ArrayList<>();
                                }
                                v.add(value);
                                return v;
                            });
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            // only read after writing is done
            latch.await();
            if (!errors.isEmpty()) {
                System.out.println("Errors occurred during concurrent writes: " + errors.poll());
            }
            upper:
            for (int i = 0; i < 1_00; i++) {
                String key = ("key-" + i);
                List<String> potentialValues = reference.get(key);
                if (potentialValues == null || potentialValues.isEmpty()) {
                    throw new AssertionError("No values found for key-" + i);
                }
                byte[] storedValue = tree.get(key.getBytes());
                for (String potentialValue : potentialValues) {
                    if (Arrays.compare(potentialValue.getBytes(), storedValue) == 0) {
                        continue upper;
                    }
                    fail("Value for key-" + i + " should match one of the potential values");
                }
            }
        }
    }

    @Test
    public void shouldKeepALotOfKeysConcurrent() throws InterruptedException {
        ConcurrentMap<String, List<String>> reference = new ConcurrentHashMap<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(16)) {
            CountDownLatch latch = new CountDownLatch(16);
            Queue<Exception> errors = new ConcurrentLinkedQueue<>();
            for (int t = 0; t < 16; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < 1_00; i++) {
                            String key = "key-" + Math.random() * 1_000_000_000;
                            String value = ("value-" + i);
                            tree.put(key.getBytes(), value.getBytes());
                            reference.compute(key, (k, v) -> {
                                if (v == null) {
                                    v = new ArrayList<>();
                                }
                                v.add(value);
                                return v;
                            });
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            // only read after writing is done
            latch.await();
            if (!errors.isEmpty()) {
                System.out.println("Errors occurred during concurrent writes: " + errors.poll());
            }
            upper:
            for (String key : reference.keySet()) {
                List<String> potentialValues = reference.get(key);
                if (potentialValues == null || potentialValues.isEmpty()) {
                    throw new AssertionError("No values found for key: " + key);
                }
                byte[] storedValue = tree.get(key.getBytes());
                for (String potentialValue : potentialValues) {
                    if (Arrays.compare(potentialValue.getBytes(), storedValue) == 0) {
                        continue upper;
                    }
                    fail("Value for key:" + key + " should match one of the potential values");
                }
            }
        }
    }
}
