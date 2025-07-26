package org.logart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.mmap.MMAPBasedPageManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled // only run it for final tests
public class BPlusTreeLongTest {
    private BPlusTree tree;
    private Path tempFile;

    @BeforeEach
    void setup() throws Exception {
        tempFile = Files.createTempFile("bplustree-test", ".db");
        tree = new DefaultBPlusTree(new DefaultNodeManager(new MMAPBasedPageManager(tempFile.toFile(), 4096, false))); // assuming 4KB pages
    }

    @AfterEach
    void teardown() throws Exception {
        tree.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    public void shouldKeepALotOfKeys() {
        for (int i = 0; i < 100_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            tree.put(key, value);
        }
        for (int i = 0; i < 100_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            assertArrayEquals(value, tree.get(key), "Value for key-" + i + " should match");
        }
    }

    @Test
    public void shouldKeepALotOfKeysConcurrent() {
        ConcurrentMap<byte[], List<byte[]>> reference = new ConcurrentHashMap<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(16)) {
            executor.submit(() -> {
                for (int i = 0; i < 100_000; i++) {
                    byte[] key = ("key-" + i).getBytes();
                    byte[] value = ("value-" + i).getBytes();
                    tree.put(key, value);
                    reference.compute(key, (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        v.add(value);
                        return v;
                    });
                }
            });
            upper:
            for (int i = 0; i < 100_000; i++) {
                byte[] key = ("key-" + i).getBytes();
                byte[] value = ("value-" + i).getBytes();
                List<byte[]> potentialValues = reference.get(key);
                if (potentialValues == null || potentialValues.isEmpty()) {
                    throw new AssertionError("No values found for key-" + i);
                }
                byte[] storedValue = tree.get(key);
                for (byte[] potentialValue : potentialValues) {
                    if (Arrays.compare(potentialValue, storedValue) == 0) {
                        continue upper;
                    }
                    fail("Value for key-" + i + " should match one of the potential values");
                }
            }
        }
    }
}
