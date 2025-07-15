package org.logart;

import org.junit.jupiter.api.Test;
import org.logart.node.MapBasedNodeManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeConcurrentTest {
    private final int NUM_THREADS = 8;
    private final int NUM_OPS = 1000;

    @Test
    public void testConcurrentPutAndGet() throws InterruptedException {
        try (ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS)) {
            ConcurrentMap<String, byte[]> expectedMap = new ConcurrentHashMap<>();

            BPlusTree tree = new DefaultBPlusTree(new MapBasedNodeManager());

            CountDownLatch latch = new CountDownLatch(NUM_THREADS);

            List<Throwable> errors = new ArrayList<>();
            for (int t = 0; t < NUM_THREADS; t++) {
                executor.submit(() -> {
                    try {
                        Random rand = new Random();
                        for (int i = 0; i < NUM_OPS; i++) {
                            String key = "key-" + rand.nextInt(500);
                            byte[] value = ("val-" + rand.nextInt(1000)).getBytes();

                            tree.put(key.getBytes(), value);
                            expectedMap.put(key, value);

                            // at this stage, the value could be overwritten already
                            // to make this check work we need to lock around put and get operations
                            byte[] retrieved = tree.get(key.getBytes());

                            assertTrue(retrieved == null || Arrays.equals(retrieved, expectedMap.get(key)));
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                    latch.countDown();
                });
            }

            latch.await();
            executor.shutdown();

            if (!errors.isEmpty()) {
                for (Throwable e : errors) {
                    e.printStackTrace();
                }
                fail("Some operations failed.", errors.get(0));
            }
            // Final consistency check
            for (String key : expectedMap.keySet()) {
                byte[] expectedVal = expectedMap.get(key);
                byte[] actualVal = tree.get(key.getBytes());
                assertNotNull(actualVal);
                assertArrayEquals(expectedVal, actualVal);
            }
        }
    }
}
