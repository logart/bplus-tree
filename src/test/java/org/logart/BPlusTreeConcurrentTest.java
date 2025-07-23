package org.logart;

import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.memory.MapBasedPageManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeConcurrentTest {
    private final int NUM_THREADS = 1;
    private final int NUM_OPS = 1000;

    @Test
    public void testConcurrentPutAndGet() throws InterruptedException {
        try (ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS)) {
            // since we put concurrently it could be already overwritten byt the next put,
            // so instead of checking equals, we have to check contains,
            // meaning that at least one value which is present in btree was added
            // at some point in the past
            ConcurrentMap<String, Set<byte[]>> expectedMultiMap = new ConcurrentHashMap<>();

            BPlusTree tree = new DefaultBPlusTree(new DefaultNodeManager(new MapBasedPageManager()));

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
                            expectedMultiMap.compute(key, (k, v) -> {
                                v = v == null ? ConcurrentHashMap.newKeySet() : v;
                                v.add(value);
                                return v;
                            });

                            // at this stage, the value could be overwritten already
                            // to make this check work we need to lock around put and get operations
                            byte[] retrieved = tree.get(key.getBytes());

                            assertTrue(retrieved == null || expectedMultiMap.get(key).contains(retrieved),
                                    "Value for key '" + new String(key) + "' should be one of the expected values. " +
                                            "Expected: " + expectedMultiMap.get(key).stream().map(String::new).toList()
                                            + ", but got: " + (retrieved == null ? "null" : new String(retrieved, StandardCharsets.UTF_8)))
                            ;
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
            for (String key : expectedMultiMap.keySet()) {
                Set<byte[]> possibleExpectedVal = expectedMultiMap.get(key);
                byte[] actualVal = tree.get(key.getBytes());
                assertNotNull(actualVal);
                assertTrue(possibleExpectedVal.contains(actualVal));
            }
        }
    }
}
