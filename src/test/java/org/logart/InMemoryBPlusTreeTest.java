package org.logart;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.memory.MapBasedPageManager;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryBPlusTreeTest {

    private BPlusTree tree;

    @BeforeEach
    void setUp() {
        tree = new DefaultBPlusTree(new DefaultNodeManager(new MapBasedPageManager()));
    }

    @Test
    void testSimplePutAndGet() {
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        tree.put(key, value);
        assertArrayEquals(value, tree.get(key));
    }

    @Test
    void testOverwriteValue() {
        byte[] key = "key".getBytes();
        byte[] value1 = "val1".getBytes();
        byte[] value2 = "val2".getBytes();

        tree.put(key, value1);
        assertArrayEquals(value1, tree.get(key));

        tree.put(key, value2);
        assertArrayEquals(value2, tree.get(key));
    }

    @Test
    void testGetUnknownKey() {
        assertNull(tree.get("unknown".getBytes()));
    }

    @Test
    void testBulkInsertAndReadMeaningfulKeys() {
        Map<String, String> reference = new HashMap<>();
        Random rnd = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            byte[] key = new byte[8];
            byte[] value = new byte[16];
            String keyBytes = randomReadableBytes("k", rnd, 8);
            System.arraycopy(keyBytes.getBytes(), 0, key, 0, Math.min(keyBytes.getBytes().length, key.length));
            String valueBytes = randomReadableBytes("v", rnd, 16);
            System.arraycopy(valueBytes.getBytes(), 0, value, 0, Math.min(valueBytes.getBytes().length, value.length));
            tree.put(key, value);
            reference.put(keyBytes, valueBytes);
            assertArrayEquals(value, tree.get(key));
        }

        for (Map.Entry<String, String> entry : reference.entrySet()) {
            byte[] receivedBytes = tree.get(entry.getKey().getBytes());
            assertNotNull(receivedBytes, "Value for key " + entry.getKey() + " should not be null");
            assertArrayEquals(entry.getValue().getBytes(), receivedBytes,
                    "Value for key " + entry.getKey() + " should match"
                            + " but was " + new String(receivedBytes)
                            + " expected " + entry.getValue()
            );
        }
    }

    private static String randomReadableBytes(String prefix, Random rnd, int limit) {
        byte[] bytes = (prefix + rnd.nextLong(9_000) + "           padding           ").getBytes();
        byte[] croppedBytes = new byte[limit];
        System.arraycopy(bytes, 0, croppedBytes, 0, limit);
        return new String(croppedBytes);
    }

    @Test
    void testBulkInsertAndRead() {
        Map<byte[], byte[]> reference = new HashMap<>();
        Random rnd = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            byte[] key = new byte[8];
            byte[] value = new byte[16];
            rnd.nextBytes(key);
            rnd.nextBytes(value);
            tree.put(key, value);
            reference.put(key, value);
        }

        for (Map.Entry<byte[], byte[]> entry : reference.entrySet()) {
            assertArrayEquals(entry.getValue(), tree.get(entry.getKey()));
        }
    }

    @Test
    void testSortedLeafOrder() {
        for (int i = 0; i < 1000; i++) {
            String s = String.format("%04d", i);
            tree.put(s.getBytes(), ("val" + s).getBytes());
        }

        List<byte[]> keysInOrder = tree.getAllKeysInOrder();
        for (int i = 1; i < keysInOrder.size(); i++) {
            assertTrue(compare(keysInOrder.get(i - 1), keysInOrder.get(i)) < 0);
        }
    }

    @Test
    void testVariableKeyAndValueSizes() {
        for (int i = 1; i <= 1024; i *= 2) {
            byte[] key = new byte[i];
            byte[] value = new byte[i * 2];
            Arrays.fill(key, (byte) i);
            Arrays.fill(value, (byte) (255 - i));
            tree.put(key, value);
            assertArrayEquals(value, tree.get(key));
        }
    }

    @Test
    void testDuplicateKeysInsertion() {
        byte[] key = "same-key".getBytes();
        for (int i = 0; i < 100; i++) {
            byte[] value = ("val" + i).getBytes();
            tree.put(key, value);
            assertArrayEquals(value, tree.get(key));
        }
    }

    @Test
    void testInsertInDescendingOrder() {
        for (int i = 1000; i >= 0; i--) {
            byte[] key = String.format("%04d", i).getBytes();
            byte[] value = ("val" + i).getBytes();
            tree.put(key, value);
        }

        for (int i = 0; i <= 1000; i++) {
            byte[] key = String.format("%04d", i).getBytes();
            byte[] value = ("val" + i).getBytes();
            assertArrayEquals(value, tree.get(key));
        }
    }

    // Utility method to compare byte arrays lexicographically
    private int compare(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }
}
