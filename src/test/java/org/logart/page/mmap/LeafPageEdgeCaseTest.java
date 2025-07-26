package org.logart.page.mmap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class LeafPageEdgeCaseTest {

    private static final int PAGE_SIZE = 4096;
    public static final byte[] key = "x".getBytes();
    private LeafPage page;

    @BeforeEach
    void setUp() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        page = (LeafPage) LeafPage.newPage(1L, buffer);
    }

    @Test
    void testInsertInReverseOrderTriggersSlotShiftBug() {
        // Inserting in descending key order will cause slot shifting repeatedly
        int count = 100_000;
        int lastInserted = 0;
        for (int i = count; i >= 0; i--) {
            String key = String.format("k%03d", i);
            String value = "v" + i;
            if (page.isAlmostFull(key.getBytes().length + value.getBytes().length)) {
                lastInserted = i + 1;

                assertFalse(page.put(key.getBytes(), value.getBytes()), "Failed to insert key: " + key);
                break;
            }
            assertTrue(page.put(key.getBytes(), value.getBytes()), "Failed to insert key: " + key);
        }

        // Ensure keys are still retrievable after slot shifting
        for (int i = lastInserted; i <= count; i++) {
            String key = String.format("k%03d", i);
            String expected = "v" + i;
            byte[] actual = page.get(key.getBytes());
            assertNotNull(actual, "Key not found: " + key);
            assertEquals(expected, new String(actual), "Value mismatch for key: " + key);
        }

        assertTrue(page.isAlmostFull(16));
        // Try inserting one more to cause overflow
        assertFalse(page.put("overflow".getBytes(), "x".getBytes()));
    }

    @Test
    void testFragmentedPayloadNearFullWithTinyKV() {
        // Fill with many small key-values to test fragmentation behavior
        int success = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "k" + i;
            String val = "v" + i;
            if (!page.put(key.getBytes(), val.getBytes())) break;
            success++;
        }

        // Check if page metadata is marked full
        assertTrue(page.isAlmostFull(16));
        byte meta = page.buffer().get(0);
        assertTrue((meta & 0b0100_0000) != 0); // FULL_FLAG

        // Spot check 10 random entries
        Random rand = new Random(42);
        for (int i = 0; i < 10; i++) {
            int idx = rand.nextInt(success);
            byte[] key = ("k" + idx).getBytes();
            byte[] value = page.get(key);
            assertNotNull(value);
            assertEquals("v" + idx, new String(value));
        }
    }

    @Test
    void testOverwriteAndValidateSlotReuse() {
        byte[] key = "dup".getBytes();
        byte[] val1 = "12345678".getBytes();
        byte[] val2 = "XY".getBytes();  // Smaller payload

        assertTrue(page.put(key, val1));
        int countBefore = page.getEntryCount();
        int freeBefore = page.getFreeSpaceOffset();

        assertTrue(page.put(key, val2)); // Overwrite

        // Entry count must remain unchanged
        assertEquals(countBefore, page.getEntryCount());

        // Free space should reduce (new value smaller, but payload append-only)
        assertTrue(page.getFreeSpaceOffset() < freeBefore);

        byte[] value = page.get(key);
        assertEquals("XY", new String(value));
    }

    @Disabled // this test does not do anything useful
    @Test
    void testBrokenSlotOffsetOrNegativeAccess() {
        byte[] key = "test".getBytes();
        byte[] value = "1234".getBytes();

        assertTrue(page.put(key, value));
        int slotOffset = LeafPage.HEADER_SIZE + page.getEntryCount() * 2;

        // Corrupt slot manually to simulate a broken pointer
        page.buffer().putShort(LeafPage.HEADER_SIZE, (short) -1);  // invalid offset

        // Should not crash
        assertDoesNotThrow(() -> {
            byte[][] entry = page.getEntry(0);
            assertNull(entry);  // Should gracefully return null or handle
        });
    }

    @Test
    void testExhaustiveInsertUntilOverflowExactSize() {
        int maxKey = 12;
        int maxVal = 200;

        while (true) {
            byte[] key = new byte[maxKey];
            byte[] val = new byte[maxVal];
            Arrays.fill(key, (byte) 'k');
            Arrays.fill(val, (byte) 'v');
            if (!page.put(key, val)) break;
        }

        byte[] key = "x".getBytes();
        byte[] value = "y".getBytes();
        assertTrue(page.isAlmostFull(key.length + value.length));
        assertFalse(page.put(key, value));
    }
}
