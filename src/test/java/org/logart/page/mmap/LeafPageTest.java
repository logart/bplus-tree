package org.logart.page.mmap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logart.page.Page;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.logart.page.mmap.AbstractPage.HEADER_SIZE;
import static org.logart.page.mmap.LeafPage.PAYLOAD_SIZE_FIELD_SIZE;
import static org.logart.page.mmap.LeafPage.SLOT_SIZE;

public class LeafPageTest {
    private static final int PAGE_SIZE = 4096;
    private ByteBuffer buffer;
    private LeafPage page;

    @BeforeEach
    void setUp() {
        buffer = ByteBuffer.allocate(PAGE_SIZE);
        page = (LeafPage) LeafPage.newPage(1L, buffer, true);
    }

    @Test
    void testSingleInsertAndGet() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        assertTrue(page.put(key, value));

        byte[] retrieved = page.get(key);
        assertNotNull(retrieved);
        assertArrayEquals(value, retrieved);
    }

    @Test
    void testOverwriteKey() {
        byte[] key = "dup".getBytes();
        byte[] val1 = "one".getBytes();
        byte[] val2 = "two".getBytes();

        assertTrue(page.put(key, val1));
        assertTrue(page.put(key, val2));

        assertEquals(1, page.getEntryCount());
        assertArrayEquals(val2, page.get(key));
    }

    @Test
    void testInsertMultipleAndOrdering() {
        byte[] k1 = "a".getBytes();
        byte[] v1 = "1".getBytes();
        byte[] k2 = "b".getBytes();
        byte[] v2 = "2".getBytes();
        byte[] k3 = "c".getBytes();
        byte[] v3 = "3".getBytes();

        assertTrue(page.put(k2, v2));
        assertTrue(page.put(k1, v1));
        assertTrue(page.put(k3, v3));

        assertEquals(3, page.getEntryCount());

        assertArrayEquals(v1, page.get(k1));
        assertArrayEquals(v2, page.get(k2));
        assertArrayEquals(v3, page.get(k3));
    }

    @Test
    void testPageOverflow() {
        byte[] value = new byte[100]; // Large enough to eventual overflow
        Arrays.fill(value, (byte) 'v');

        int count = 0;
        while (true) {
            byte[] k = ("k" + count).getBytes();
            if (!page.put(k, value)) break;
            count++;
        }

        assertTrue(page.isAlmostFull(100));
        assertFalse(page.put("overflow".getBytes(), value));

        byte meta = buffer.get(0);
        assertTrue((meta & 0b0100_0000) != 0); // FULL_FLAG set
    }

    @Test
    void testPageOverflow2() {
        int lastI = 0;
        for (int i = 0; i < 10_000_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            if (!page.put(key, value)) {
                lastI = i;
                break;
            }
        }
        for (int i = 0; i < lastI; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            assertArrayEquals(value, page.get(key));
        }
    }

    @Test
    void testMaxSizeEntry() {
        int usable = page.getFreeSpaceOffset() - (HEADER_SIZE + 2);
        byte[] key = new byte[usable / 2 - 2];
        byte[] val = new byte[usable / 2 - 2];

        Arrays.fill(key, (byte) 'k');
        Arrays.fill(val, (byte) 'v');

        assertTrue(page.put(key, val));

        byte[] retrieved = page.get(key);
        assertNotNull(retrieved);
        assertArrayEquals(val, retrieved);
    }

    @Test
    void testEntryOutOfBounds() {
        assertNull(page.getEntry(0));
        byte[] key = "key".getBytes();
        byte[] val = "val".getBytes();
        page.put(key, val);
        assertNull(page.getEntry(2)); // Invalid index
    }

    @Test
    void shouldReorderSlotDirectoryWithoutExceptions() {
        long l = 0;
        long r = Long.MAX_VALUE >> 62;
        byte[] hugeValue = new byte[4];
        Arrays.fill(hugeValue, (byte) 'v');
        while (l < r) {
            long key = (r - l) / 2;
            byte[] keyPayload = ("key-" + key).getBytes();
            assertTrue(page.put(keyPayload, hugeValue));
            for (int i = 1; i < page.getEntryCount(); i++) {
                byte[][] prev = page.getEntry(i - 1);
                byte[][] current = page.getEntry(i);
                int cmp = new String(current[0]).compareTo(new String(prev[0]));
                assertTrue(cmp > 0);
            }
            if (Math.random() < 0.5) {
                l--;
            } else {
                r--;
            }
        }
        // I know there is no assert, but this test will fail in with ArrayIndexOutOfBoundsException if the slot directory is not reordered correctly
    }

    @Test
    void shouldNotOverwriteSlotWithWithDataIfPageIsPacked100Percent() {
        int j = 0;
        String v = "v";
        String k = "k0";
        byte[] superSmallValue = v.getBytes();
        while (page.put((k + j).getBytes(), superSmallValue)) {
            j++;
            assertTrue(sanityCheck(page));
        }
        assertTrue(sanityCheck(page));
    }

    private boolean sanityCheck(Page checkedPage) {
        try {
            int cnt = checkedPage.getEntryCount();
            for (int i = 0; i < cnt; i++) {
                ByteBuffer buffer = ((LeafPage) checkedPage).buffer(true);
                short dataStart = buffer.getShort(HEADER_SIZE + (SLOT_SIZE * i));
                short kSize = buffer.getShort(dataStart);
                byte[] key = new byte[kSize];
                buffer.get(dataStart + PAYLOAD_SIZE_FIELD_SIZE, key);
                short vSize = buffer.getShort(dataStart + PAYLOAD_SIZE_FIELD_SIZE + key.length);
                byte[] value = new byte[vSize];
                buffer.get(dataStart + PAYLOAD_SIZE_FIELD_SIZE + kSize + PAYLOAD_SIZE_FIELD_SIZE, value);
                if (!isValid(key) || !isValid(value)) {
                    throw new IllegalStateException("Invalid entry at index " + i + " in LeafPage with ID: " + checkedPage.pageId());
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isValid(byte[] key) {
        return new String(key).matches("[a-zA-Z0-9_\\-]+");
    }
}