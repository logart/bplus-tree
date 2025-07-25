package org.logart.page.mmap;

import org.junit.jupiter.api.Test;
import org.logart.page.Page;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.logart.page.mmap.AbstractPage.*;
import static org.logart.page.mmap.InternalPage.SLOT_CHILD_POINTER;
import static org.logart.page.mmap.InternalPage.SLOT_SIZE;

public class InternalPageTest {
    @Test
    public void shouldCopyChildren() {
        InternalPage original = (InternalPage) InternalPage.newPage(1, ByteBuffer.allocate(PAGE_SIZE));
        InternalPage copy = (InternalPage) InternalPage.newPage(2, ByteBuffer.allocate(PAGE_SIZE));
        byte[] prevKey = "testKey1".getBytes();
        byte[] key = "testKey2".getBytes();
        original.addChild(key, 100, 200);
        assertEquals(100, original.getChild(prevKey), "Child key should match the original");
        assertEquals(200, original.getChild(key), "Child key should match the original");
        copy.copyChildren(original, 0, 1);
        assertEquals(100, copy.getChild(prevKey), "Child key should match the original");
        assertEquals(200, copy.getChild(key), "Child key should match the original");
    }

    @Test
    public void shouldCopyFewChildren() {
        ByteBuffer originalBuffer = ByteBuffer.allocate(PAGE_SIZE);
        ByteBuffer copyBuffer = ByteBuffer.allocate(PAGE_SIZE);
        InternalPage original = (InternalPage) InternalPage.newPage(1, originalBuffer);
        InternalPage copy = (InternalPage) InternalPage.newPage(2, copyBuffer);
        for (int i = 1; i < 10; i++) {
            original.addChild(key(i), i * 100, (i + 1) * 100);
            assertEquals(i * 100, original.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, original.getChild(key(i)), "Child key should match the original");
        }
        copy.copyChildren(original, 0, 10);
        for (int i = 1; i < 10; i++) {
            assertEquals(i * 100, copy.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, copy.getChild(key(i)), "Child key should match the original");
        }
        for (int i = HEADER_SIZE; i < PAGE_SIZE; i += 8) {
            assertEquals(originalBuffer.getLong(i), copyBuffer.getLong(i), "buffers are different at " + i);
        }
    }

    @Test
    public void shouldCopyFirstPartOfKeys() {
        ByteBuffer originalBuffer = ByteBuffer.allocate(PAGE_SIZE);
        ByteBuffer copyBuffer = ByteBuffer.allocate(PAGE_SIZE);
        InternalPage original = (InternalPage) InternalPage.newPage(1, originalBuffer);
        InternalPage copy = (InternalPage) InternalPage.newPage(2, copyBuffer);
        for (int i = 1; i < 10; i++) {
            original.addChild(key(i), i * 100, (i + 1) * 100);
            assertEquals(i * 100, original.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, original.getChild(key(i)), "Child key should match the original");
        }
        copy.copyChildren(original, 0, 3);
        for (int i = 1; i < 4; i++) {
            assertEquals(i * 100, copy.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, copy.getChild(key(i)), "Child key should match the original");
        }
        for (int i = 4; i < 10; i++) {
            assertEquals(400, copy.getChild(key(i)), "Child key should match the original");
        }
    }

    @Test
    public void shouldCopySecondPartOfKeys() {
        ByteBuffer originalBuffer = ByteBuffer.allocate(PAGE_SIZE);
        ByteBuffer copyBuffer = ByteBuffer.allocate(PAGE_SIZE);
        InternalPage original = (InternalPage) InternalPage.newPage(1, originalBuffer);
        InternalPage copy = (InternalPage) InternalPage.newPage(2, copyBuffer);
        for (int i = 1; i < 10; i++) {
            original.addChild(key(i), i * 100, (i + 1) * 100);
            assertEquals(i * 100, original.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, original.getChild(key(i)), "Child key should match the original");
        }
        copy.copyChildren(original, 3, 6);
        for (int i = 1; i < 4; i++) {
            assertEquals(400, copy.getChild(key(i)), "Child key should match the original " + i);
        }
        for (int i = 4; i < 7; i++) {
            assertEquals(i * 100, copy.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, copy.getChild(key(i)), "Child key should match the original");
        }
        for (int i = 7; i < 10; i++) {
            assertEquals(700, copy.getChild(key(i)), "Child key should match the original");
        }
    }

    @Test
    public void shouldReplaceChildrenId() {
        ByteBuffer originalBuffer = ByteBuffer.allocate(PAGE_SIZE);
        InternalPage original = (InternalPage) InternalPage.newPage(1, originalBuffer);
        for (int i = 1; i < 10; i++) {
            original.addChild(key(i), i * 100, (i + 1) * 100);
            assertEquals(i * 100, original.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100, original.getChild(key(i)), "Child key should match the original");
        }
        // it should be <= here since we have +1 child compared to keys
        for (int i = 1; i <= 10; i++) {
            original.replaceChild(i * 100, i * 100 + 50);
        }
        for (int i = 1; i < 10; i++) {
            assertEquals(i * 100 + 50, original.getChild(key(i - 1)), "Child key should match the original");
            assertEquals((i + 1) * 100 + 50, original.getChild(key(i)), "Child key should match the original " + i);
        }
    }

    private byte[] key(int i) {
        return ("testKey" + i).getBytes();
    }


    @Test
    public void shouldInserKeyInTheMiddle() {
        ByteBuffer originalBuffer = ByteBuffer.allocate(PAGE_SIZE);
        InternalPage page = (InternalPage) InternalPage.newPage(1, originalBuffer);
        byte[] leftKey = "k1".getBytes();
        page.addChild(leftKey, 1, 2);
        // leftKey
        // 1, 2
        assertEquals(1, originalBuffer.getLong(HEADER_SIZE)); // 0
        assertEquals(2, originalBuffer.getLong(HEADER_SIZE + SLOT_SIZE)); // 1
        byte[] rightKey = "k3".getBytes();
        page.addChild(rightKey, 3, 4);
        // leftKey, rightKey
        // 1, 3, 4
        assertEquals(1, originalBuffer.getLong(HEADER_SIZE)); // 0
        assertEquals(3, originalBuffer.getLong(HEADER_SIZE + SLOT_SIZE)); // 1
        assertEquals(4, originalBuffer.getLong(HEADER_SIZE + SLOT_SIZE * 2)); // 3
        byte[] midKey = "k2".getBytes();
        page.addChild(midKey, 5, 6);
        // leftKey, midKey, rightKey
        // 1, 5, 6, 4
        assertEquals(1, originalBuffer.getLong(HEADER_SIZE)); // 0
        assertEquals(5, originalBuffer.getLong(HEADER_SIZE + SLOT_SIZE)); // 1
        assertEquals(6, originalBuffer.getLong(HEADER_SIZE + SLOT_SIZE * 2)); // 2
        assertEquals(4, originalBuffer.getLong(HEADER_SIZE + SLOT_SIZE * 3)); // 3

    }

    @Test
    void shouldNotOverwriteSlotWithWithDataIfPageIsPacked100Percent() {
        ByteBuffer originalBuffer = ByteBuffer.allocate(PAGE_SIZE);
        InternalPage page = (InternalPage) InternalPage.newPage(1, originalBuffer);

        for (int i = 0; i < 1000; i++) {
            int j = 0;
            String k = "k" + i;
            try {
                while (page.addChild((k + j).getBytes(), 1, 1)) {
                    j++;
                    assertTrue(sanityCheck(page));
                }
                assertTrue(sanityCheck(page));
            } catch (Exception e) {
                throw new RuntimeException("Failed to add key: " + i + " - " + j + " - " + k + " with value: ", e);
            }
        }
    }

    private boolean sanityCheck(Page checkedPage) {
        try {
            int cnt = checkedPage.getEntryCount();
            for (int i = 0; i < cnt; i++) {
                InternalPage internalPage = (InternalPage) checkedPage;
                ByteBuffer buffer = internalPage.buffer2();
                long leftId = buffer.getLong(HEADER_SIZE + (SLOT_SIZE * i));
                int keyOffset = buffer.getShort(HEADER_SIZE + (SLOT_SIZE * i) + SLOT_CHILD_POINTER);
                long rightId = buffer.getLong(HEADER_SIZE + SLOT_CHILD_POINTER + (SLOT_SIZE * i) + SLOT_KEY_SIZE);

                short keyDataSize = buffer.getShort(keyOffset);
                byte[] key = new byte[keyDataSize];
                buffer.get(keyOffset + 2, key);
                if (leftId != 1 || rightId != 1) {
                    throw new IllegalStateException("Child pointers is corrupted: " + leftId + ", " + rightId);
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