package org.logart.page.mmap;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logart.page.mmap.AbstractPage.HEADER_SIZE;
import static org.logart.page.mmap.AbstractPage.PAGE_SIZE;

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
}