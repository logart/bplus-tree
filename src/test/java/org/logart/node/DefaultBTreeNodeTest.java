package org.logart.node;

import org.junit.jupiter.api.Test;
import org.logart.page.Page;
import org.logart.page.memory.InMemoryPage;
import org.logart.page.mmap.LeafPage;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.logart.page.mmap.AbstractPage.PAGE_SIZE;

public class DefaultBTreeNodeTest {
    @Test
    public void shouldToString() {
        DefaultBTreeNode node = new DefaultBTreeNode(new InMemoryPage(1, true));
        node.put("key".getBytes(), "value".getBytes());
        assertNotNull(node);
        assertTrue(node.toString().contains("DefaultBTreeNode"));
    }

    @Test
    public void shouldToStringByteBufferNodes() {
        Page page = LeafPage.newPage(0, ByteBuffer.allocate(PAGE_SIZE));
        DefaultBTreeNode node = new DefaultBTreeNode(page);
        node.put("key".getBytes(), "value".getBytes());
        assertNotNull(node);
        assertTrue(node.toString().contains("DefaultBTreeNode"));
    }
}