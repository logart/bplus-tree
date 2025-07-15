package org.logart.node;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryBTreeNodeTest {
    @Test
    public void shouldReturnPreviousIndexIfKeyIsLessThenStoredKey() {
        InMemoryBTreeNode node = new InMemoryBTreeNode(1, 0, true);
        node.key(0, "05".getBytes());
        node.key(1, "10".getBytes());
        node.key(2, "20".getBytes());
        assertEquals(1, node.searchKeyIdx("09".getBytes()));
    }

    @Test
    public void shouldReturnCurrentIndexIfKeyIsEqualToStoredKey() {
        InMemoryBTreeNode node = new InMemoryBTreeNode(1, 0, true);
        node.key(0, "05".getBytes());
        node.key(1, "10".getBytes());
        node.key(2, "20".getBytes());
        assertEquals(1, node.searchKeyIdx("10".getBytes()));
    }

    @Test
    public void shouldReturnLastIndexIfKeyIsGreaterThenStoredKey() {
        InMemoryBTreeNode node = new InMemoryBTreeNode(1, 0, true);
        node.key(0, "05".getBytes());
        node.key(1, "10".getBytes());
        node.key(2, "20".getBytes());
        assertEquals(3, node.searchKeyIdx("21".getBytes()));
    }

    @Test
    public void shouldRemoveKeys() {
        InMemoryBTreeNode node = new InMemoryBTreeNode(1, 0, true);
        node.key(0, "05".getBytes());
        node.key(1, "10".getBytes());
        node.key(2, "20".getBytes());
        assertArrayEquals("05".getBytes(), node.get(0)[0]);
        assertArrayEquals("10".getBytes(), node.get(1)[0]);
        assertArrayEquals("20".getBytes(), node.get(2)[0]);
        node.remove(2, 3);
        assertArrayEquals("05".getBytes(), node.get(0)[0]);
        assertArrayEquals("10".getBytes(), node.get(1)[0]);
        assertNull(node.get(2));
        node.remove(1, 2);
        assertArrayEquals("05".getBytes(), node.get(0)[0]);
        assertNull(node.get(1));
        assertNull(node.get(2));
    }

    @Test
    public void shouldRemoveChildren() {
        InMemoryBTreeNode node = new InMemoryBTreeNode(1, 0, false);
        node.addChildren("05".getBytes(), 0, 1);
        node.addChildren("10".getBytes(), 1, 2);
        node.addChildren("20".getBytes(), 2, 3);
        assertEquals(0, node.findChild("04".getBytes()));
        assertEquals(1, node.findChild("05".getBytes()));
        assertEquals(1, node.findChild("06".getBytes()));
        assertEquals(1, node.findChild("09".getBytes()));
        assertEquals(2, node.findChild("10".getBytes()));
        assertEquals(2, node.findChild("11".getBytes()));
        assertEquals(2, node.findChild("19".getBytes()));
        assertEquals(3, node.findChild("20".getBytes()));
        assertEquals(3, node.findChild("21".getBytes()));
        node.remove(2, 3);
        assertEquals(0, node.findChild("04".getBytes()));
        assertEquals(1, node.findChild("05".getBytes()));
        assertEquals(1, node.findChild("06".getBytes()));
        assertEquals(1, node.findChild("09".getBytes()));
        assertEquals(2, node.findChild("10".getBytes()));
        assertEquals(2, node.findChild("11".getBytes()));
        assertEquals(2, node.findChild("19".getBytes()));
        assertEquals(2, node.findChild("20".getBytes()));
        assertEquals(2, node.findChild("21".getBytes()));
    }
}