package org.logart;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logart.node.MapBasedNodeManager;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BPlusTreeSplitTest {

    BPlusTree tree;

    @BeforeEach
    void setUp() {
        tree = new InMemoryBPlusTree(new MapBasedNodeManager());
    }

    @Test
    void testInsertWithoutSplit() {
        tree.put("10".getBytes(), "A".getBytes());
        tree.put("20".getBytes(), "B".getBytes());
        tree.put("30".getBytes(), "C".getBytes());

        assertArrayEquals("A".getBytes(), tree.get("10".getBytes()));
        assertArrayEquals("B".getBytes(), tree.get("20".getBytes()));
        assertArrayEquals("C".getBytes(), tree.get("30".getBytes()));
    }

    @Test
    void testSplitLeafNode() {
        tree.put("10".getBytes(), "A".getBytes());
        tree.put("20".getBytes(), "B".getBytes());
        tree.put("30".getBytes(), "C".getBytes());
        tree.put("40".getBytes(), "D".getBytes()); // Causes split

        assertArrayEquals("A".getBytes(), tree.get("10".getBytes()));
        assertArrayEquals("D".getBytes(), tree.get("40".getBytes()));
        assertNull(tree.get("50".getBytes()));

        // Optional: visualize
        System.out.println(((InMemoryBPlusTree) tree).printStructure());
    }

    @Test
    void testSplitPropagatesToInternalNode() {
        for (int i = 10; i <= 100; i += 10) {
            tree.put(String.valueOf(i).getBytes(), ("V" + i).getBytes());
            System.out.println("Added: " + i);
            System.out.println(((InMemoryBPlusTree) tree).printStructure());
        }

        // Root should have children now
        for (int i = 10; i <= 100; i += 10) {
            assertArrayEquals(("V" + i).getBytes(), tree.get(String.valueOf(i).getBytes()));
        }

        System.out.println(((InMemoryBPlusTree) tree).printStructure());
    }

    @Test
    void testSplitRootCreation() {
        int[] keys = {5, 15, 25, 35, 45, 55, 65, 75};

        for (int k : keys) {
            tree.put(String.valueOf(k).getBytes(), ("Val" + k).getBytes());
        }

        for (int k : keys) {
            assertArrayEquals(("Val" + k).getBytes(), tree.get(String.valueOf(k).getBytes()));
        }

        // Check the tree still valid
        assertNull(tree.get(String.valueOf(999).getBytes()));

        System.out.println(((InMemoryBPlusTree) tree).printStructure());
    }

    @Test
    void testDuplicateKeyInsertion() {
        tree.put("10".getBytes(), "X".getBytes());
        tree.put("10".getBytes(), "Y".getBytes());

        assertArrayEquals("Y".getBytes(), tree.get("10".getBytes())); // Assuming overwrite semantics
    }
}
