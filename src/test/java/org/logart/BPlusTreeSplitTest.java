package org.logart;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.MapBasedPageManager;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeSplitTest {

    BPlusTree tree;

    @BeforeEach
    void setUp() {
        tree = new DefaultBPlusTree(new DefaultNodeManager(new MapBasedPageManager()));
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
        System.out.println(((DefaultBPlusTree) tree).printStructure());
    }

    @Test
    void testSplitPropagatesToInternalNode() {
        for (int i = 10; i <= 100; i += 10) {
            tree.put(String.valueOf(i).getBytes(), ("V" + i).getBytes());
            System.out.println("Added: " + i);
            System.out.println(((DefaultBPlusTree) tree).printStructure());
        }

        // Root should have children now
        for (int i = 10; i <= 100; i += 10) {
            assertArrayEquals(("V" + i).getBytes(), tree.get(String.valueOf(i).getBytes()));
        }

        System.out.println(((DefaultBPlusTree) tree).printStructure());
    }

    @Test
    void testSplitRootCreation() {
        int[] keys = {5, 15, 25, 35, 45, 55, 65, 75};

        for (int k : keys) {
            tree.put(String.valueOf(k).getBytes(), ("Val" + k).getBytes());
            System.out.println("Added: " + k);
            System.out.println(((DefaultBPlusTree) tree).printStructure());
        }

        for (int k : keys) {
            assertArrayEquals(("Val" + k).getBytes(), tree.get(String.valueOf(k).getBytes()));
        }

        // Check the tree still valid
        assertNull(tree.get(String.valueOf(999).getBytes()));

        System.out.println(((DefaultBPlusTree) tree).printStructure());
    }

    @Test
    void testDuplicateKeyInsertion() {
        tree.put("10".getBytes(), "X".getBytes());
        tree.put("10".getBytes(), "Y".getBytes());

        assertArrayEquals("Y".getBytes(), tree.get("10".getBytes())); // Assuming overwrite semantics
    }

    @Test
    public void shouldOnlyUseRootLeafBeforeOverflow() {
        for (int i = 0; i < 2; i++) {
            byte[] key = String.format("%02d", i).getBytes(StandardCharsets.UTF_8);
            byte[] value = ("val" + i).getBytes(StandardCharsets.UTF_8);
            tree.put(key, value);
            List<Long> usedPageIds = ((DefaultBPlusTree) tree).collectReachablePageIds();
            assertEquals(1, usedPageIds.size());
        }
    }

    @Test
    public void splitShouldNotProduceDuplicatePageIdReferences() {
        for (int i = 0; i < 2000; i++) {
            byte[] key = String.format("%02d", i).getBytes(StandardCharsets.UTF_8);
            byte[] value = ("val" + i).getBytes(StandardCharsets.UTF_8);
            tree.put(key, value);
            // split starts
            if (i >= 3) {
                List<Long> usedPageIds = ((DefaultBPlusTree) tree).collectReachablePageIds();
                assertTrue(usedPageIds.size() > 1);
                Map<Long, List<Long>> groupedUsedPageIds = usedPageIds.stream()
                        .collect(groupingBy(Long::longValue));
                for (Map.Entry<Long, List<Long>> entry : groupedUsedPageIds.entrySet()) {
                    assertEquals(1, entry.getValue().size(),
                            "Page ID " + entry.getKey() + " should not be duplicated, found " +
                                    entry.getValue().size() + " times"
                    );

                }
            }
        }
    }
}