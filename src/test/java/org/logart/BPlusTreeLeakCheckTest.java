package org.logart;

import org.junit.jupiter.api.Test;
import org.logart.node.MapBasedNodeManager;
import org.logart.node.NodeManager;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BPlusTreeLeakCheckTest {
    @Test
    public void testNoLeakedPagesAfterCopyOnWrite() {
        NodeManager nodeManager = new MapBasedNodeManager();
        BPlusTree tree = new DefaultBPlusTree(nodeManager);

        int total = 100;

        // Fill the tree with data
        for (int i = 0; i < total; i++) {
            System.out.println("Adding key: key" + i);
            tree.put(("key" + i).getBytes(), ("value" + i).getBytes());
            assertNoLeak((DefaultBPlusTree) tree, (MapBasedNodeManager) nodeManager);
        }

        // Force copy-on-write: overwrite same keys
        for (int i = 0; i < total; i++) {
            System.out.println("Updating key: key" + i);
            tree.put(("key" + i).getBytes(), ("newvalue" + i).getBytes());
            assertNoLeak((DefaultBPlusTree) tree, (MapBasedNodeManager) nodeManager);
        }

        assertNoLeak((DefaultBPlusTree) tree, (MapBasedNodeManager) nodeManager);
    }

    private static void assertNoLeak(DefaultBPlusTree tree, MapBasedNodeManager nodeManager) {
        // Gather IDs from a live tree
        Set<Long> reachablePages = Set.copyOf(tree.collectReachablePageIds());

        // Get all ever-allocated pages and freed ones
        Set<Long> allocatedPages = nodeManager.getAllAllocatedNodeIds();
        Set<Long> freedPages = nodeManager.getFreedNodeIds();

        // Check for leaks: allocated pages not reachable and not freed
        Set<Long> leaked = new HashSet<>(allocatedPages);
        leaked.removeAll(reachablePages);
        leaked.removeAll(freedPages);

        System.out.println("Reachable: " + reachablePages.size());
        System.out.println("Allocated: " + allocatedPages.size());
        System.out.println("Freed: " + freedPages.size());
        System.out.println("Leaked: " + leaked);

        assertTrue(leaked.isEmpty(), "There are leaked pages: " + leaked);
    }
}
