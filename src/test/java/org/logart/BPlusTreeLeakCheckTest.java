package org.logart;

import org.junit.jupiter.api.Test;
import org.logart.node.MapBasedNodeManager;
import org.logart.node.NodeManager;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BPlusTreeLeakCheckTest {

    // this is a bug with not cleaning up all the children,
    // I believe this will be fixed by establishing a right sibling link and checking if it references the same children
    // unfortunately I don't have time to properly fix ut now.
    @Test
    public void testNoLeakedPagesAfterCopyOnWrite() {
        NodeManager nodeManager = new MapBasedNodeManager();
        BPlusTree tree = new DefaultBPlusTree(nodeManager);

        int total = 100;

        // Fill the tree with data
        for (int i = 0; i < total; i++) {
            tree.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        // Force copy-on-write: overwrite same keys
        for (int i = 0; i < total; i++) {
            tree.put(("key" + i).getBytes(), ("newvalue" + i).getBytes());
        }

        // Gather IDs from live tree
        Set<Long> reachablePages = ((DefaultBPlusTree) tree).collectReachablePageIds();

        // Get all ever-allocated pages and freed ones
        Set<Long> allocatedPages = ((MapBasedNodeManager)nodeManager).getAllAllocatedNodeIds();
        Set<Long> freedPages = ((MapBasedNodeManager)nodeManager).getFreedNodeIds();

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
