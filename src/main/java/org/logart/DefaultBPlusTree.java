package org.logart;

import org.logart.node.BTreeNode;
import org.logart.node.NodeManager;
import org.logart.tree.PutHandler;
import org.logart.tree.PutResult;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DefaultBPlusTree implements BPlusTree {
    private final NodeManager nodeManager;
    private final PutHandler putHandler;

    public DefaultBPlusTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.putHandler = new PutHandler(nodeManager);
    }

    @Override
    public byte[] get(byte[] key) {
        Versioned<BTreeNode> versionedRoot = nodeManager.lockVersion();
        try {
            return recursiveGet(key, versionedRoot.get(), versionedRoot.version());
        } finally {
            nodeManager.releaseVersion(versionedRoot);
        }
    }

    private byte[] recursiveGet(byte[] key, BTreeNode node, long version) {
        assert (node != null) : "At version: " + version
                + " version ref counter is: " + nodeManager.refCounter().getRefCount(version)
                + " available version are: " + nodeManager.refCounter().toString()
                + " Node: " + node + ", key: " + new String(key);
        if (node.isLeaf()) {
            return node.get(key);
        }
        long next = node.findChild(key);
        try {
            return recursiveGet(key, nodeManager.readNode(next), version);
        } catch (NullPointerException | AssertionError e) {
            System.out.println("At version: " + version
                    + " version ref counter is: " + nodeManager.refCounter().getRefCount(version)
                    + " available version are: " + nodeManager.refCounter().toString()
                    + " Node: " + node + ", key: " + new String(key)
            );
            throw e;
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        boolean rootUpdated = false;
        PutResult put = null;
        Versioned<BTreeNode> currentVersionedRoot = null;
        while (!rootUpdated) {
            currentVersionedRoot = nodeManager.lockVersion();
            try {
                put = putHandler.put(currentVersionedRoot.get(), key, value, currentVersionedRoot.version());
                rootUpdated = nodeManager.advanceVersion(currentVersionedRoot, put.nodeCopy());
            } finally {
                nodeManager.releaseVersion(currentVersionedRoot);
            }
        }
        // write, free node and return
        for (long oldNodeId : put.oldNodes()) {
            nodeManager.freeNode(oldNodeId, currentVersionedRoot.version());
        }
    }

    @Override
    public List<byte[]> getAllKeysInOrder() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        nodeManager.close();
    }

    public String printStructure() {
        StringBuilder sb = new StringBuilder();
        sb.append("B+ Tree Structure:\n");
        Versioned<BTreeNode> currentVersionedRoot = nodeManager.lockVersion();
        try {
            printRecursive(currentVersionedRoot.get(), sb, 0);
        } finally {
            nodeManager.releaseVersion(currentVersionedRoot);
        }
        sb.append("B+ Tree Structure end.\n\n");
        return sb.toString();
    }

    private void printRecursive(BTreeNode node, StringBuilder sb, int level) {
        if (node == null) {
            return;
        }
        String indent = "  ".repeat(level);

        if (node.isLeaf()) {
            sb.append(indent).append("Leaf ");
            for (int i = 0; i < node.numKeys(); i++) {
                sb.append(new String(node.get(i)[0])).append(", ");
            }
            sb.append(" -> ");
            for (int i = 0; i < node.numKeys(); i++) {
                sb.append(new String(node.get(i)[1])).append(", ");
            }
            sb.append("\n");
        } else {
            sb.append(indent).append("Internal ");
            for (int i = 0; i < node.numKeys(); i++) {
                sb.append(new String(node.get(i)[0])).append(", ");
            }
            sb.append("\n");
            for (long childId : node.childrenDebugTODOREMOVE()) {
                BTreeNode child = nodeManager.readNode(childId);
                printRecursive(child, sb, level + 1);
            }
        }
    }

    @Override
    public String toString() {
        return printStructure();
    }

    // this implementation uses list and not set since it is used for debugging purposes,
    // and part of testing and debugging is to make sure no page is referenced twice
    public List<Long> collectReachablePageIds() {
        Versioned<BTreeNode> bTreeNodeVersioned = nodeManager.lockVersion();
        try {
            return new LinkedList<>(collectRecursive(bTreeNodeVersioned.get()));
        } finally {
            nodeManager.releaseVersion(bTreeNodeVersioned);
        }
    }

    private List<Long> collectRecursive(BTreeNode node) {
        List<Long> visited = new LinkedList<>();

        visited.add(node.id());

        if (!node.isLeaf()) {
            for (long childId : node.childrenDebugTODOREMOVE()) {
                if (childId != -1) {
                    BTreeNode child = nodeManager.readNode(childId);
                    if (child == null) {
                        throw new IllegalStateException("Child node with id " + childId + " is reachable from node " + node.id() + " but is deallocated.");
                    }
                    visited.addAll(collectRecursive(child));
                }
            }
        }
        return visited;
    }
}
