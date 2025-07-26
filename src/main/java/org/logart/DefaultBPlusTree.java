package org.logart;

import org.logart.node.BTreeNode;
import org.logart.node.DefaultBTreeNode;
import org.logart.node.NodeManager;
import org.logart.tree.PutHandler;
import org.logart.tree.PutResult;

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
    public void load() {
        nodeManager.open();
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
    public void close() {
        nodeManager.close();
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
            for (long childId : ((DefaultBTreeNode)node).children()) {
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
