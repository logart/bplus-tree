package org.logart;

import org.logart.node.BTreeNode;
import org.logart.node.NodeManager;

import java.util.*;

public class DefaultBPlusTree implements BPlusTree {
    private final NodeManager nodeManager;

    public DefaultBPlusTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
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
        boolean rootUpdated;
        BTreeNode newRoot;
        BTreeNode currentRoot;
        SplitResult splitResult;
        Versioned<BTreeNode> currentVersionedRoot;
        long version;
        do {
            currentVersionedRoot = nodeManager.lockVersion();
            try {
                currentRoot = currentVersionedRoot.get();
                version = currentVersionedRoot.version();

                try {
                    splitResult = put(currentRoot, key, value, version);
                } catch (NullPointerException | AssertionError e) {
                    System.out.println("At version: " + version
                            + " version ref counter is: " + nodeManager.refCounter().getRefCount(version)
                            + " available version are: " + nodeManager.refCounter().toString()
                            + " Node: " + currentRoot + ", key: " + new String(key)
                    );
                    throw e;
                }

//             in case root was updated, but there is no split
                newRoot = splitResult.nodeCopy();
                if (splitResult.promotedKey() != null) {
                    newRoot = nodeManager.allocateNode();
                    newRoot.addChildren(splitResult.promotedKey(), splitResult.left().id(), splitResult.right().id());
                }
                rootUpdated = nodeManager.advanceVersion(currentVersionedRoot, newRoot);
            } finally {
                nodeManager.releaseVersion(currentVersionedRoot);
            }
//             if root is already different, we have to retry
        } while (!rootUpdated);
        // we have to only write nodes after root is updated
        nodeManager.freeNode(currentRoot.id(), version);
        for (long oldNodeId : splitResult.oldNodes()) {
            nodeManager.freeNode(oldNodeId, version);
        }
        nodeManager.writeNode(newRoot.id(), newRoot);
    }

    private SplitResult put(BTreeNode node, byte[] key, byte[] value, long version) {
        assert (node != null) : "At version: " + version
                + " version ref counter is: " + nodeManager.refCounter().getRefCount(version)
                + " available version are: " + nodeManager.refCounter().toString()
                + " Node: " + node + ", key: " + new String(key);

        Set<Long> oldNodes = new HashSet<>();
        BTreeNode nodeCopy;
        if (node.isLeaf()) {
            nodeCopy = nodeManager.allocateLeafNode();
            nodeCopy.copy(node);
            nodeCopy.put(key, value);
            oldNodes.add(node.id());
        } else {
            long childId = node.findChild(key);
            BTreeNode child = nodeManager.readNode(childId);

            SplitResult splitResult;
            try {
                splitResult = put(child, key, value, version);
            } catch (NullPointerException | AssertionError e) {
                System.out.println("At version: " + version
                        + " version ref counter is: " + nodeManager.refCounter().getRefCount(version)
                        + " available version are: " + nodeManager.refCounter().toString()
                        + " Node: " + node + ", key: " + new String(key)
                );
                throw e;
            }
            oldNodes.addAll(splitResult.oldNodes());
            BTreeNode childCopy = splitResult.nodeCopy();

            nodeCopy = nodeManager.allocateNode();
            nodeCopy.copy(node);
            oldNodes.add(node.id());
            if (childCopy != null) {
                nodeCopy.replaceChild(childId, childCopy.id());
                oldNodes.add(childId);
            }

            if (splitResult.promotedKey() != null) {
                // Insert promotedKey and child pointers
                nodeCopy.addChildren(splitResult.promotedKey(), splitResult.left().id(), splitResult.right().id());
                oldNodes.add(node.id());
            }
        }
        if (nodeCopy.isFull()) {
            // this split could be moved to the beginning, right now it's suboptimal we first copy the node and write to it, but then split could copy it one more time
            // unfortunately, I would not be able to it now because of lack of time
            SplitResult splitResult = split(nodeCopy);
            // add old nodes to the set
            Set<Long> combinedOldNodes = new HashSet<>(splitResult.oldNodes());
            combinedOldNodes.addAll(oldNodes);
            // I know that copying of objects is expensive, but I would like to have immutability here
            return new SplitResult(splitResult.nodeCopy(), splitResult.promotedKey(), splitResult.left(), splitResult.right(), combinedOldNodes);
        }

        nodeManager.writeNode(nodeCopy.id(), nodeCopy);
        return new SplitResult(nodeCopy, null, null, null, oldNodes);
    }

    private SplitResult split(BTreeNode node) {
        int mid = (node.numKeys() + 1) / 2;
        if (node.isLeaf()) {
            BTreeNode left = nodeManager.allocateLeafNode();
            for (int i = 0; i < mid; i++) {
                byte[][] data = node.get(i);
                left.put(data[0], data[1]);
            }

            BTreeNode right = nodeManager.allocateLeafNode();
            for (int i = mid; i < node.numKeys(); i++) {
                byte[][] data = node.get(i);
                right.put(data[0], data[1]);
            }

            nodeManager.writeNode(left.id(), left);
            nodeManager.writeNode(right.id(), right);
            return new SplitResult(null, right.get(0)[0], left, right, Collections.singleton(node.id()));
        } else {
            byte[] promotedKey = node.get(mid)[0];
            BTreeNode left = nodeManager.allocateNode();
            left.copyChildren(node, 0, mid);

            BTreeNode right = nodeManager.allocateNode();
            right.copyChildren(node, mid + 1, node.numKeys());

            nodeManager.writeNode(left.id(), left);
            nodeManager.writeNode(right.id(), right);
            return new SplitResult(null, promotedKey, left, right, Collections.singleton(node.id()));
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
