package org.logart;

import org.logart.node.BTreeNode;
import org.logart.node.NodeManager;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultBPlusTree implements BPlusTree {
    private final NodeManager nodeManager;
    private final AtomicReference<Versioned<BTreeNode>> root;

    public DefaultBPlusTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.root = new AtomicReference<>(new Versioned<>(nodeManager.allocateLeafNode(), 0)); // start with an empty node
    }

    @Override
    public byte[] get(byte[] key) {
        return recursiveGet(key, root.get().get());
    }

    private byte[] recursiveGet(byte[] key, BTreeNode node) {
        if (node.isLeaf()) {
            return node.get(key);
        }
        long next = node.findChild(key);
        return recursiveGet(key, nodeManager.readNode(next));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        boolean rootUpdated;
        BTreeNode newRoot;
        do {
            Versioned<BTreeNode> currentVersionedRoot = root.get();
            BTreeNode currentRoot = currentVersionedRoot.get();
            SplitResult splitResult = put(currentRoot, key, value);

//             in case root was updated, but there is no split
            newRoot = splitResult.nodeCopy();
            if (splitResult.promotedKey() != null) {
                newRoot = nodeManager.allocateNode();
                newRoot.addChildren(splitResult.promotedKey(), splitResult.left().id(), splitResult.right().id());
            }
            rootUpdated = root.compareAndSet(currentVersionedRoot, new Versioned<>(newRoot, currentVersionedRoot.version() + 1));
//             if root is already different, we have to retry
        } while (!rootUpdated);
        nodeManager.writeNode(newRoot.id(), newRoot);
    }

    private SplitResult put(BTreeNode node, byte[] key, byte[] value) {
        BTreeNode nodeCopy;
        if (node.isLeaf()) {
            nodeCopy = nodeManager.allocateLeafNode();
            nodeCopy.copy(node);
            nodeCopy.put(key, value);
        } else {
            long childId = node.findChild(key);
            BTreeNode child = nodeManager.readNode(childId);

            assert (child != null) : "Node with id: " + childId + " was null. Node: " + node + ", key: " + new String(key);
            SplitResult splitResult = put(child, key, value);
            BTreeNode childCopy = splitResult.nodeCopy();

            nodeCopy = nodeManager.allocateNode();
            nodeCopy.copy(node);
            if (childCopy != null) {
                nodeCopy.replaceChild(childId, childCopy.id());
            }

            if (splitResult.promotedKey() != null) {
                // Insert promotedKey and child pointers
                nodeCopy = nodeManager.allocateNode();
                nodeCopy.copy(node);
                nodeCopy.addChildren(splitResult.promotedKey(), splitResult.left().id(), splitResult.right().id());
            }
        }
        if (nodeCopy.isFull()) {
            // this split could be moved to the beginning, right now it's suboptimal we first copy the node and write to it, but then split could copy it one more time
            // unfortunately, I would not be able to it now because of lack of time
            return split(nodeCopy);
        }

        return new SplitResult(nodeCopy, null, null, null);
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

            return new SplitResult(null, right.get(0)[0], left, right);
        } else {
            byte[] promotedKey = node.get(mid)[0];
            BTreeNode left = nodeManager.allocateNode();
            left.copyChildren(node, 0, mid);

            BTreeNode right = nodeManager.allocateNode();
            right.copyChildren(node, mid, node.numKeys());

            return new SplitResult(null, promotedKey, left, right);
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
        printRecursive(root.get().get(), sb, 0);
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
}
