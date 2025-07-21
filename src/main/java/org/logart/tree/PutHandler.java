package org.logart.tree;

import org.logart.node.BTreeNode;
import org.logart.node.NodeManager;

import java.util.*;

public class PutHandler {
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;
    private final NodeManager nodeManager;

    public PutHandler(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public PutResult put(final BTreeNode node, byte[] key, byte[] value, long version) {
        Set<Long> oldNodes = new HashSet<>();
        SplitResult splitResult = null;
        if (node.isAlmostFull(key.length + value.length)) {
            // split node prematurely if after insert it will be full
            splitResult = split(node);
        }
        oldNodes.add(node.id());
        final BTreeNode nodeCopy;
        if (splitResult != null && splitResult.promotedKey() != null) {
            nodeCopy = nodeManager.allocateNode();
            nodeCopy.addChildren(splitResult.promotedKey(), splitResult.left().id(), splitResult.right().id());
            putIntoNewlyAllocatedChild(key, value, splitResult);
            return new PutResult(nodeCopy, oldNodes);
        }
        nodeCopy = node.isLeaf()
                ? nodeManager.allocateLeafNode()
                : nodeManager.allocateNode();
        nodeCopy.copy(node);
        if (nodeCopy.isLeaf()) {
            nodeCopy.put(key, value);
        } else {
            long childId = nodeCopy.findChild(key);
            BTreeNode child = nodeManager.readNode(childId);

            PutResult putResult = put(child, key, value, version);
            oldNodes.addAll(putResult.oldNodes());

            BTreeNode childCopy = putResult.nodeCopy();
            if (childCopy != null) {
                nodeCopy.replaceChild(childId, childCopy.id());
                oldNodes.add(childId);
            }
        }

        return new PutResult(nodeCopy, oldNodes);
    }

    private void putIntoNewlyAllocatedChild(byte[] key, byte[] value, SplitResult splitResult) {
        final BTreeNode childNode;
        if (COMPARATOR.compare(key, splitResult.promotedKey()) < 0) {
            childNode = nodeManager.readNode(splitResult.left().id());
        } else {
            childNode = nodeManager.readNode(splitResult.right().id());
        }
        childNode.put(key, value);
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
            return new SplitResult(right.get(0)[0], left, right);
        } else {
            byte[] promotedKey = node.get(mid)[0];
            BTreeNode left = nodeManager.allocateNode();
            left.copyChildren(node, 0, mid);

            BTreeNode right = nodeManager.allocateNode();
            right.copyChildren(node, mid + 1, node.numKeys());

            nodeManager.writeNode(left.id(), left);
            nodeManager.writeNode(right.id(), right);
            return new SplitResult(promotedKey, left, right);
        }
    }
}
