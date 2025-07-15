package org.logart;

import org.logart.node.BTreeNode;
import org.logart.node.NodeManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class InMemoryBPlusTree implements BPlusTree {
    private final NodeManager nodeManager;
    private final Comparator<byte[]> comparator;
    private BTreeNode root;

    public InMemoryBPlusTree(NodeManager nodeManager) {
        this.comparator = Arrays::compareUnsigned;
        this.nodeManager = nodeManager;
        this.root = nodeManager.allocateLeafNode(); // start with an empty node
    }

    @Override
    public byte[] get(byte[] key) {
        return recursiveGet(key, root);
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
        SplitResult splitResult = put(root, key, value);

        if (splitResult != null) {
            BTreeNode newRoot = nodeManager.allocateNode();
            newRoot.addChildren(splitResult.promotedKey, splitResult.left.id(), splitResult.right.id());
            root = newRoot;
        }
    }

    private SplitResult put(BTreeNode node, byte[] key, byte[] value) {
        if (node.isLeaf()) {
            node.put(key, value);
        } else {
            long childId = node.findChild(key);
            BTreeNode child = nodeManager.readNode(childId);

            SplitResult splitResult = put(child, key, value);
            if (splitResult != null) {
                // Insert promotedKey and child pointers
                node.addChildren(splitResult.promotedKey, splitResult.left.id(), splitResult.right.id());
            }
        }
        if (node.isFull()) {
            return split(node);
        }

        return null;
    }

    private SplitResult split(BTreeNode node) {
        int mid = (node.numKeys() + 1) / 2;
        if (node.isLeaf()) {
            BTreeNode right = nodeManager.allocateLeafNode();

            for (int i = mid; i < node.numKeys(); i++) {
                byte[][] data = node.get(i);
                right.put(data[0], data[1]);
            }

            node.remove(mid, node.numKeys());

            // I don't support it right now
//            right.next = this.next;
//            this.next = right;

            return new SplitResult(right.get(0)[0], node, right);
        } else {
            byte[] promotedKey = node.get(mid)[0];

            BTreeNode right = nodeManager.allocateNode();
            right.copyChildren(node, mid, node.numKeys());
            node.remove(mid, node.numKeys());

            return new SplitResult(promotedKey, node, right);
        }
    }

    @Override
    public List<byte[]> getAllKeysInOrder() {
        return Collections.emptyList();
    }

    public String printStructure() {
        StringBuilder sb = new StringBuilder();
        sb.append("B+ Tree Structure:\n");
//        printRecursive(root, sb, 0);
        sb.append("B+ Tree Structure end.\n\n");
        return sb.toString();
    }

//    private void printRecursive(BTreeNode node, StringBuilder sb, int level) {
//        if (node == null) {
//            return;
//        }
//        String indent = "  ".repeat(level);
//
//        if (node.isLeaf()) {
//            sb.append(indent).append("Leaf ");
//            for (int i = 0; i < node.numKeys(); i++) {
//                sb.append(new String(node.key(i))).append(", ");
//            }
//            sb.append(" -> ");
//            for (int i = 0; i < node.numKeys(); i++) {
//                sb.append(new String(node.get(i)[1])).append(", ");
//            }
//            sb.append("\n");
//        } else {
//            sb.append(indent).append("Internal ");
//            for (int i = 0; i < node.numKeys(); i++) {
//                sb.append(new String(node.key(i))).append(", ");
//            }
//            sb.append("\n");
//            for (long childId : node.childrenDebugTODOREMOVE()) {
//                BTreeNode child = nodeManager.readNode(childId);
//                printRecursive(child, sb, level + 1);
//            }
//        }
//    }

    @Override
    public String toString() {
        return printStructure();
    }
}
