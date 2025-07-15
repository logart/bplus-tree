package org.logart;

import org.logart.node.BTreeNode;
import org.logart.node.NodeManager;
import org.logart.node.PageBasedNodeManager;
import org.logart.node.PersistentBTreeNode;
import org.logart.page.MMAPBasedPageManager;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultBPlusTree implements BPlusTree {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Comparator<byte[]> comparator = Arrays::compareUnsigned;

    private final File file;
    private final int pageSize;

    private final NodeManager nodeManager;
    private long rootPageId;

    public DefaultBPlusTree(File file, int pageSize) throws IOException {
        this.file = file;
        this.pageSize = pageSize;
        this.nodeManager = new PageBasedNodeManager(new MMAPBasedPageManager(file, pageSize));
        this.rootPageId = nodeManager.allocateLeafNode().id(); // Starting with a single leaf
    }

    @Override
    public byte[] get(byte[] key) {
        lock.readLock().lock();
        try {
            return getRecursive(rootPageId, key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        lock.writeLock().lock();
        try {
            BTreeNode root = nodeManager.readNode(rootPageId);

            if (root.isFull()) {
                // Create new root
                BTreeNode newRoot = nodeManager.allocateNode();
                long newRootId = newRoot.id();
//                newRoot.addChildren(0, rootPageId);

                splitChild(newRoot, 0, rootPageId);
                insertNonFull(newRoot, key, value);

                nodeManager.writeNode(newRootId, newRoot);
                rootPageId = newRootId;
            } else {
                insertNonFull(root, key, value);
                nodeManager.writeNode(rootPageId, root);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<byte[]> getAllKeysInOrder() {
        return List.of();
    }

    private void splitChild(BTreeNode newRoot, int i, long rootPageId) {

    }

    private void insertNonFull(BTreeNode node, byte[] key, byte[] value) {
        node.put(key, value);
    }

    public void close() throws IOException {
        nodeManager.close();
    }

    private byte[] getRecursive(long pageId, byte[] key) {
        BTreeNode node = nodeManager.readNode(pageId);

        int idx = 0;
        while (idx < node.numKeys() && comparator.compare(key, node.key(idx)) > 0) {
            idx++;
        }

        if (node.isLeaf()) {
            if (idx < node.numKeys() && comparator.compare(key, node.key(idx)) == 0) {
                return ((PersistentBTreeNode) node).loadValue(idx);
            } else {
                return null;
            }
        } else {
//            long childPageId = node.children(idx);
//            return getRecursive(childPageId, key);
        }
        return null;
    }
}

