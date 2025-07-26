package org.logart.node;

import org.logart.page.Page;
import org.logart.page.memory.InMemoryPage;

import java.util.Arrays;
import java.util.Comparator;

public class DefaultBTreeNode implements BTreeNode {
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;
    private final Page page;

    public DefaultBTreeNode(Page page) {
        this.page = page;
    }

    @Override
    public long id() {
        return page.pageId();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        page.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) {
        return page.get(key);
    }

    @Override
    public byte[][] get(int idx) {
        return page.getEntry(idx);
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        // we can cache this value, but again for speed and simplicity it does not matter for in-memory implementation
        return page.isAlmostFull(capacity);
    }

    @Override
    public void copyChildren(BTreeNode node, int startIdx, int endIdx) {
        if (node.isLeaf()) {
            throw new UnsupportedOperationException("Cannot copy children from a leaf node.");
        }
        page.copyChildren(node.page(), startIdx, endIdx);
    }

    @Override
    public void addChildren(byte[] key, long leftPageId, long rightPageId) {
        page.addChild(key, leftPageId, rightPageId);
    }

    @Override
    public void replaceChild(long childId, long newId) {
        page.replaceChild(childId, newId);
    }

    @Override
    public long findChild(byte[] key) {
        if (isLeaf()) {
            throw new UnsupportedOperationException("Leaf nodes do not have children.");
        }
        return page.getChild(key);
    }


    @Override
    public int numKeys() {
        return page.getEntryCount();
    }

    @Override
    public boolean isLeaf() {
        return page.isLeaf();
    }

    @Override
    public void copy(BTreeNode node) {
        page.copy(node.page());
    }

    @Override
    public Page page() {
        return page;
    }

    public long[] children() {
        return ((InMemoryPage) page).children();
    }
}
