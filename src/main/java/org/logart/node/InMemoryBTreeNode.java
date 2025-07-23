package org.logart.node;

import org.logart.page.Page;

import java.util.Arrays;
import java.util.Comparator;

public class InMemoryBTreeNode implements BTreeNode {
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;
    private final Page page;

    public InMemoryBTreeNode(Page page) {
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

    @Override
    public long[] childrenDebugTODOREMOVE() {
        return page.childrenDbugTODOREMOVE();
    }

    @Override
    public String toString() {
        return "InMemoryBTreeNode{" +
                "id=" + page.pageId() +
                ", l=" + page.isLeaf() +
                ", data=" + dataToString() +
                ", children=" + Arrays.toString(childrenDebugTODOREMOVE()) +
                '}';
    }

    private String dataToString() {
        StringBuilder result = new StringBuilder();
        int size = page.getEntryCount();
        for (int i = 0; i < size; i++) {
            byte[][] e = page.getEntry(i);
            result.append("{key[").append(i).append("]=").append(e[0] == null ? null : new String(e[1]));
        }
        return result.toString();
    }
}
