package org.logart.node;

import org.logart.page.Page;

public class PersistentBTreeNode implements BTreeNode {
    private final Page page;
    public boolean isLeaf;
    public int numKeys;

    private byte[][] keys;
    private byte[][] values;
    public long[] children; // Only if !isLeaf

    public PersistentBTreeNode(Page page) {
        this.page = page;
        this.isLeaf = page.isLeaf();
        this.numKeys = page.getEntryCount();
    }

    @Override
    public long id() {
        return page().pageId();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        page.put(key, value);
    }

    public byte[] get(byte[] key) {
        byte[][] entry = page.getEntry(key);
        if (entry == null) {
            return null; // Key not found
        }
        return entry[1];
    }

    @Override
    public byte[][] get(int idx) {
        return page.getEntry(idx);
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        return page.isAlmostFull(capacity);
    }

    @Override
    public void copyChildren(BTreeNode node, int startIdx, int endIdx) {

    }

    @Override
    public void addChildren(byte[] key, long leftPageId, long rightPageId) {

    }

    @Override
    public void replaceChild(long childId, long newId) {

    }

    @Override
    public long findChild(byte[] key) {
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
        this.page.copy(((PersistentBTreeNode) node).page());
    }

    @Override
    public long[] childrenDebugTODOREMOVE() {
        return new long[0];
    }

    public Page page() {
        return page;
    }
}
