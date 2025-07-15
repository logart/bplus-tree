package org.logart.node;

import org.logart.Page;

public class PersistentBTreeNode implements BTreeNode {
    private final Page page;
    public boolean isLeaf;
    public int numKeys;

    public byte[][] keys;
    public long[] children; // Only if !isLeaf

    public PersistentBTreeNode(Page page) {
        this.page = page;
        this.isLeaf = page.isLeaf();
        this.numKeys = page.getEntryCount();
        this.keys = page.loadKeys();
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
        // todo
        return page.getEntry(0)[1];
    }

    @Override
    public byte[][] get(int idx) {
        return new byte[0][];
    }

    @Override
    public void remove(int start, int end) {

    }

    @Override
    public boolean isFull() {
        return page.isFull();
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
        return 0;
    }

    @Override
    public int numKeys() {
        return 0;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public void copy(BTreeNode node) {

    }

    @Override
    public long[] childrenDebugTODOREMOVE() {
        return new long[0];
    }

    public byte[] loadValue(int idx) {
        // this could be optimized or cached
        return page.getEntry(idx)[1];
    }

    public Page page() {
        return page;
    }
}
