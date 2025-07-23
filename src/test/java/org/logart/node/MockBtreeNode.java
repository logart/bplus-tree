package org.logart.node;

import org.logart.page.Page;

public class MockBtreeNode implements BTreeNode {
    @Override
    public long id() {
        return 0;
    }

    @Override
    public void put(byte[] key, byte[] value) {

    }

    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public byte[][] get(int idx) {
        return new byte[0][];
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        return false;
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
    public Page page() {
        return null;
    }

    @Override
    public long[] childrenDebugTODOREMOVE() {
        return new long[0];
    }
}
