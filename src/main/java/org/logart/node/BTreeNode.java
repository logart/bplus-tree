package org.logart.node;

public interface BTreeNode {
    long id();

    void put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    byte[][] get(int idx);

    boolean isFull();

    void copyChildren(BTreeNode node, int startIdx, int endIdx);

    void addChildren(byte[] key, long leftPageId, long rightPageId);

    void replaceChild(long childId, long newId);

    long findChild(byte[] key);

    int numKeys();

    boolean isLeaf();

    void copy(BTreeNode node);

    long[] childrenDebugTODOREMOVE();
}
