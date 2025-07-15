package org.logart.node;

public interface BTreeNode {
    long id();

    long parent();

    void put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    byte[][] get(int idx);

    void remove(int start, int end);

    boolean hasParent();

    boolean isFull();

    void copyChildren(BTreeNode node, int startIdx, int endIdx);

    void addChildren(byte[] key, long leftPageId, long rightPageId);

    long findChild(byte[] key);

    int numKeys();

    int keyIdx(byte[] key);

    byte[] key(int idx);

    void key(int idx, byte[] key);

    boolean isLeaf();

    long[] childrenDebugTODOREMOVE();
}
