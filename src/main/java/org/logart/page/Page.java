package org.logart.page;

import org.logart.node.BTreeNode;

import java.nio.ByteBuffer;

public interface Page {

    long pageId();

    int getEntryCount();

    boolean put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    boolean isLeaf();

    byte[][] getEntry(byte[] key);

    byte[][] getEntry(int idx);

    boolean isAlmostFull(long capacity);

    boolean isDeleted();

    void markDeleted();

    long getChild(byte[] key);

    boolean addChild(byte[] key, long left, long right);

    void copy(Page page);

    void copyChildren(Page page, int startIdx, int endIdx);

    void replaceChild(long childId, long newId);

    long[] childrenDbugTODOREMOVE();

    ByteBuffer buffer();
}
