package org.logart.page;

import java.nio.ByteBuffer;

public interface Page {

    long pageId();

    int getEntryCount();

    boolean put(byte[] key, byte[] value);

    boolean isLeaf();

    byte[][] getEntry(byte[] key);

    byte[][] getEntry(int idx);

    boolean isFull();

    long getChild(byte[] key);

    boolean addChild(byte[] key, int left, int right);

    ByteBuffer buffer();

    void copy(Page page);
}
